module Insurello.RabbitMqClient.RabbitMqClient

open Microsoft.Extensions.Logging
open RabbitMQ.Client
open RabbitMQ.Client.Events
open System.Threading.Tasks

type Connection = private Connection of ConnectionModel

and private ConnectionModel = {
    logger: ILogger
    connection: IConnection
    onUnexpectedEvent: string -> string -> UnexpectedEvent -> Task<unit>
}

and UnexpectedEvent =
    | UnexpectedException of exn * UnexpectedExceptionDetails
    | UnexpectedChannelClosed of UnexpectedChannelClosedDetails
    | UnexpectedConsumerUnregistered of UnexpectedConsumerUnregistered

and UnexpectedExceptionDetails = {
    eventName: string
    detailFromRabbitMQClient: System.Collections.Generic.IDictionary<string, obj>
}

and UnexpectedChannelClosedDetails = { replyCode: int; replyText: string }

and UnexpectedConsumerUnregistered = {
    consumerTag: string
    queueName: string
}

type InitException internal (message: string, cause: exn) =
    inherit System.Exception(message, cause)

module Connection =

    type Config = {
        /// Application-specific connection name, will be displayed in the management UI.
        name: string

        nodeEndpoints: NodeEndpointsConfig

        connectionTimeout: System.TimeSpan

        heartbeat: System.TimeSpan

        /// Amount of time client will wait for before retrying to recover the connection.
        /// Note that this interval is also used as the connection timeout during recovery, if the value is too low it will result in `connection.start was never received, likely due to a network timeout` errors.
        recoveryInterval: System.TimeSpan

        onUnexpectedEvent: UnexpectedEvent -> Async<unit>
    }

    and NodeEndpointsConfig = {
        username: string
        password: string
        vhost: string
        endpoints: List<Endpoint>
    }

    and Endpoint = { host: string; port: int }

    /// <summary>Initialize a connection against one of the configured RabbitMQ node endpoints.</summary>
    /// <exception cref="InitException">On any thrown exception during initialization, e.g. no node endpoints were reachable. The inner exception holds the thrown exception.</exception>
    let init (logger: ILogger) (config: Config) : Async<Connection> =

        let onUnexpectedEventAsTask clientTypeName clientName event =
            task {
                match event with
                | UnexpectedException (exn, details) ->
                    logger.LogError (
                        exn,
                        "Unexpected exception from {clientTypeName} {clientName} on event {eventName}. Details: {@detailFromRabbitMQClient}",
                        clientTypeName,
                        clientName,
                        details.eventName,
                        details.detailFromRabbitMQClient
                    )

                | UnexpectedChannelClosed details ->
                    logger.LogError (
                        "Unexpected channel closed from {clientTypeName} {clientName}. Details: {@details}",
                        clientTypeName,
                        clientName,
                        details
                    )

                | UnexpectedConsumerUnregistered details ->
                    logger.LogError (
                        "Unexpected consumer unregistered from {clientTypeName} {clientName}. Did the queue got deleted? Details: {@details}",
                        clientTypeName,
                        clientName,
                        details
                    )

                return! config.onUnexpectedEvent event
            }

        task {
            try
                let factory =
                    ConnectionFactory (
                        VirtualHost = config.nodeEndpoints.vhost,
                        UserName = config.nodeEndpoints.username,
                        Password = config.nodeEndpoints.password,

                        AutomaticRecoveryEnabled = true,
                        TopologyRecoveryEnabled = true,
                        NetworkRecoveryInterval = config.recoveryInterval,

                        RequestedConnectionTimeout = config.connectionTimeout,
                        RequestedHeartbeat = config.heartbeat,

                        ContinuationTimeout = System.TimeSpan.FromSeconds 10., // QueueDeclareAsync, BasicConsumeAsync etc.
                        HandshakeContinuationTimeout = System.TimeSpan.FromSeconds 10.
                    )

                let! connection =
                    factory.CreateConnectionAsync (
                        endpoints =
                            List.map
                                (fun endpoint ->
                                    AmqpTcpEndpoint (hostName = endpoint.host, portOrMinusOne = endpoint.port)
                                )
                                config.nodeEndpoints.endpoints,
                        clientProvidedName = config.name
                    )

                logger.LogInformation ("Connected to node endpoint {connectionEndpoint}", string connection.Endpoint)

                connection.add_CallbackExceptionAsync (fun _ eventArgs ->
                    onUnexpectedEventAsTask
                        "connection"
                        config.name
                        (UnexpectedException (
                            eventArgs.Exception,
                            {
                                eventName = "CallbackExceptionEvent"
                                detailFromRabbitMQClient = eventArgs.Detail
                            }
                        ))
                )

                connection.add_RecoverySucceededAsync (fun _ _ ->
                    task {
                        logger.LogInformation (
                            "Connection recovery succeeded. Connected to node endpoint {connectionEndpoint}",
                            string connection.Endpoint
                        )
                    }
                )

                connection.add_ConnectionRecoveryErrorAsync (fun _ eventArgs ->
                    onUnexpectedEventAsTask
                        "connection"
                        config.name
                        (UnexpectedException (
                            eventArgs.Exception,
                            {
                                eventName = "ConnectionRecoveryError"
                                detailFromRabbitMQClient = Map.empty
                            }
                        ))
                )

                connection.add_ConnectionBlockedAsync (fun _ eventArgs ->
                    task {
                        logger.LogWarning (
                            "Connection {connectionName} blocked. Details: {reason}",
                            config.name,
                            eventArgs.Reason
                        )
                    }
                )

                connection.add_ConnectionUnblockedAsync (fun _ _ ->
                    task { logger.LogWarning ("Connection {connectionName} unblocked", config.name) }
                )

                connection.add_ConnectionShutdownAsync (fun _ eventArgs ->
                    task {
                        let replyCode = int eventArgs.ReplyCode

                        // ReplySuccess (200) is (usually) passed when the connection is closed from current running program.
                        if replyCode = Constants.ReplySuccess then
                            logger.LogInformation (
                                "Connection {connectionName} on node endpoint {connectionEndpoint} closed. {replyCode} - {replyText}",
                                config.name,
                                string connection.Endpoint,
                                replyCode,
                                eventArgs.ReplyText
                            )

                        // ConnectionForced (320) is (usually) passed when the connected node (broker) is restarted on purpose, e.g. on upgrades.
                        else if replyCode = Constants.ConnectionForced then
                            logger.LogWarning (
                                "Connection {connectionName} on node endpoint {connectionEndpoint} closed. {replyCode} - {replyText}. Will try to automatically recover in {recoveryInterval} seconds",
                                config.name,
                                string connection.Endpoint,
                                replyCode,
                                eventArgs.ReplyText,
                                config.recoveryInterval.TotalSeconds
                            )

                        else
                            // By throwing we interrupt the recovery interval. The exception will be picked up by `add_CallbackExceptionAsync`.
                            return
                                raise (
                                    Exceptions.OperationInterruptedException (
                                        eventArgs,
                                        "RabbitMqClient.Connection: Unexpected closed"
                                    )
                                )
                    }
                )

                return
                    Connection {
                        logger = logger
                        connection = connection
                        onUnexpectedEvent = onUnexpectedEventAsTask
                    }

            with exn ->
                return
                    raise (InitException ($"RabbitMqClient.Connection: Failed create connection %s{config.name}", exn))
        }
        |> Async.AwaitTask

    /// <summary>Initialize a connection against one of the configured RabbitMQ node endpoints.</summary>
    let tryInit (logger: ILogger) (config: Config) : Async<Result<Connection, InitException>> =
        async {
            try
                let! connection = init logger config
                return Ok connection

            with :? InitException as exn ->
                return Error exn
        }

    /// Closes the connection.
    let close (closeTimeout: System.TimeSpan) (Connection model) =
        model.logger.LogInformation (
            "Closing connection to node endpoint {connectionEndpoint}",
            string model.connection.Endpoint
        )

        model.connection.AbortAsync closeTimeout |> Async.AwaitTask

module Consumer =

    type Client = private Client of ConsumerModel

    and private ConsumerModel = {
        consumer: AsyncEventingBasicConsumer
        consumedQueue: string
        logger: ILogger
    }

    type ReceivedMessage = private ReceivedMessage of BasicDeliverEventArgs * ConsumerModel

    type ReplyMessage = {
        headers: List<string * string>
        body: ReplyBody
    }

    and ReplyBody =
        | Json of string
        | Binary of byte[]

    type QueueConfig = {
        queueName: string
        bindings: List<QueueBinding>
        messageTimeToLive: Option<int>
        /// Maximum number of unacked messages to be fetched at once. The messages will still be processed one at a time.
        prefetchCount: uint16
        queueType: QueueType
        onReceivedAsync: ReceivedMessage -> Async<unit>
    }

    and QueueBinding = { exchange: string; routingKey: string }

    and QueueType =
        | Quorum
        /// Deprecated. Use `Quorum` instead.
        | Classic

    /// <summary>Initializes a Consumer client. Declares and optionally binds the specified queue to an exchange, then starts consuming messages.</summary>
    /// <exception cref="InitException">On any thrown exception during initialization. The inner exception holds the thrown exception.</exception>
    let init (config: QueueConfig) (Connection model: Connection) : Async<Client> =
        task {
            try
                let onUnexpectedEvent = model.onUnexpectedEvent "consumer" config.queueName

                let! channel = model.connection.CreateChannelAsync ()

                // When the unregistered callback is run the connection recovery may have already succeed.
                // And to be able to different between expected unregistrations, due to connection recovery,
                // and unexpected unregistrations, where the queue is removed, we track if the channel was closed
                // prior to an unregistration. We clear this flag after the unregistration is handled.
                let mutable wasChannelShutdownBeforeUnregistered = false

                channel.add_CallbackExceptionAsync (fun _ eventArgs ->
                    onUnexpectedEvent (
                        UnexpectedException (
                            eventArgs.Exception,
                            {
                                eventName = "CallbackExceptionEvent"
                                detailFromRabbitMQClient = eventArgs.Detail
                            }
                        )
                    )
                )

                channel.add_ChannelShutdownAsync (fun _ eventArgs ->
                    task {
                        // We assume this callback is run before the unregistered callback,
                        // and ignore any thread race conditions because of that.
                        wasChannelShutdownBeforeUnregistered <- true

                        let replyCode = int eventArgs.ReplyCode

                        // Assume channel shutdown is expected on ReplySuccess (200) or ConnectionForced (320).
                        // ReplySuccess is (usually) passed when the connection is closed from current running program.
                        // ConnectionForced is (usually) passed when the connected node (broker) is restarted on purpose, e.g. on upgrades.
                        if not (replyCode = Constants.ReplySuccess || replyCode = Constants.ConnectionForced) then
                            return!
                                onUnexpectedEvent (
                                    UnexpectedChannelClosed {
                                        replyCode = replyCode
                                        replyText = eventArgs.ReplyText
                                    }
                                )
                    }
                )

                do! channel.BasicQosAsync (uint32 0, config.prefetchCount, false)

                let queueDeclareArguments =
                    dict [
                        "x-queue-type",
                        match config.queueType with
                        | Quorum -> "quorum"
                        | Classic -> "classic"
                        :> obj

                        if config.messageTimeToLive.IsSome then
                            "x-message-ttl", config.messageTimeToLive.Value
                    ]

                let! _ =
                    channel.QueueDeclareAsync (
                        queue = config.queueName,
                        durable = true,
                        exclusive = false,
                        autoDelete = false,
                        arguments = queueDeclareArguments
                    )

                for queueBinding in config.bindings do
                    do!
                        channel.QueueBindAsync (
                            queue = config.queueName,
                            exchange = queueBinding.exchange,
                            routingKey = queueBinding.routingKey,
                            arguments = null
                        )

                let consumer = AsyncEventingBasicConsumer channel

                let consumerTag =
                    $"%s{config.queueName}-consumer-%s{System.Guid.NewGuid().ToString ()}"

                let consumerModel = {
                    consumer = consumer
                    consumedQueue = config.queueName
                    logger = model.logger
                }

                consumer.add_ReceivedAsync (fun _ event ->
                    task {
                        if event.ConsumerTag = consumerTag then
                            do! config.onReceivedAsync (ReceivedMessage (event, consumerModel))

                        else
                            model.logger.LogError (
                                "Received message with unknown consumer tag {unknownConsumerTag}, expected consumer tag {expectedConsumerTag}",
                                event.ConsumerTag,
                                consumerTag
                            )
                    }
                )

                consumer.add_RegisteredAsync (fun _ eventArgs ->
                    task {
                        if eventArgs.ConsumerTags |> Array.contains consumerTag then
                            model.logger.LogInformation (
                                "Consumer registered on queue {queueName} using tag {consumerTag}",
                                config.queueName,
                                consumerTag
                            )

                        else
                            model.logger.LogError (
                                "Consumer registered with unknown consumer tags {unknownConsumerTags}, expected consumer tag {expectedConsumerTag}",
                                eventArgs.ConsumerTags,
                                consumerTag
                            )
                    }
                )

                consumer.add_UnregisteredAsync (fun _ eventArgs ->
                    task {
                        if eventArgs.ConsumerTags |> Array.contains consumerTag then
                            // If the consumer became unregistered without a channel shutdown
                            // it probably means the queue was deleted, which is unexpected.
                            let isUnexpected = not wasChannelShutdownBeforeUnregistered

                            // Once we know if the unregistration is expected or not, clear the flag.
                            // We assume this is called after channel shutdown and ignore any thread race conditions because of that.
                            wasChannelShutdownBeforeUnregistered <- false

                            if isUnexpected then
                                return!
                                    onUnexpectedEvent (
                                        UnexpectedConsumerUnregistered {
                                            consumerTag = consumerTag
                                            queueName = config.queueName
                                        }
                                    )

                        else
                            model.logger.LogError (
                                "Consumer unregistered with unknown consumer tags {unknownConsumerTags}, expected consumer tag {expectedConsumerTag}",
                                eventArgs.ConsumerTags,
                                consumerTag
                            )
                    }
                )

                let! _ =
                    channel.BasicConsumeAsync (
                        queue = config.queueName,
                        autoAck = false,
                        consumerTag = consumerTag,
                        noLocal = false,
                        exclusive = false,
                        arguments = null,
                        consumer = consumer
                    )

                return Client consumerModel

            with exn ->
                return
                    raise (
                        InitException ($"RabbitMqClient.Consumer: Failed to consume queue %s{config.queueName}", exn)
                    )
        }
        |> Async.AwaitTask

    /// <summary>Initializes a Consumer client. Declares and optionally binds the specified queue to an exchange, then starts consuming messages.</summary>
    let tryInit (config: QueueConfig) (connection: Connection) : Async<Result<Client, InitException>> =
        async {
            try
                let! client = init config connection
                return Ok client

            with :? InitException as exn ->
                return Error exn
        }

    /// Closes the channel consuming messages.
    let close (Client model: Client) : Async<unit> =
        model.consumer.Channel.CloseAsync () |> Async.AwaitTask

    let ack (ReceivedMessage (event, model): ReceivedMessage) : Async<unit> =
        async {
            try
                do!
                    model.consumer.Channel.BasicAckAsync(deliveryTag = event.DeliveryTag, multiple = false).AsTask ()
                    |> Async.AwaitTask

            with :? Exceptions.AlreadyClosedException as exn ->
                // `AlreadyClosedException` is thrown when the connection is closed.
                // As we enforce connection recovery we can assume the connection is in recovery mode when this happens.
                model.logger.LogWarning (
                    exn,
                    "Unable to ack message ({messageId}) from queue {consumedQueue}. The connection is closed and waiting to be recovered. The message will be consumed again once a new connection has been established",
                    event.BasicProperties.MessageId,
                    model.consumedQueue
                )
        }

    let nack (ReceivedMessage (event, model): ReceivedMessage) : Async<unit> =
        async {
            try
                do!
                    model.consumer.Channel
                        .BasicNackAsync(deliveryTag = event.DeliveryTag, multiple = false, requeue = true)
                        .AsTask ()
                    |> Async.AwaitTask

            with :? Exceptions.AlreadyClosedException as exn ->
                // `AlreadyClosedException` is thrown when the connection is closed.
                // As we enforce connection recovery we can assume the connection is in recovery mode when this happens.
                model.logger.LogWarning (
                    exn,
                    "Unable to nack message ({messageId}) from queue {consumedQueue}. The connection is closed and waiting to be recovered. The message will be reconsumed once a new connection has been established",
                    event.BasicProperties.MessageId,
                    model.consumedQueue
                )
        }

    /// The consumed message will be nacked after a specified delay. Will return after waiting for the delay.
    let private nackWithDelayBlocking (delay: System.TimeSpan) (message: ReceivedMessage) : Async<unit> =
        async {
            do! Async.Sleep delay

            return! nack message
        }

    /// The consumed message will be nacked after a specified delay. Will return without waiting for the delay.
    let nackWithDelayNonBlocking (delay: System.TimeSpan) (message: ReceivedMessage) : Async<unit> =
        async {
            // `StartChild` means we share the same `cancellation token`,
            // so any exceptions should be propagated back to this call.
            let! _ = nackWithDelayBlocking delay message |> Async.StartChild

            return ()
        }

    let messageId (ReceivedMessage (event, _): ReceivedMessage) : string = event.BasicProperties.MessageId

    // TODO: Ensure message body is copied
    let messageBody (ReceivedMessage (event, _): ReceivedMessage) : string =
        System.Text.Encoding.UTF8.GetString event.Body.Span

    let messageRoutingKey (ReceivedMessage (event, _): ReceivedMessage) : string = event.RoutingKey

    let reply
        (replyMessage: ReplyMessage)
        (ReceivedMessage (eventArgs, model): ReceivedMessage)
        : Async<Result<unit, string>> =
        task {
            try
                match eventArgs.BasicProperties.ReplyTo, eventArgs.BasicProperties.MessageId with
                | null, _ -> return Error "Missing reply_to property"
                | _, null -> return Error "Missing message_id property"

                | replyTo, correlationId ->
                    let messageId = System.Guid.NewGuid().ToString ()

                    let contentType, body =
                        match replyMessage.body with
                        | Json jsonContent -> "application/json", System.Text.Encoding.UTF8.GetBytes jsonContent
                        | Binary data -> "application/octet-stream", data

                    let headers =
                        // `sequence_end` is required by rabbot (foo-foo-mq) clients (https://github.com/arobson/rabbot/issues/76).
                        ("sequence_end", "true") :: replyMessage.headers
                        |> Seq.map System.Collections.Generic.KeyValuePair<string, obj>
                        |> System.Collections.Generic.Dictionary

                    do!
                        model.consumer.Channel.BasicPublishAsync (
                            exchange = "",
                            routingKey = replyTo,
                            // `mandatory` should be false when publishing to a direct-reply-to queue (https://www.rabbitmq.com/direct-reply-to.html#limitations).
                            mandatory = false,
                            basicProperties =
                                BasicProperties (
                                    ContentType = contentType,
                                    Persistent = true,
                                    MessageId = messageId,
                                    CorrelationId = correlationId,
                                    Headers = headers
                                ),
                            body = body
                        )

                    return Ok ()

            with exn ->
                return Error (string exn)
        }
        |> Async.AwaitTask

module RPC =
    open System.Collections.Concurrent
    open System.Threading

    type RequestMessage = {
        queue: string
        headers: List<string * string>
        body: RequestBody
        timeout: System.TimeSpan
    }

    and RequestBody = Json of string

    type ResponseHeaders = System.Collections.Generic.IDictionary<string, obj>

    type RawResponseMessage = {
        body: byte[]
        headers: ResponseHeaders
    }

    type ResponseMessage = {
        body: string
        headers: ResponseHeaders
    }

    type RequestError =
        | RequestTimedOut of RequestTimedOutError
        | ConnectionInRecoveryMode of ConnectionInRecoveryModeError
        | UnexpectedError of UnexpectedError

    and RequestTimedOutError = {
        clientName: string
        toQueue: string
        withTimeout: System.TimeSpan
    }

    and ConnectionInRecoveryModeError = {
        clientName: string
        replyCode: int
        replyText: string
    }

    and UnexpectedError = {
        clientName: string
        thrownException: exn
    }

    type Client = private Client of RPCModel

    and private RPCModel = {
        clientName: string
        pendingRequests: PendingRequests
        consumer: AsyncEventingBasicConsumer
    }

    and private PendingRequests =
        ConcurrentDictionary<
            CorrelationId,
            TaskCompletionSource<Result<IReadOnlyBasicProperties * byte[], RequestError>>
         >

    and private CorrelationId = string

    [<Literal>]
    let private queueDirectReplyTo = "amq.rabbitmq.reply-to"

    /// <summary>Initializes an RPC client.</summary>
    /// <exception cref="InitException">On any thrown exception during initialization. The inner exception holds the thrown exception.</exception>
    let init (clientName: string) (Connection model: Connection) : Async<Client> =
        task {
            try
                let pendingRequests: PendingRequests = ConcurrentDictionary ()

                let consumerTag =
                    queueDirectReplyTo
                    + "-"
                    + clientName
                    + "-consumer-"
                    + System.Guid.NewGuid().ToString ()

                let onUnexpectedEvent = model.onUnexpectedEvent "RPC consumer" clientName

                let! channel = model.connection.CreateChannelAsync ()

                channel.add_CallbackExceptionAsync (fun _ eventArgs ->
                    onUnexpectedEvent (
                        UnexpectedException (
                            eventArgs.Exception,
                            {
                                eventName = "CallbackExceptionEvent"
                                detailFromRabbitMQClient = eventArgs.Detail
                            }
                        )
                    )
                )

                channel.add_ChannelShutdownAsync (fun _ eventArgs ->
                    task {
                        let replyCode = int eventArgs.ReplyCode

                        // Assume channel shutdown is expected on ReplySuccess (200) or ConnectionForced (320).
                        // ReplySuccess is (usually) passed when the connection is closed from current running program.
                        // ConnectionForced is (usually) passed when the connected node (broker) is restarted on purpose, e.g. on upgrades.
                        if not (replyCode = Constants.ReplySuccess || replyCode = Constants.ConnectionForced) then
                            return!
                                onUnexpectedEvent (
                                    UnexpectedChannelClosed {
                                        replyCode = replyCode
                                        replyText = eventArgs.ReplyText
                                    }
                                )
                    }
                )

                let consumer = AsyncEventingBasicConsumer channel

                consumer.add_ReceivedAsync (fun _ eventArgs ->
                    task {
                        let correlationId = eventArgs.BasicProperties.CorrelationId

                        match pendingRequests.TryRemove correlationId with
                        | true, tcs ->
                            // Ensure the message body is copied.
                            let result = Ok (eventArgs.BasicProperties, eventArgs.Body.ToArray ())

                            if not (tcs.TrySetResult result) then
                                model.logger.LogWarning (
                                    "Consumer {clientName} received reply but unable to set task completion source with correlation id {correlationId}",
                                    clientName,
                                    correlationId
                                )

                        | _ ->
                            model.logger.LogWarning (
                                "Consumer {clientName} received reply without known correlation id {correlationId}",
                                clientName,
                                correlationId
                            )
                    }
                )

                consumer.add_RegisteredAsync (fun _ eventArgs ->
                    task {
                        model.logger.LogInformation (
                            "Consumer registered on queue {queueName} using tag {consumerTags}",
                            queueDirectReplyTo,
                            if eventArgs.ConsumerTags.Length = 1 then
                                eventArgs.ConsumerTags[0]: obj
                            else
                                eventArgs.ConsumerTags
                        )
                    }
                )

                // TODO: Test with delay from RPC
                consumer.add_UnregisteredAsync (fun _ _ ->
                    task {
                        // TODO: Add wasChannelShutdownBeforeUnregistered?
                        if consumer.Channel.IsOpen then
                            return!
                                onUnexpectedEvent (
                                    UnexpectedConsumerUnregistered {
                                        consumerTag = consumerTag
                                        queueName = queueDirectReplyTo
                                    }
                                )
                    }
                )

                let! _ =
                    channel.BasicConsumeAsync (
                        queue = queueDirectReplyTo,
                        autoAck = true, // Must be true for direct-reply-to.
                        consumerTag = consumerTag,
                        noLocal = false,
                        exclusive = false,
                        arguments = null,
                        consumer = consumer
                    )

                return
                    Client {
                        clientName = clientName
                        pendingRequests = pendingRequests
                        consumer = consumer
                    }

            with exn ->
                return raise (InitException ($"RabbitMqClient.RPC: Failed to create client %s{clientName}", exn))
        }
        |> Async.AwaitTask

    /// <summary>Initializes an RPC client.</summary>
    let tryInit (clientName: string) (connection: Connection) : Async<Result<Client, InitException>> =
        async {
            try
                let! client = init clientName connection
                return Ok client

            with :? InitException as exn ->
                return Error exn
        }

    let private requestRawInternal<'response>
        (mapResponse: IReadOnlyBasicProperties -> byte[] -> 'response)
        (Client model: Client)
        : RequestMessage -> Async<Result<'response, RequestError>> =
        fun message ->
            task {
                let messageId = System.Guid.NewGuid().ToString ()

                let requestCompletionSource =
                    TaskCompletionSource<_> TaskCreationOptions.RunContinuationsAsynchronously

                use requestTimeoutCancellationTokenSource =
                    new CancellationTokenSource (message.timeout)

                // On received reply we will return from this function and `Dispose` will be called.
                // `Dispose` will cancel the call to the callback.
                use _cancellationRegistration =
                    requestTimeoutCancellationTokenSource.Token.Register (
                        callback =
                            fun () ->
                                // Ensure the  pending request is removed to prevent memory leaks.
                                model.pendingRequests.TryRemove messageId |> ignore

                                // Try set error result.
                                requestCompletionSource.TrySetResult (
                                    Error (
                                        RequestTimedOut {
                                            clientName = model.clientName
                                            toQueue = message.queue
                                            withTimeout = message.timeout
                                        }
                                    )
                                )
                                |> ignore
                        , useSynchronizationContext = false
                    )

                try
                    if model.pendingRequests.TryAdd (messageId, requestCompletionSource) then
                        let contentType, requestBody =
                            match message.body with
                            | Json json -> "application/json", System.Text.Encoding.UTF8.GetBytes json

                        let requestHeaders =
                            message.headers
                            |> Seq.map System.Collections.Generic.KeyValuePair<string, obj>
                            |> System.Collections.Generic.Dictionary

                        do!
                            model.consumer.Channel.BasicPublishAsync (
                                exchange = "",
                                routingKey = message.queue,
                                // We don't care if the request message got routed or not as we're using timeout.
                                mandatory = false,
                                basicProperties =
                                    BasicProperties (
                                        ContentType = contentType,
                                        Persistent = false,
                                        MessageId = messageId,
                                        ReplyTo = queueDirectReplyTo,
                                        Headers = requestHeaders
                                    ),
                                body = requestBody
                            )

                        match! requestCompletionSource.Task with
                        | Error error -> return Error error
                        | Ok (basicProperties, body) -> return Ok (mapResponse basicProperties body)

                    else
                        return
                            failwith
                                $"%s{model.clientName}: Message id %s{messageId} already added to pending requests. This should not happen as the message id is a generated UUID and thus should be unique"

                with
                | :? Exceptions.AlreadyClosedException as e ->
                    // `AlreadyClosedException` is thrown when the connection is closed.
                    // As we enforce connection recovery we can assume the connection is in
                    // recovery mode when this happens.
                    return
                        Error (
                            ConnectionInRecoveryMode {
                                clientName = model.clientName
                                replyCode = int e.ShutdownReason.ReplyCode
                                replyText = e.ShutdownReason.ReplyText
                            }
                        )

                | exn ->
                    // Interpret all other exceptions as `UnexpectedError`.
                    return
                        Error (
                            UnexpectedError {
                                clientName = model.clientName
                                thrownException = exn
                            }
                        )

            }
            |> Async.AwaitTask

    let private toRawResponseMessage (basicProperties: IReadOnlyBasicProperties) (body: byte[]) : RawResponseMessage = {
        headers = basicProperties.Headers
        body = body
    }

    let private toResponseMessage (basicProperties: IReadOnlyBasicProperties) (body: byte[]) : ResponseMessage = {
        headers = basicProperties.Headers
        body = System.Text.Encoding.UTF8.GetString body
    }

    let requestRaw (message: RequestMessage) (client: Client) : Async<Result<RawResponseMessage, RequestError>> =
        message |> requestRawInternal toRawResponseMessage client

    let request (message: RequestMessage) (client: Client) : Async<Result<ResponseMessage, RequestError>> =
        message |> requestRawInternal toResponseMessage client

    let responseHeaderAsString (key: string) (headers: ResponseHeaders) : Option<string> =
        match headers.TryGetValue key with
        | true, objectValue ->
            match objectValue with
            | :? array<byte> as bytes -> Some (System.Text.Encoding.UTF8.GetString bytes)

            | _ -> None

        | _ -> None

    let requestErrorToString: RequestError -> string =
        function
        | ConnectionInRecoveryMode details ->
            $"%s{details.clientName}: Connection in recovery mode. %d{details.replyCode} - %s{details.replyText}"

        | RequestTimedOut details ->
            $"%s{details.clientName}: Request to queue %s{details.toQueue} timed out after %d{int details.withTimeout.TotalSeconds} seconds"

        | UnexpectedError details ->
            $"%s{details.clientName}: Unexpected exception. Details: %s{details.thrownException.ToString ()}"

module Publish =

    type PublishMessage = {
        queue: string
        headers: List<string * string>
        body: PublishMessageBody
        timeout: System.TimeSpan
    }

    and PublishMessageBody = Json of string

    type Client = private Client of PublishModel

    and private PublishModel = {
        clientName: string
        channel: IChannel
    }

    /// <summary>Initializes Publish client.</summary>
    /// <exception cref="InitException">On any thrown exception during initialization. The inner exception holds the thrown exception.</exception>
    let init (clientName: string) (Connection model: Connection) : Async<Client> =
        task {
            try
                let onUnexpectedEvent = model.onUnexpectedEvent "publish" clientName

                let! channel =
                    model.connection.CreateChannelAsync (
                        CreateChannelOptions (
                            publisherConfirmationsEnabled = true,
                            publisherConfirmationTrackingEnabled = true
                        )
                    )

                channel.add_CallbackExceptionAsync (fun _ eventArgs ->
                    onUnexpectedEvent (
                        UnexpectedException (
                            eventArgs.Exception,
                            {
                                eventName = "CallbackExceptionEvent"
                                detailFromRabbitMQClient = eventArgs.Detail
                            }
                        )
                    )
                )

                channel.add_ChannelShutdownAsync (fun _ eventArgs ->
                    task {
                        let replyCode = int eventArgs.ReplyCode

                        // Assume channel shutdown is expected on ReplySuccess (200) or ConnectionForced (320).
                        // ReplySuccess is (usually) passed when the connection is closed from current running program.
                        // ConnectionForced is (usually) passed when the connected node (broker) is restarted on purpose, e.g. on upgrades.
                        if not (replyCode = Constants.ReplySuccess || replyCode = Constants.ConnectionForced) then
                            return!
                                onUnexpectedEvent (
                                    UnexpectedChannelClosed {
                                        replyCode = replyCode
                                        replyText = eventArgs.ReplyText
                                    }
                                )
                    }
                )

                return
                    Client {
                        clientName = clientName
                        channel = channel
                    }

            with exn ->
                return raise (InitException ($"RabbitMqClient.Publish: Failed to create client %s{clientName}", exn))
        }
        |> Async.AwaitTask

    /// <summary>Initializes a Publish client.</summary>
    let tryInit (clientName: string) (connection: Connection) : Async<Result<Client, InitException>> =
        async {
            try
                let! client = init clientName connection
                return Ok client

            with :? InitException as exn ->
                return Error exn
        }

    /// Publishes a message and awaits publisher confirmation.
    // References:
    // https://github.com/rabbitmq/rabbitmq-dotnet-client/issues/1818
    // https://github.com/rabbitmq/rabbitmq-dotnet-client/blob/87a4788e54cfb0745dd7b6e6a3456c2e1fa23b23/projects/Applications/PublisherConfirms/PublisherConfirms.cs
    // https://github.com/lukebakken/rabbitmq-dotnet-client-1721/blob/eae2aff74d2f2eca2e3e32e3d1a5f01aee461ef8/Program.cs
    let publish (message: PublishMessage) (Client model: Client) : Async<Result<unit, string>> =
        task {
            try
                let messageId = System.Guid.NewGuid().ToString ()

                let contentType, publishBody =
                    match message.body with
                    | Json jsonString -> "application/json", System.Text.Encoding.UTF8.GetBytes jsonString

                // The IDictionary implementation must be mutable due to we're using `publisherConfirmationTrackingEnabled`
                // which adds the header `x-dotnet-pub-seq-no` in the `BasicPublishAsync` call.
                let requestHeaders =
                    message.headers
                    |> Seq.map System.Collections.Generic.KeyValuePair<string, obj>
                    |> System.Collections.Generic.Dictionary

                use cts = new System.Threading.CancellationTokenSource (message.timeout)

                do!
                    model.channel.BasicPublishAsync (
                        exchange = "",
                        routingKey = message.queue,
                        body = publishBody,
                        mandatory = true, // If the queue doesn't exist, a `basic return` is replied.
                        basicProperties =
                            BasicProperties (
                                ContentType = contentType,
                                Persistent = true,
                                MessageId = messageId,
                                Headers = requestHeaders
                            ),
                        cancellationToken = cts.Token
                    )

                return Ok ()

            with exn ->
                // TODO: Catch AlreadyClosedException and create error type.
                // TODO: Catch RabbitMQ.Client.Exceptions.PublishException and create error type.

                // `BasicPublishAsync` with `publisherConfirmationsEnabled` and `publisherConfirmationTrackingEnabled` indicates `nack` or `basic return` by throwing.
                return Error $"%s{model.clientName}: Failed to publish to queue. Details: %s{string exn}"
        }
        |> Async.AwaitTask
