module Insurello.RabbitMqClient.RabbitMqClient

open Microsoft.Extensions.Logging
open RabbitMQ.Client
open RabbitMQ.Client.Events
open System.Threading.Tasks

type Connection =
    private
    | Connection of ConnectionModel

    interface System.IDisposable with
        member connection.Dispose() =
            match connection with
            | Connection model ->
                model.logger.LogInformation (
                    "Closing connection to node endpoint {connectionEndpoint}",
                    string model.connection.Endpoint
                )

                model.connection.AbortAsync model.closeTimeout
                |> Async.AwaitTask
                |> Async.RunSynchronously

and private ConnectionModel = {
    logger: ILogger
    connection: IConnection
    closeTimeout: System.TimeSpan
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

        /// <summary>Amount of time to wait for before retrying to recover a lost connection. Please <b>read all important notes</b> below on how to pick this value.</summary>
        /// <remarks>
        /// <list>
        /// <item>
        /// <description>
        /// <b>Important note 1:</b> During connection recovery any in-flight messages will be "returned" to the RabbitMQ nodes (brokers) and will be up for consuming again.
        /// Upon successful recovery the message may be consumed again even though the current handling of the message isn't finished.
        /// This means that <b>all</b> consumers must support handling the same message <b>multiple times</b>, possible at almost the <b>same time</b>.
        /// Increasing this interval will make it less likely to happen with the tradeoff of being unresponsive for a longer time.
        /// </description>
        /// </item>
        /// <item>
        /// <description>
        /// <b>Important note 2:</b> This interval is also used as the connection timeout during recovery, if the value is too low it will result in <c>connection.start was never received, likely due to a network timeout</c> errors.
        /// </description>
        /// </item>
        /// </list>
        /// </remarks>
        recoveryInterval: System.TimeSpan

        closeTimeout: System.TimeSpan

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
                        closeTimeout = config.closeTimeout
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
    let close (connection: Connection) =
        (connection: System.IDisposable).Dispose ()

module Consumer =

    type Client = private Client of ConsumerModel

    and private ConsumerModel = {
        connection: IConnection
        consumer: AsyncEventingBasicConsumer
        consumedQueue: string
        logger: ILogger
    }

    type ReceivedMessage = private ReceivedMessage of ReceivedMessageData * ConsumerModel

    and private ReceivedMessageData = {
        deliveryTag: uint64
        routingKey: string
        messageId: string
        replyTo: string
        body: string
    }

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
        onMessageReceived: ReceivedMessage -> Async<unit>
    }

    and QueueBinding = { exchange: string; routingKey: string }

    and QueueType =
        | Quorum
        /// Deprecated. Use `Quorum` instead.
        | Classic

    type ReplyError =
        | UnableToReply of UnableToReplyError
        | ConnectionInRecoveryMode of ConnectionInRecoveryModeError
        | UnexpectedError of UnexpectedError

    and UnableToReplyError = {
        consumedQueue: string
        reason: string
    }

    and ConnectionInRecoveryModeError = {
        consumedQueue: string
        replyToQueue: string
        replyCode: int
        replyText: string
    }

    and UnexpectedError = {
        consumedQueue: string
        replyToQueue: string
        thrownException: exn
    }

    [<RequireQualifiedAccess>]
    type AckOutcome =
        | Acked
        /// The ack was skipped because the connection is closed, e.g. during shutdown or while waiting to be recovered.
        /// RabbitMQ requeues all unacked messages when the connection closes, so the message will be consumed again.
        | SkippedDueToConnectionClosed

    [<RequireQualifiedAccess>]
    type NackOutcome =
        | Nacked
        /// The nack was skipped because the connection is closed, e.g. during shutdown or while waiting to be recovered.
        /// RabbitMQ requeues all unacked messages when the connection closes, so the message will be consumed again.
        | SkippedDueToConnectionClosed

    [<RequireQualifiedAccess>]
    type DelayedNackOutcome =
        /// The nack is scheduled. It's skipped and logged as a warning if the connection closes during the delay.
        | NackScheduled
        /// The nack was skipped because the connection is closed, e.g. during shutdown or while waiting to be recovered.
        /// RabbitMQ requeues all unacked messages when the connection closes, so the message will be consumed again.
        | SkippedDueToConnectionClosed

    /// <summary>Initializes a Consumer client. Declares and optionally binds the specified queue to an exchange, then starts consuming messages.</summary>
    /// <exception cref="InitException">On any thrown exception during initialization. The inner exception holds the thrown exception.</exception>
    let init (config: QueueConfig) (Connection model: Connection) : Async<Client> =
        task {
            try
                let onUnexpectedEvent = model.onUnexpectedEvent "consumer" config.queueName

                let! channel = model.connection.CreateChannelAsync ()

                // When the unregistered callback is run the connection recovery may have already succeeded.
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
                    connection = model.connection
                    consumer = consumer
                    consumedQueue = config.queueName
                    logger = model.logger
                }

                consumer.add_ReceivedAsync (fun _ eventArgs ->
                    task {
                        if eventArgs.ConsumerTag = consumerTag then
                            do!
                                config.onMessageReceived (
                                    ReceivedMessage (
                                        {
                                            deliveryTag = eventArgs.DeliveryTag
                                            routingKey = eventArgs.RoutingKey
                                            messageId = eventArgs.BasicProperties.MessageId
                                            replyTo = eventArgs.BasicProperties.ReplyTo
                                            // Ensure the message body is copied.
                                            body = System.Text.Encoding.UTF8.GetString eventArgs.Body.Span
                                        },
                                        consumerModel
                                    )
                                )

                        else
                            model.logger.LogError (
                                "Received message with unknown consumer tag {unknownConsumerTag}, expected consumer tag {expectedConsumerTag}",
                                eventArgs.ConsumerTag,
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
                        // On channel shutdown the RabbitMQ client reports the consumer's current tag set,
                        // which can already be empty, assume empty was meant for this consumer.
                        let isThisConsumer =
                            Array.isEmpty eventArgs.ConsumerTags
                            || Array.contains consumerTag eventArgs.ConsumerTags

                        if isThisConsumer then
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

    let inline private logSkippedDueToConnectionClosed
        (operation: string)
        (message: ReceivedMessageData)
        (model: ConsumerModel)
        : unit =
        model.logger.LogWarning (
            "Unable to {operation} message ({messageId}) from queue {consumedQueue} because the connection is closed. RabbitMQ requeues all unacked messages when the connection closes, so the message will be consumed again",
            operation,
            message.messageId,
            model.consumedQueue
        )

    /// Tries to ack the message. If the connection is closed the ack is skipped and logged as a warning.
    let tryAck (ReceivedMessage (message, model): ReceivedMessage) : Async<AckOutcome> =
        async {
            // `BasicAckAsync` doesn't always throw while the connection is closed, so check first.
            // Racing with the connection closing or recovering is harmless: RabbitMQ requeues all unacked messages
            // when the connection closes and acks only apply on the connection the message was delivered on,
            // so at worst the message is consumed one more time. That's RabbitMQ's at-least-once delivery, which applies
            // whenever the connection closes before the ack reaches the broker, so consumers must be idempotent anyway.
            if model.connection.IsOpen then
                try
                    do!
                        model.consumer.Channel
                            .BasicAckAsync(deliveryTag = message.deliveryTag, multiple = false)
                            .AsTask ()
                        |> Async.AwaitTask

                    return AckOutcome.Acked

                with :? Exceptions.AlreadyClosedException ->
                    // The connection closed during acking.
                    logSkippedDueToConnectionClosed "ack" message model

                    return AckOutcome.SkippedDueToConnectionClosed

            else
                logSkippedDueToConnectionClosed "ack" message model

                return AckOutcome.SkippedDueToConnectionClosed
        }

    /// Deprecated. Use `tryAck` instead.
    let ack (receivedMessage: ReceivedMessage) : Async<unit> = tryAck receivedMessage |> Async.Ignore

    /// Tries to nack the message so it's requeued. If the connection is closed the nack is skipped and logged as a warning.
    let tryNack (ReceivedMessage (message, model): ReceivedMessage) : Async<NackOutcome> =
        async {
            // `BasicNackAsync` doesn't always throw while the connection is closed, so check first.
            // Racing with the connection closing or recovering is harmless: RabbitMQ requeues all unacked messages
            // when the connection closes and nacks only apply on the connection the message was delivered on,
            // so the message is consumed again either way. That's RabbitMQ's at-least-once delivery, which applies
            // whenever the connection closes before the nack reaches the broker, so consumers must be idempotent anyway.
            if model.connection.IsOpen then
                try
                    do!
                        model.consumer.Channel
                            .BasicNackAsync(deliveryTag = message.deliveryTag, multiple = false, requeue = true)
                            .AsTask ()
                        |> Async.AwaitTask

                    return NackOutcome.Nacked

                with :? Exceptions.AlreadyClosedException ->
                    // The connection closed during nacking.
                    logSkippedDueToConnectionClosed "nack" message model

                    return NackOutcome.SkippedDueToConnectionClosed

            else
                logSkippedDueToConnectionClosed "nack" message model

                return NackOutcome.SkippedDueToConnectionClosed
        }

    /// Deprecated. Use `tryNack` instead.
    let nack (receivedMessage: ReceivedMessage) : Async<unit> = tryNack receivedMessage |> Async.Ignore

    /// Deprecated. Use `tryNackWithDelayNonBlocking` instead.
    ///
    /// The consumed message will be nacked after a specified delay. Will return after waiting for the delay.
    /// Returns right away, without waiting, when the connection is already closed.
    /// If the connection closes during the delay, the nack is skipped as well. Skipped nacks are logged as warnings.
    let tryNackWithDelayBlocking
        (delay: System.TimeSpan)
        (ReceivedMessage (message, model) as receivedMessage: ReceivedMessage)
        : Async<NackOutcome> =
        async {
            if model.connection.IsOpen then
                do! Async.Sleep delay

                return! tryNack receivedMessage

            else
                logSkippedDueToConnectionClosed "nack" message model

                return NackOutcome.SkippedDueToConnectionClosed
        }

    /// <summary>Schedules a nack of the message after <c>delay</c>, so it's requeued, and returns right away without waiting for the delay.</summary>
    /// <remarks>
    /// <list>
    /// <item><description>Returns <c>DelayedNackOutcome.NackScheduled</c> when the nack was scheduled. The message stays unacked during the delay, so it keeps its prefetch slot.</description></item>
    /// <item><description>Returns <c>DelayedNackOutcome.SkippedDueToConnectionClosed</c> without scheduling anything when the connection is already closed, and logs it as a warning. RabbitMQ requeues all unacked messages when the connection closes, so the message will be consumed again.</description></item>
    /// <item><description>If the connection closes during the delay, the nack is skipped and logged as a warning. RabbitMQ requeues the message in that case too.</description></item>
    /// <item><description>The delayed nack runs detached from the caller, so cancelling the caller doesn't cancel it.</description></item>
    /// <item><description>Any other failure of the delayed nack is logged as an error, since the caller can't be told about it.</description></item>
    /// </list>
    /// </remarks>
    let tryNackWithDelayNonBlocking
        (delay: System.TimeSpan)
        (ReceivedMessage (message, model) as receivedMessage: ReceivedMessage)
        : Async<DelayedNackOutcome> =
        async {
            if model.connection.IsOpen then
                // Runs detached from the caller, so cancelling the caller doesn't leave the message unacked.
                // Nothing awaits its result, so failures are logged here instead of crashing the process.
                Async.Start (
                    async {
                        try
                            do! Async.Sleep delay

                            let! _ = tryNack receivedMessage

                            return ()
                        with exn ->
                            // A closed connection is handled by `tryNack`, so this is unexpected.
                            model.logger.LogError (
                                exn,
                                "Unable to nack message ({messageId}) from queue {consumedQueue} after the delay",
                                message.messageId,
                                model.consumedQueue
                            )
                    },
                    System.Threading.CancellationToken.None
                )

                return DelayedNackOutcome.NackScheduled

            else
                logSkippedDueToConnectionClosed "nack" message model

                return DelayedNackOutcome.SkippedDueToConnectionClosed
        }

    /// Deprecated. Use `tryNackWithDelayNonBlocking` instead.
    let nackWithDelayNonBlocking (delay: System.TimeSpan) (receivedMessage: ReceivedMessage) : Async<unit> =
        tryNackWithDelayNonBlocking delay receivedMessage |> Async.Ignore

    let messageId (ReceivedMessage (message, _): ReceivedMessage) : string = message.messageId

    let messageBody (ReceivedMessage (message, _): ReceivedMessage) : string = message.body

    /// Returns false while the connection the message was received on is closed, e.g. during shutdown or while waiting to be recovered.
    /// RabbitMQ requeues all unacked messages when the connection closes, so the message will be consumed again.
    let isConnectionStillOpen (ReceivedMessage (_, model): ReceivedMessage) : bool = model.connection.IsOpen

    let messageRoutingKey (ReceivedMessage (message, _): ReceivedMessage) : string = message.routingKey

    let reply
        (replyMessage: ReplyMessage)
        (ReceivedMessage (message, model): ReceivedMessage)
        : Async<Result<unit, ReplyError>> =
        task {
            match message.replyTo, message.messageId with
            | null, _ ->
                return
                    Error (
                        UnableToReply {
                            consumedQueue = model.consumedQueue
                            reason = "Missing `reply_to` message property"
                        }
                    )

            | _, null ->
                return
                    Error (
                        UnableToReply {
                            consumedQueue = model.consumedQueue
                            reason = "Missing `message_id` message property to be used as correlation id"
                        }
                    )

            | replyTo, correlationId ->
                try
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

                with
                | :? Exceptions.AlreadyClosedException as exn ->
                    // `AlreadyClosedException` is thrown when the connection is closed.
                    // As we enforce connection recovery we can assume the connection is in
                    // recovery mode when this happens.
                    return
                        Error (
                            ConnectionInRecoveryMode {
                                consumedQueue = model.consumedQueue
                                replyToQueue = replyTo
                                replyCode = int exn.ShutdownReason.ReplyCode
                                replyText = exn.ShutdownReason.ReplyText
                            }
                        )

                | exn ->
                    return
                        Error (
                            UnexpectedError {
                                consumedQueue = model.consumedQueue
                                replyToQueue = replyTo
                                thrownException = exn
                            }
                        )
        }
        |> Async.AwaitTask

    let replyErrorToString: ReplyError -> string =
        function
        | UnableToReply details -> $"%s{details.consumedQueue}: Unable to reply. Details: %s{details.reason}"

        | ConnectionInRecoveryMode details ->
            $"%s{details.consumedQueue}: Connection in recovery mode while trying to reply to %s{details.replyToQueue}. %d{details.replyCode} - %s{details.replyText}"

        | UnexpectedError details ->
            $"%s{details.consumedQueue}: Unexpected exception while replying to %s{details.replyToQueue}. Details: %s{details.thrownException.ToString ()}"

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

    and private PendingRequests = ConcurrentDictionary<CorrelationId, TaskCompletionSource<PendingRequestResult>>

    and private CorrelationId = string

    and private PendingRequestResult =
        | Replied of IReadOnlyBasicProperties * byte[]
        | TimedOut
        | ChannelShutdown of replyCode: int * replyText: string

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

                // When the unregistered callback is run the connection recovery may have already succeeded.
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

                        // Replies are sent to direct reply-to on this channel and will never be received once it's closed.
                        // Fail all pending requests now instead of letting them wait until they time out.
                        for correlationId in pendingRequests.Keys do
                            match pendingRequests.TryRemove correlationId with
                            | true, tcs ->
                                tcs.TrySetResult (ChannelShutdown (replyCode, eventArgs.ReplyText)) |> ignore
                            | _ -> ()

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
                            // Ensure the message body is copied before setting the response.
                            let response = Replied (eventArgs.BasicProperties, eventArgs.Body.ToArray ())

                            if not (tcs.TrySetResult response) then
                                model.logger.LogWarning (
                                    "Consumer {clientName} received reply but unable to set task completion source with correlation id {correlationId}. Probably due to already timed out",
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

                consumer.add_UnregisteredAsync (fun _ _ ->
                    task {
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
                                // Ensure the pending request is removed to prevent memory leaks.
                                model.pendingRequests.TryRemove messageId |> ignore

                                // Try set timed out response.
                                requestCompletionSource.TrySetResult TimedOut |> ignore
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

                        // Wait until response received, timed out or the channel was shut down.
                        match! requestCompletionSource.Task with
                        | Replied (basicProperties, body) -> return Ok (mapResponse basicProperties body)

                        | TimedOut ->
                            return
                                Error (
                                    RequestTimedOut {
                                        clientName = model.clientName
                                        toQueue = message.queue
                                        withTimeout = message.timeout
                                    }
                                )

                        | ChannelShutdown (replyCode, replyText) ->
                            return
                                Error (
                                    ConnectionInRecoveryMode {
                                        clientName = model.clientName
                                        replyCode = replyCode
                                        replyText = replyText
                                    }
                                )

                    else
                        return
                            failwith
                                $"%s{model.clientName}: Message id %s{messageId} already added to pending requests. This shouldn't happen as the message id is a generated UUID and thus should be unique"

                with
                | :? Exceptions.AlreadyClosedException as exn ->
                    // `AlreadyClosedException` is thrown when the connection is closed.
                    // As we enforce connection recovery we can assume the connection is in
                    // recovery mode when this happens.
                    return
                        Error (
                            ConnectionInRecoveryMode {
                                clientName = model.clientName
                                replyCode = int exn.ShutdownReason.ReplyCode
                                replyText = exn.ShutdownReason.ReplyText
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
        | RequestTimedOut details ->
            $"%s{details.clientName}: Request to queue %s{details.toQueue} timed out after %d{int details.withTimeout.TotalSeconds} seconds"

        | ConnectionInRecoveryMode details ->
            $"%s{details.clientName}: Connection in recovery mode. %d{details.replyCode} - %s{details.replyText}"

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

    type PublishError =
        | PublishTimedOut of PublishTimedOutError
        | PublishReturnError of PublishErrorDetails
        | ConnectionInRecoveryMode of PublishErrorDetails
        | UnexpectedError of UnexpectedError

    and PublishTimedOutError = {
        clientName: string
        toQueue: string
        withTimeout: System.TimeSpan
    }

    and PublishErrorDetails = {
        clientName: string
        toQueue: string
        replyCode: int
        replyText: string
    }

    and UnexpectedError = {
        clientName: string
        thrownException: exn
    }

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

                // Create channel with publisher confirmations enabled.
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
    let publish (message: PublishMessage) (Client model: Client) : Async<Result<unit, PublishError>> =
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

            with
            | :? Exceptions.AlreadyClosedException as e ->
                // `AlreadyClosedException` is thrown when the connection is closed.
                // As we enforce connection recovery we can assume the connection is in
                // recovery mode when this happens.
                return
                    Error (
                        ConnectionInRecoveryMode {
                            clientName = model.clientName
                            toQueue = message.queue
                            replyCode = int e.ShutdownReason.ReplyCode
                            replyText = e.ShutdownReason.ReplyText
                        }
                    )

            | :? Exceptions.PublishReturnException as exn ->
                // `PublishReturnException` is thrown if the queue couldn't be routed or the confirmation failed.
                return
                    Error (
                        PublishReturnError {
                            clientName = model.clientName
                            toQueue = message.queue
                            replyCode = int exn.ReplyCode
                            replyText = exn.ReplyText
                        }
                    )

            | :? TaskCanceledException ->
                // `TaskCanceledException` is thrown when the CancellationTokenSource (cts) is run.
                return
                    Error (
                        PublishTimedOut {
                            clientName = model.clientName
                            toQueue = message.queue
                            withTimeout = message.timeout
                        }
                    )

            | exn ->
                return
                    Error (
                        UnexpectedError {
                            clientName = model.clientName
                            thrownException = exn
                        }
                    )
        }
        |> Async.AwaitTask

    let publishErrorToString: PublishError -> string =
        function
        | PublishTimedOut details ->
            $"%s{details.clientName}: Publish to queue %s{details.toQueue} timed out after %d{int details.withTimeout.TotalSeconds} seconds"

        | PublishReturnError details ->
            $"%s{details.clientName}: Publish to queue %s{details.toQueue} returned with error. %d{details.replyCode} - %s{details.replyText}"

        | ConnectionInRecoveryMode details ->
            $"%s{details.clientName}: Connection in recovery mode while trying to publish to queue %s{details.toQueue}. %d{details.replyCode} - %s{details.replyText}"

        | UnexpectedError details ->
            $"%s{details.clientName}: Unexpected exception. Details: %s{details.thrownException.ToString ()}"
