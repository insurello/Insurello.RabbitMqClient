module Insurello.RabbitMqClient.RabbitMqClient

open Microsoft.Extensions.Logging
open RabbitMQ.Client
open RabbitMQ.Client.Events
open System.Threading.Tasks

type Connection = private Connection of IConnection

let private connectionCloseTimeout = System.TimeSpan.FromSeconds 15.0

module Connection =

    type OnConnectionShutdown = ILogger -> OnConnectionShutdownEvent -> unit

    and OnConnectionShutdownEvent = {
        connectionName: string
        connectionEndpoint: string
        replyCode: int
        replyText: string
    }

    type CloseConnection = unit -> unit

    type Endpoint = { host: string; port: int }

    type ConnectionConfig = {
        name: string
        username: string
        password: string
        vhost: string
        endpoints: List<Endpoint>
    }

    let initAsync
        (logger: ILogger)
        (config: ConnectionConfig)
        (onConnectionShutdown: OnConnectionShutdown)
        : Async<Result<Connection * CloseConnection, string>> =
        task {
            try
                let factory =
                    ConnectionFactory (
                        AutomaticRecoveryEnabled = false,
                        RequestedHeartbeat = System.TimeSpan.FromSeconds 15.,
                        VirtualHost = config.vhost
                    )

                let! connection =
                    factory.CreateConnectionAsync (
                        endpoints =
                            List.map
                                (fun endpoint ->
                                    AmqpTcpEndpoint (hostName = endpoint.host, portOrMinusOne = endpoint.port)
                                )
                                config.endpoints,
                        clientProvidedName = config.name
                    )

                logger.LogInformation (
                    "Connected to RabbitMQ node endpoint {connectionEndpoint}",
                    string connection.Endpoint
                )

                connection.add_ConnectionShutdownAsync (fun _ eventArgs ->
                    task {
                        onConnectionShutdown logger {
                            connectionName = config.name
                            connectionEndpoint = string connection.Endpoint
                            replyCode = int eventArgs.ReplyCode
                            replyText = eventArgs.ReplyText
                        }
                    }
                )

                return
                    Ok (
                        Connection connection,
                        (fun () ->
                            connection.AbortAsync connectionCloseTimeout
                            |> Async.AwaitTask
                            |> Async.RunSynchronously
                        )
                    )

            with exn ->
                return Error $"%s{config.name}: Failed to connect to RabbitMQ: %s{string exn}"
        }
        |> Async.AwaitTask

    /// If the connection is unexpectedly closed the system will exit with error code 13.
    let terminateOnUnexpectedShutdown: OnConnectionShutdown =
        fun logger event ->
            // ReplySuccess (200) is passed when we close the connection from the client side, and thus is expected.
            if event.replyCode = Constants.ReplySuccess then
                logger.LogWarning (
                    "Closing RabbitMQ connection {connectionName} on node endpoint {connectionEndpoint}",
                    event.connectionName,
                    event.connectionEndpoint
                )

            else if event.replyCode <> Constants.ReplySuccess then
                logger.LogError (
                    "Got unexpected event `Shutdown` on connection {connectionName}. {replyCode} - {replyText}. Will terminate the process",
                    event.connectionName,
                    event.replyCode,
                    event.replyText
                )

                exit 13

module Consumer =

    type ReceivedMessage = private ReceivedMessage of BasicDeliverEventArgs * AsyncEventingBasicConsumer

    type Callbacks = {
        onReceived: ReceivedMessage -> Async<unit>
        onRegistered: string -> unit
        onUnregistered: UnregisteredEvent -> unit
        onShutdown: UnregisteredEvent -> unit
    }

    and UnregisteredEvent = {
        queueName: string
        replyCode: int
        replyText: string
    }

    and QueueConfig = {
        queueName: string
        bindings: List<QueueBinding>
        callbacks: Callbacks
        messageTimeToLive: Option<int>
        /// Maximum number of unacked messages to be fetched at once. The messages will still be processed one at a time.
        prefetchCount: uint16
        queueType: QueueType
    }

    and QueueBinding = { exchange: string; routingKey: string }

    and QueueType =
        | Quorum
        /// Deprecated. Use `Quorum` instead.
        | Classic

    type ReplyMessage = {
        headers: List<string * string>
        body: ReplyBody
    }

    and ReplyBody =
        | Json of string
        | Binary of byte[]

    let ackAsync (ReceivedMessage (event, consumer): ReceivedMessage) : Async<unit> =
        consumer.Channel.BasicAckAsync(deliveryTag = event.DeliveryTag, multiple = false).AsTask ()
        |> Async.AwaitTask

    let nackAsync (ReceivedMessage (event, consumer): ReceivedMessage) : Async<unit> =
        consumer.Channel.BasicNackAsync(deliveryTag = event.DeliveryTag, multiple = false, requeue = true).AsTask ()
        |> Async.AwaitTask

    /// The consumed message will be nacked after a specified delay. Will return after waiting for the delay.
    let private nackWithDelayBlockingAsync (delay: System.TimeSpan) : ReceivedMessage -> Async<unit> =
        fun message ->
            async {
                do! Async.Sleep (int delay.TotalMilliseconds |> max 0 |> min System.Int32.MaxValue)

                return! nackAsync message
            }

    /// The consumed message will be nacked after a specified delay. Will return without waiting for the delay.
    let nackWithDelayNonBlockingAsync (delay: System.TimeSpan) : ReceivedMessage -> Async<unit> =
        fun message ->
            async {
                // `StartChild` means we share the same `cancellation token`,
                // so any exceptions should be propagated back to this call.
                let! _ = nackWithDelayBlockingAsync delay message |> Async.StartChild

                return ()
            }

    let messageId (ReceivedMessage (event, _): ReceivedMessage) : string = event.BasicProperties.MessageId

    let messageBody (ReceivedMessage (event, _): ReceivedMessage) : string =
        System.Text.Encoding.UTF8.GetString event.Body.Span

    let messageRoutingKey (ReceivedMessage (event, _): ReceivedMessage) : string = event.RoutingKey

    let replyAsync
        (ReceivedMessage (eventArgs, consumer): ReceivedMessage)
        (replyMessage: ReplyMessage)
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
                        consumer.Channel.BasicPublishAsync (
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

    /// <summary>Declares and optionally binds the specified queue, then starts consuming messages.</summary>
    /// <param name="config">Queue configuration.</param>
    let initAsync (config: QueueConfig) (Connection connection: Connection) : Async<Result<unit, string>> =
        task {
            try
                let! channel = connection.CreateChannelAsync ()

                do! channel.BasicQosAsync (uint32 0, config.prefetchCount, false)

                channel.add_CallbackExceptionAsync (fun _ _ ->
                    task {
                        do!
                            connection.AbortAsync (
                                uint16 Constants.ChannelError,
                                "RabbitMqClient.Consumer: Unhandled channel exception, closing connection",
                                connectionCloseTimeout
                            )
                    }
                )

                let! _ =
                    channel.QueueDeclareAsync (
                        queue = config.queueName,
                        durable = true,
                        exclusive = false,
                        autoDelete = false,
                        arguments =
                            dict (
                                ("x-queue-type",
                                 match config.queueType with
                                 | Quorum -> "quorum"
                                 | Classic -> "classic"
                                 :> obj)
                                :: (config.messageTimeToLive
                                    |> Option.map (fun ttl -> [ "x-message-ttl", ttl :> obj ])
                                    |> Option.defaultValue [])
                            )
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

                consumer.add_ReceivedAsync (fun _ event ->
                    task {
                        if event.ConsumerTag = consumerTag then
                            do! config.callbacks.onReceived (ReceivedMessage (event, consumer))
                    }
                )

                consumer.add_RegisteredAsync (fun _ eventArgs ->
                    task {
                        if eventArgs.ConsumerTags |> Array.contains consumerTag then
                            config.callbacks.onRegistered config.queueName
                    }
                )

                consumer.add_UnregisteredAsync (fun _ eventArgs ->
                    task {
                        if eventArgs.ConsumerTags |> Array.contains consumerTag then
                            let replyCode, replyText =
                                if consumer.ShutdownReason = null then
                                    0, "Missing ShutdownReason. Was the queue deleted?"
                                else
                                    int consumer.ShutdownReason.ReplyCode, consumer.ShutdownReason.ReplyText

                            config.callbacks.onUnregistered {
                                queueName = config.queueName
                                replyCode = replyCode
                                replyText = replyText
                            }
                    }
                )

                consumer.add_ShutdownAsync (fun _ eventArgs ->
                    task {
                        config.callbacks.onShutdown {
                            queueName = config.queueName
                            replyCode = int eventArgs.ReplyCode
                            replyText = eventArgs.ReplyText
                        }
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

                return Ok ()

            with exn ->
                return Error $"RabbitMqClient.Consumer: %s{string exn}"
        }
        |> Async.AwaitTask

    /// <summary>Wrap callbacks with default implementations. Will terminate the process on unexpected events.</summary>
    /// <param name="logger">Logger.</param>
    /// <param name="onReceivedAsync">Callback that's called when a new message is received.</param>
    /// <returns>Callbacks</returns>
    let terminateOnUnexpectedEvents (logger: ILogger) (onReceivedAsync: ReceivedMessage -> Async<unit>) : Callbacks = {
        onReceived =
            fun message ->
                async {
                    try
                        do! onReceivedAsync message
                    with exn ->
                        logger.LogError (
                            exn,
                            "Got unexpected error during event `Received`. Will terminate the process"
                        )

                        exit 9
                }

        onRegistered = fun queueName -> logger.LogInformation ("Consumer registered on queue {queueName}", queueName)

        onUnregistered =
            fun event ->
                // ReplySuccess (200) is passed when we close the connection from the client side, and thus is expected.
                if event.replyCode <> Constants.ReplySuccess then
                    logger.LogError (
                        "Got unexpected event `Unregistered` for consumer on queue {queueName}. {replyCode} - {replyText}. Will terminate the process",
                        event.queueName,
                        event.replyCode,
                        event.replyText
                    )

                    exit 11

        onShutdown =
            fun event ->
                // ReplySuccess (200) is passed when we close the connection from the client side, and thus is expected.
                if event.replyCode = Constants.ReplySuccess then
                    logger.LogInformation (
                        "Got expected event `Shutdown` for consumer on queue {queueName}",
                        event.queueName
                    )

                else
                    logger.LogError (
                        "Got unexpected event `Shutdown` for consumer on queue {queueName}. {replyCode} - {replyText}. Will terminate the process",
                        event.queueName,
                        event.replyCode,
                        event.replyText
                    )

                    exit 13
    }

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

    type RequestRawAsync = RequestMessage -> Async<Result<RawResponseMessage, string>>

    type RequestAsync = RequestMessage -> Async<Result<ResponseMessage, string>>

    type Client = {
        requestRawAsync: RequestRawAsync
        requestAsync: RequestAsync
    }

    type private CorrelationId = string

    type private PendingRequests =
        ConcurrentDictionary<CorrelationId, TaskCompletionSource<Result<BasicDeliverEventArgs, string>>>

    [<Literal>]
    let private queueDirectReplyTo = "amq.rabbitmq.reply-to"

    let private requestRawAsync<'response>
        (pendingRequests: PendingRequests)
        (consumer: AsyncEventingBasicConsumer)
        (mapResponse: ResponseHeaders -> System.ReadOnlyMemory<byte> -> 'response)
        : RequestMessage -> Async<Result<'response, string>> =
        fun message ->
            task {
                let messageId = System.Guid.NewGuid().ToString ()

                let completionSource =
                    TaskCompletionSource<_> TaskCreationOptions.RunContinuationsAsynchronously

                use cancellationSource = new CancellationTokenSource (message.timeout)

                let cancellationCallback () =
                    // Ensure the  pending request is removed to prevent memory leaks.
                    pendingRequests.TryRemove messageId |> ignore

                    // Try set error result.
                    completionSource.TrySetResult (
                        $"Request to queue '%s{message.queue}' timed out after %s{message.timeout.TotalSeconds.ToString ()}s"
                        |> Error
                    )
                    |> ignore

                // On received reply we will return from this function and `Dispose` will be called.
                // `Dispose` will cancel the call to the callback.
                use _cancellationRegistration =
                    cancellationSource.Token.Register (
                        callback = cancellationCallback,
                        useSynchronizationContext = false
                    )

                try
                    if pendingRequests.TryAdd (messageId, completionSource) then
                        let contentType, requestBody =
                            match message.body with
                            | Json json -> "application/json", System.Text.Encoding.UTF8.GetBytes json

                        let requestHeaders =
                            message.headers
                            |> Seq.map System.Collections.Generic.KeyValuePair<string, obj>
                            |> System.Collections.Generic.Dictionary

                        do!
                            consumer.Channel.BasicPublishAsync (
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

                        match! completionSource.Task with
                        | Error error -> return Error error
                        | Ok response -> return Ok (mapResponse response.BasicProperties.Headers response.Body)

                    else
                        return Error $"RabbitMqClient.RPC: Duplicate message id %s{messageId}"

                with exn ->
                    return Error (string exn)
            }
            |> Async.AwaitTask

    let initAsync
        (logger: ILogger)
        (clientName: string)
        (Connection connection: Connection)
        : Async<Result<Client, string>> =
        task {
            try
                let pendingRequests: PendingRequests = ConcurrentDictionary ()

                let consumerTag =
                    queueDirectReplyTo
                    + "-"
                    + clientName
                    + "-consumer-"
                    + System.Guid.NewGuid().ToString ()

                let! channel = connection.CreateChannelAsync ()

                channel.add_CallbackExceptionAsync (fun _ eventArgs ->
                    task {
                        let reason = $"%s{clientName}: Unhandled channel exception, closing connection"

                        logger.LogError (eventArgs.Exception, reason)

                        do! connection.AbortAsync (uint16 Constants.ChannelError, reason, connectionCloseTimeout)
                    }
                )

                let consumer = AsyncEventingBasicConsumer channel

                consumer.add_ReceivedAsync (fun _ eventArgs ->
                    task {
                        let correlationId = eventArgs.BasicProperties.CorrelationId

                        match pendingRequests.TryRemove correlationId with
                        | true, tcs ->
                            if not (tcs.TrySetResult (Ok eventArgs)) then
                                logger.LogWarning (
                                    "Consumer {clientName} received reply but unable to set task completion source with correlation id {correlationId}",
                                    clientName,
                                    correlationId
                                )

                        | _ ->
                            logger.LogWarning (
                                "Consumer {clientName} received reply without known correlation id: {correlationId}",
                                clientName,
                                correlationId
                            )
                    }
                )

                consumer.add_RegisteredAsync (fun _ eventArgs ->
                    task {
                        logger.LogInformation (
                            "Consumer {clientName} registered {consumerTags}",
                            clientName,
                            eventArgs.ConsumerTags
                        )
                    }
                )

                consumer.add_UnregisteredAsync (fun _ eventArgs ->
                    task {
                        let replyCode =
                            if consumer.ShutdownReason = null then
                                0
                            else
                                int consumer.ShutdownReason.ReplyCode

                        // ReplySuccess (200) is passed when we close the connection from the client side, and thus is expected.
                        if replyCode <> Constants.ReplySuccess then
                            logger.LogWarning (
                                "Consumer {clientName} unregistered {consumerTags}",
                                clientName,
                                eventArgs.ConsumerTags
                            )
                    }
                )

                consumer.add_ShutdownAsync (fun _ eventArgs ->
                    task {
                        if eventArgs.ReplyCode = uint16 Constants.ReplySuccess then
                            logger.LogInformation ("Got expected event `Shutdown` for {clientName}", clientName)

                        else
                            logger.LogWarning (
                                "Got unexpected event `Shutdown` for {clientName}. {replyCode} - {replyText}",
                                clientName,
                                eventArgs.ReplyCode,
                                eventArgs.ReplyText
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
                    Ok {
                        requestRawAsync =
                            requestRawAsync
                                pendingRequests
                                consumer
                                (fun headers body -> {
                                    headers = headers
                                    body = body.ToArray ()
                                })

                        requestAsync =
                            requestRawAsync
                                pendingRequests
                                consumer
                                (fun headers body -> {
                                    headers = headers
                                    body = System.Text.Encoding.UTF8.GetString body.Span
                                })
                    }

            with exn ->
                return Error (string exn)
        }
        |> Async.AwaitTask

    let responseHeaderAsString (key: string) (headers: ResponseHeaders) : Option<string> =
        match headers.TryGetValue key with
        | true, objectValue ->
            match objectValue with
            | :? array<byte> as bytes -> Some (System.Text.Encoding.UTF8.GetString bytes)

            | _ -> None

        | _ -> None

module Publish =

    type PublishMessage = {
        queue: string
        headers: List<string * string>
        body: PublishMessageBody
        timeout: System.TimeSpan
    }

    and PublishMessageBody = Json of string

    type PublishAsync = PublishMessage -> Async<Result<unit, string>>

    type Client = { publishAsync: PublishAsync }

    // References:
    // https://github.com/rabbitmq/rabbitmq-dotnet-client/issues/1818
    // https://github.com/rabbitmq/rabbitmq-dotnet-client/blob/87a4788e54cfb0745dd7b6e6a3456c2e1fa23b23/projects/Applications/PublisherConfirms/PublisherConfirms.cs
    // https://github.com/lukebakken/rabbitmq-dotnet-client-1721/blob/eae2aff74d2f2eca2e3e32e3d1a5f01aee461ef8/Program.cs
    let private publishAsync (clientName: string) (channel: IChannel) : PublishAsync =
        fun message ->
            task {
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

                try
                    use cts = new System.Threading.CancellationTokenSource (message.timeout)

                    do!
                        channel.BasicPublishAsync (
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
                    // `BasicPublishAsync` with `publisherConfirmationsEnabled` and `publisherConfirmationTrackingEnabled` indicates `nack` or `basic return` by throwing.
                    return Error $"%s{clientName}: Failed to publish to queue. Details: %s{string exn}"
            }
            |> Async.AwaitTask

    let initAsync
        (logger: ILogger)
        (clientName: string)
        (Connection connection: Connection)
        : Async<Result<Client, string>> =
        task {
            try
                let! channel =
                    connection.CreateChannelAsync (
                        CreateChannelOptions (
                            publisherConfirmationsEnabled = true,
                            publisherConfirmationTrackingEnabled = true
                        )
                    )

                channel.add_CallbackExceptionAsync (fun _ eventArgs ->
                    task {
                        let reason = $"%s{clientName}: Unhandled channel exception, closing connection"

                        logger.LogError (eventArgs.Exception, reason)

                        do! connection.AbortAsync (uint16 Constants.ChannelError, reason, connectionCloseTimeout)
                    }
                )

                return
                    Ok {
                        publishAsync = publishAsync clientName channel
                    }

            with exn ->
                return Error (string exn)
        }
        |> Async.AwaitTask
