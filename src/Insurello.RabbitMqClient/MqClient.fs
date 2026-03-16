module Insurello.RabbitMqClient

open Microsoft.Extensions.Logging
open RabbitMQ.Client
open RabbitMQ.Client.Events
open System.Threading.Tasks

type Connection = private Connection of IConnection

let private connectionCloseTimeout = System.TimeSpan.FromSeconds 15.0

module Connection =

    type OnConnectionShutdown = OnConnectionShutdownEvent -> unit

    and OnConnectionShutdownEvent = {
        connectionName: string
        replyCode: int
        replyText: string
    }

    type CloseConnection = unit -> unit

    let initAsync
        (connectionName: string)
        (endpoints: List<System.Uri>)
        (onConnectionShutdown: OnConnectionShutdown)
        : Async<Result<Connection * CloseConnection, string>> =
        task {
            try
                let factory =
                    ConnectionFactory (
                        AutomaticRecoveryEnabled = false,
                        RequestedHeartbeat = System.TimeSpan.FromSeconds 15.,
                        VirtualHost = "quorum-vhost"
                    )

                let! connection =
                    factory.CreateConnectionAsync (
                        endpoints = List.map AmqpTcpEndpoint endpoints,
                        clientProvidedName = connectionName
                    )

                connection.add_ConnectionShutdownAsync (fun _ eventArgs ->
                    task {
                        onConnectionShutdown {
                            connectionName = connectionName
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
                return Error $"%s{connectionName}: Failed to connect to RabbitMQ: %s{string exn}"
        }
        |> Async.AwaitTask

    /// If the connection is unexpectedly closed the system will exit with error code 13.
    let terminateOnUnexpectedShutdown (logger: ILogger) : OnConnectionShutdown =
        fun event ->
            // ReplySuccess (200) is passed when we close the connection from the client side, and thus is expected.
            if event.replyCode = Constants.ReplySuccess then
                logger.LogWarning ("Closing RabbitMQ connection {connectionName}", event.connectionName)

            else if event.replyCode <> Constants.ReplySuccess then
                logger.LogError (
                    "Got unexpected event `Shutdown` on connection {connectionName}. {replyCode} - {replyText}. Will terminate the process",
                    event.connectionName,
                    event.replyCode,
                    event.replyText
                )

                exit 13

module Consumer =

    type Model = private Consumer of AsyncEventingBasicConsumer

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
        bindToExchange: Option<string>
        callbacks: Callbacks
        messageTimeToLive: Option<int>
        queueType: QueueType
    }

    and QueueType =
        | Quorum
        /// Deprecated. Use `Quorum` instead.
        | Classic

    /// The number of unacked messages to pre-fetch.
    /// This also defines the number of possible messages to be nacking concurrently.
    [<Struct>]
    type QueuePrefetchCount = TenMessages

    type ReplyMessage = {
        headers: List<string * string>
        body: ReplyBody
    }

    and ReplyBody = Json of string

    let ackAsync (ReceivedMessage (event, consumer): ReceivedMessage) : Async<unit> =
        consumer.Channel
            .BasicAckAsync(deliveryTag = event.DeliveryTag, multiple = false)
            .AsTask ()
        |> Async.AwaitTask

    let nackAsync (ReceivedMessage (event, consumer): ReceivedMessage) : Async<unit> =
        consumer.Channel
            .BasicNackAsync(deliveryTag = event.DeliveryTag, multiple = false, requeue = true)
            .AsTask ()
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
                | null, _ -> return Error "Missing reply_to property."
                | _, null -> return Error "Missing message_id property."

                | replyTo, correlationId ->
                    let messageId = System.Guid.NewGuid().ToString ()

                    let contentType, body =
                        match replyMessage.body with
                        | Json jsonContent -> "application/json", System.Text.Encoding.UTF8.GetBytes jsonContent

                    let publishWithHeaders =
                        // `sequence_end` is required by rabbot (foo-foo-mq) clients (https://github.com/arobson/rabbot/issues/76).
                        ("sequence_end", "true") :: replyMessage.headers
                        |> Seq.map (fun (k, v) -> k, v :> obj)
                        |> dict

                    do!
                        consumer.Channel.BasicPublishAsync (
                            exchange = "",
                            routingKey = replyTo,
                            // mandatory should be false when publishing to a direct-reply-to queue (https://www.rabbitmq.com/direct-reply-to.html#limitations)
                            // which is a very common use case for replies.
                            mandatory = false,
                            basicProperties =
                                BasicProperties (
                                    ContentType = contentType,
                                    Persistent = true,
                                    MessageId = messageId,
                                    CorrelationId = correlationId,
                                    Headers = publishWithHeaders
                                ),
                            body = body
                        )

                    return Ok ()

            with exn ->
                return Error (string exn)
        }
        |> Async.AwaitTask

    /// <summary>Declares and optionally binds the specified queue, then starts consuming messages.</summary>
    /// <param name="queuePrefetchCount">Maximum number of unacked messages to be fetched at once. The messages will still be processed one at a time.</param>
    /// <param name="queueConfig">Queue configuration.</param>
    let initAsync
        (queuePrefetchCount: QueuePrefetchCount)
        (queueConfig: QueueConfig)
        (Connection connection: Connection)
        : Async<Result<Model, string>> =
        task {
            try
                let! channel = connection.CreateChannelAsync ()

                let prefetchCount =
                    match queuePrefetchCount with
                    | TenMessages -> 10us

                do! channel.BasicQosAsync (uint32 0, prefetchCount, false)

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
                        queue = queueConfig.queueName,
                        durable = true,
                        exclusive = false,
                        autoDelete = false,
                        arguments =
                            dict (
                                ("x-queue-type",
                                 match queueConfig.queueType with
                                 | Quorum -> "quorum"
                                 | Classic -> "classic"
                                 :> obj)
                                :: (queueConfig.messageTimeToLive
                                    |> Option.map (fun ttl -> [ "x-message-ttl", ttl :> obj ])
                                    |> Option.defaultValue [])
                            )
                    )

                if Option.isSome queueConfig.bindToExchange then
                    do!
                        channel.QueueBindAsync (
                            queue = queueConfig.queueName,
                            exchange = queueConfig.bindToExchange.Value,
                            routingKey = "*",
                            arguments = null
                        )

                let consumer = AsyncEventingBasicConsumer channel

                let consumerTag =
                    $"%s{queueConfig.queueName}-consumer-%s{System.Guid.NewGuid().ToString ()}"

                consumer.add_ReceivedAsync (fun _ event ->
                    task {
                        if event.ConsumerTag = consumerTag then
                            do! queueConfig.callbacks.onReceived (ReceivedMessage (event, consumer))
                    }
                )

                consumer.add_RegisteredAsync (fun _ eventArgs ->
                    task {
                        if eventArgs.ConsumerTags |> Array.contains consumerTag then
                            queueConfig.callbacks.onRegistered queueConfig.queueName
                    }
                )

                consumer.add_UnregisteredAsync (fun _ eventArgs ->
                    task {
                        if eventArgs.ConsumerTags |> Array.contains consumerTag then
                            let replyCode, replyText =
                                if consumer.ShutdownReason = null then
                                    0, "Missing ShutdownReason. Did the queue got deleted?"
                                else
                                    int consumer.ShutdownReason.ReplyCode, consumer.ShutdownReason.ReplyText

                            queueConfig.callbacks.onUnregistered {
                                queueName = queueConfig.queueName
                                replyCode = replyCode
                                replyText = replyText
                            }
                    }
                )

                consumer.add_ShutdownAsync (fun _ eventArgs ->
                    task {
                        queueConfig.callbacks.onShutdown {
                            queueName = queueConfig.queueName
                            replyCode = int eventArgs.ReplyCode
                            replyText = eventArgs.ReplyText
                        }
                    }
                )

                let! _ =
                    channel.BasicConsumeAsync (
                        queue = queueConfig.queueName,
                        autoAck = false,
                        consumerTag = consumerTag,
                        noLocal = false,
                        exclusive = false,
                        arguments = null,
                        consumer = consumer
                    )

                return Ok (Consumer consumer)

            with exn ->
                // Abort connection gracefully on any unexpected error.
                do! connection.AbortAsync connectionCloseTimeout

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
    open System.Threading
    open System.Collections.Concurrent

    type Client = { requestAsync: RequestAsync }

    and RequestAsync = RequestMessage -> Async<Result<ResponseMessage, string>>

    and RequestMessage = {
        queue: string
        headers: List<string * string>
        body: RequestBody
        timeout: System.TimeSpan
    }

    and RequestBody = Json of string

    and ResponseMessage = {
        body: string
        headers: System.Collections.Generic.IDictionary<string, obj>
    }

    type private CorrelationId = string

    type private PendingRequests =
        ConcurrentDictionary<CorrelationId, TaskCompletionSource<Result<ResponseMessage, string>>>

    [<Literal>]
    let private queueDirectReplyTo = "amq.rabbitmq.reply-to"

    let private requestAsync (pendingRequests: PendingRequests) (consumer: AsyncEventingBasicConsumer) : RequestAsync =
        fun message ->
            task {
                let messageId = System.Guid.NewGuid().ToString ()

                let completionSource = TaskCompletionSource<_> ()

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

                        let requestHeaders = message.headers |> Seq.map (fun (k, v) -> k, v :> obj) |> dict

                        do!
                            consumer.Channel.BasicPublishAsync (
                                exchange = "",
                                routingKey = message.queue,
                                // We don't care if the reply message got routed or not as we're using timeout.
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

                        return! completionSource.Task

                    else
                        return Error $"RabbitMqClient.RPC.request: Duplicate message id %s{messageId}"

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
                            let responseMessage: ResponseMessage = {
                                body = System.Text.Encoding.UTF8.GetString eventArgs.Body.Span
                                headers = eventArgs.BasicProperties.Headers
                            }

                            if not (tcs.TrySetResult (Ok responseMessage)) then
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
                                "Get unexpected event `Shutdown` for {clientName}. {replyCode} - {replyText}",
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
                        requestAsync = requestAsync pendingRequests consumer
                    }

            with exn ->
                return Error (string exn)
        }
        |> Async.AwaitTask
