namespace Insurello.RabbitMqClient

open System.Linq
open Microsoft.Extensions.Logging

[<RequireQualifiedAccess>]
module MqClient =

    open Insurello.AsyncExtra
    open RabbitMQ.Client
    open RabbitMQ.Client.Events
    open RabbitMQ.Client.Exceptions

    type private ExceptionCallback = System.Exception -> string -> IConnection -> unit

    type private ModelData =
        { channelConsumer: AsyncEventingBasicConsumer
          rpcConsumer: AsyncEventingBasicConsumer
          pendingRequests: System.Collections.Concurrent.ConcurrentDictionary<string, Result<ReceivedMessage, string> System.Threading.Tasks.TaskCompletionSource>
          connection: IConnection
          mutable ignoreCallbacksWhileClosing: bool }

    and Message<'event> = private Message of 'event * ModelData

    and ReceivedMessage = Message<BasicDeliverEventArgs>

    type RawBody = string

    type Model = private Model of ModelData

    [<RequireQualifiedAccess>]
    type GetHeaderResult =
        | StringValue of string
        | NotFound
        | ErrorConvertingHeaderValueToString of string

    /// <summary>
    /// The maximum number of MQ messages to be fetched from queues and get processed at a time by the RabbitMQ client.
    /// We recommend setting it to DefaultToTen if you don't know what you are doing.
    /// </summary>
    /// <returns>PrefetchConfig</returns>
    [<RequireQualifiedAccess>]
    type PrefetchConfig =
        | DefaultToTen
        | Count of int

    type Callbacks =
        { OnReceived: ReceivedMessage -> Async<unit>
          OnRegistered: ConsumerEventArgs Message -> Async<unit>
          OnUnregistered: UnregisteredEvent -> unit
          OnConsumerCancelled: ConsumerEventArgs Message -> Async<unit>
          OnShutdown: UnregisteredEvent -> unit }

    and UnregisteredEvent =
        { queueName: string
          replyCode: int
          replyText: string }

    type Topology = QueueTopology list

    and QueueTopology =
        { Queue: string
          BindToExchange: string option
          ConsumeCallbacks: Callbacks
          MessageTimeToLive: int option
          QueueType: QueueType }

    and QueueType =
        | Classic
        | Quorum

    [<RequireQualifiedAccess>]
    type PublishResult =
        | Acked
        | Nacked
        | ReturnError of string
        | Timeout of string
        | Unknown of string

    type LogError = exn * string * obj -> unit

    type Content =
        | Json of string
        | Binary of byte array

    [<RequireQualifiedAccess>]
    type CorrelationId =
        | Generate
        | Id of string

    type PublishMessage =
        { CorrelationId: CorrelationId
          Headers: Map<string, string>
          Content: Content }

    type private ChannelConfig =
        { withConfirmSelect: bool
          prefetchCount: uint16 }

    type ConnectionConfig =
        { connectionName: string
          endpoints: List<AmqpTcpEndpoint>
          onConnectionShutdown: OnConnectionShutdown }

    and OnConnectionShutdown = OnConnectionShutdownEvent -> unit

    and OnConnectionShutdownEvent =
        { connectionName: string
          replyCode: int
          replyText: string }

    type CloseConnection = unit -> unit

    let private connectionCloseTimeout = System.TimeSpan.FromSeconds 15.0

    let private contentTypeStringFromContent: Content -> string =
        function
        | Json _ -> "application/json"
        | Binary _ -> "application/octet-stream"

    let private bodyFromContent: Content -> byte [] =
        function
        | Json jsonContent -> System.Text.Encoding.UTF8.GetBytes jsonContent
        | Binary binaryContent -> binaryContent

    let private extractReplyTo: BasicDeliverEventArgs -> Result<string, string> =
        fun event ->
            match event.BasicProperties.ReplyTo with
            | null -> Error "Missing reply_to property."
            | replyTo -> Ok replyTo

    let private extractMesssageId: BasicDeliverEventArgs -> Result<string, string> =
        fun event ->
            match event.BasicProperties.MessageId with
            | null -> Error "Missing message_id property."
            | messageId -> Ok messageId

    let extractReplyProperties: Message<BasicDeliverEventArgs>
        -> Result<{| ReplyTo: string
                     CorrelationId: string |}, string> =
        fun (Message (event, _)) ->
            event
            |> extractReplyTo
            |> Result.bind (fun replyTo ->
                extractMesssageId event
                |> Result.map (fun messageId ->
                    {| ReplyTo = replyTo
                       CorrelationId = messageId |}))

    let routingKeyFromMessage: ReceivedMessage -> string =
        fun (Message (event, _)) -> event.RoutingKey

    let private asTask: ModelData -> 'event -> (Message<'event> -> Async<unit>) -> System.Threading.Tasks.Task =
        fun model event callback ->
            async { do! callback (Message(event, model)) }
            |> Async.StartAsTask
            :> System.Threading.Tasks.Task

    let private consumeQueue: Model -> string -> QueueTopology -> Async<Model> =
        fun (Model model) uniqueTag queueTopology ->

            let consumerTag = queueTopology.Queue + "-consumer-" + uniqueTag

            let doNothingTask: unit -> System.Threading.Tasks.Task =
                (fun () -> async.Return() |> Async.StartAsTask :> System.Threading.Tasks.Task)

            model.channelConsumer.add_ReceivedAsync (fun _sender event ->
                if event.ConsumerTag = consumerTag then
                    asTask model event queueTopology.ConsumeCallbacks.OnReceived
                else
                    doNothingTask ())

            model.channelConsumer.add_RegisteredAsync (fun _sender event ->
                if event.ConsumerTags.Contains consumerTag then
                    asTask model event queueTopology.ConsumeCallbacks.OnRegistered
                else
                    doNothingTask ())

            model.channelConsumer.add_UnregisteredAsync (fun _sender eventArgs ->
                task {
                    let shutdownReason = model.channelConsumer.ShutdownReason

                    if eventArgs.ConsumerTags
                       |> Array.contains consumerTag then
                        let replyCode, replyText =
                            if shutdownReason = null then
                                0, "Missing ShutdownReason. Did the queue got deleted?"
                            else
                                int shutdownReason.ReplyCode, shutdownReason.ReplyText

                        queueTopology.ConsumeCallbacks.OnUnregistered
                            { queueName = queueTopology.Queue
                              replyCode = replyCode
                              replyText = replyText }
                }

            )

            model.channelConsumer.add_ShutdownAsync (fun _sender event ->
                task {
                    queueTopology.ConsumeCallbacks.OnShutdown
                        { queueName = queueTopology.Queue
                          replyCode = int model.channelConsumer.ShutdownReason.ReplyCode
                          replyText = model.channelConsumer.ShutdownReason.ReplyText }
                })

            model.channelConsumer.Channel.BasicConsumeAsync(
                queue = queueTopology.Queue,
                autoAck = false,
                consumerTag = consumerTag,
                noLocal = false,
                exclusive = false,
                arguments = null,
                consumer = model.channelConsumer
            )
            |> Async.AwaitTask
            |> Async.map (fun _ -> Model model)

    let private nonNullString: string -> string =
        fun str ->
            match System.String.IsNullOrEmpty str with
            | true -> ""
            | false -> str

    /// If the connection is unexpectedly closed the system will exit with error code 13.
    let terminateOnUnexpectedShutdown (logger: ILogger) : OnConnectionShutdown =
        fun event ->
            // ReplySuccess (200) is passed when we close the connection from the client side, and thus is expected.
            if event.replyCode = Constants.ReplySuccess then
                logger.LogWarning("Closing RabbitMQ connection {connectionName}", event.connectionName)

            else if event.replyCode <> Constants.ReplySuccess then
                logger.LogError(
                    "Got unexpected event `Shutdown` on connection {connectionName}. {replyCode} - {replyText}. Will terminate the process",
                    event.connectionName,
                    event.replyCode,
                    event.replyText
                )

                exit 13

    let private connect (connectionConfig: ConnectionConfig) : Async<Result<IConnection, string>> =
        task {
            try
                let factory =
                    ConnectionFactory(
                        AutomaticRecoveryEnabled = false,

                        RequestedHeartbeat = System.TimeSpan.FromSeconds 15.,
                        VirtualHost = "quorum-vhost"
                    )

                let! connection =
                    factory.CreateConnectionAsync(
                        endpoints = connectionConfig.endpoints,
                        clientProvidedName = connectionConfig.connectionName
                    )

                connection.add_ConnectionShutdownAsync (fun _ eventArgs ->
                    task {
                        connectionConfig.onConnectionShutdown
                            { connectionName = connectionConfig.connectionName
                              replyCode = int eventArgs.ReplyCode
                              replyText = eventArgs.ReplyText }
                    })

                return Ok connection

            with
            | exn -> return Error $"%s{connectionConfig.connectionName}: Failed to connect to RabbitMQ: %s{string exn}"
        }
        |> Async.AwaitTask

    let private closeConnection: float -> IConnection -> unit =
        fun timeout connection ->
            if connection.IsOpen then
                connection.CloseAsync(System.TimeSpan.FromMilliseconds timeout)
                |> Async.AwaitTask
                |> Async.RunSynchronously
            else
                ()

    let private closeRpcConsumer: Model -> unit =
        fun (Model model) ->
            if model.rpcConsumer.Channel.IsOpen then
                model.rpcConsumer.Channel.CloseAsync()
                |> Async.AwaitTask
                |> Async.RunSynchronously
            else
                ()

    let private closeChannelConsumer: Model -> unit =
        fun (Model model) ->
            if model.channelConsumer.Channel.IsOpen then
                model.channelConsumer.Channel.CloseAsync()
                |> Async.AwaitTask
                |> Async.RunSynchronously
            else
                ()

    /// <summary>
    /// Gracefully closes the connection, the channel consumer and the rpc consumer.
    /// </summary>
    let close: Model -> unit =
        fun (Model model) ->
            // User has requested a graceful close. This will trigger callbacks that could lead to unexpected
            // exceptions. This flag will ignore any callback calls from RabbitMQ Client.
            // There is no way to undo the close.
            model.ignoreCallbacksWhileClosing <- true

            closeChannelConsumer (Model model)
            closeRpcConsumer (Model model)
            closeConnection 0.0 model.connection

    let private createChannel: ChannelConfig -> IConnection -> AsyncResult<IChannel, string> =
        fun config connection ->
            task {
                try
                    let! channel =
                        connection.CreateChannelAsync(
                            CreateChannelOptions(
                                publisherConfirmationsEnabled = config.withConfirmSelect,
                                publisherConfirmationTrackingEnabled = false
                            )
                        )

                    do! channel.BasicQosAsync(uint32 0, config.prefetchCount, false)

                    channel.add_CallbackExceptionAsync (fun a b ->
                        task {
                            do!
                                connection.AbortAsync(
                                    uint16 Constants.ChannelError,
                                    "RabbitMqClient.Consumer: Unhandled channel exception, closing connection",
                                    connectionCloseTimeout
                                )
                        })

                    return Ok channel
                with
                | :? AlreadyClosedException as ex -> return Error ex.Message
            }
            |> Async.AwaitTask

    let private declareQueue: Model -> QueueTopology -> AsyncResult<QueueTopology, string> =
        fun (Model model) queueTopology ->
            let name = nonNullString queueTopology.Queue

            let arguments =
                dict (
                    [ "x-queue-type",
                      (match queueTopology.QueueType with
                       | Quorum -> "quorum"
                       | Classic -> "classic")
                      :> obj ]
                    @ (queueTopology.MessageTimeToLive
                       |> Option.map (fun ttl -> [ ("x-message-ttl", ttl :> obj) ])
                       |> Option.defaultValue [])
                )

            try
                model.channelConsumer.Channel.QueueDeclareAsync(
                    queue = name,
                    durable = true,
                    exclusive = false,
                    autoDelete = false,
                    arguments = arguments
                )
                |> Async.AwaitTask
                |> Async.map (fun _ -> Ok queueTopology)
            with
            | :? OperationInterruptedException as ex -> Async.singleton (Error ex.Message)

    let private bindQueueToExchange: Model -> QueueTopology -> AsyncResult<QueueTopology, string> =
        fun (Model model) queueTopology ->
            try
                match queueTopology.BindToExchange with
                | Some exchangeName ->
                    model.channelConsumer.Channel.QueueBindAsync(
                        queue = queueTopology.Queue,
                        exchange = nonNullString exchangeName,
                        routingKey = "*",
                        arguments = null
                    )
                    |> Async.AwaitTask
                    |> Async.map (fun () -> Ok queueTopology)

                | None -> Async.singleton (Ok queueTopology)
            with
            | ex -> Async.singleton (Error ex.Message)

    let private dictRemoveMutable: 'key
        -> System.Collections.Concurrent.ConcurrentDictionary<'key, 'value>
        -> 'value option =
        fun key dict ->
            match dict.TryRemove key with
            | true, value -> Some value
            | _ -> None

    let private initReplyQueue: Model -> Async<Model> =
        fun (Model model) ->
            let queueName = "amq.rabbitmq.reply-to"

            let consumerTag =
                queueName
                + "-consumer-"
                + System.Guid.NewGuid().ToString()

            let onReceived: ReceivedMessage -> Async<unit> =
                (fun (Message (event, _) as message) ->
                    let correlationId = event.BasicProperties.CorrelationId

                    match dictRemoveMutable correlationId model.pendingRequests with
                    | Some tcs -> tcs.TrySetResult(Ok message) |> ignore

                    | None -> ()

                    async.Return())

            model.rpcConsumer.add_ReceivedAsync (fun _sender event -> asTask model event onReceived)

            model.rpcConsumer.add_RegisteredAsync (fun _sender event -> asTask model event (fun _ -> async.Return()))

            model.rpcConsumer.add_UnregisteredAsync (fun _sender event ->
                asTask model event (fun _ ->
                    failwith "Got Unregistered event on rpc channel"
                    async.Return()))

            model.rpcConsumer.add_ShutdownAsync (fun _sender event ->
                asTask model event (fun _ ->
                    if model.ignoreCallbacksWhileClosing then
                        async.Return()
                    else
                        failwith "Got Shutdown event on rpc channel"))

            // TODO: This doesn't exist
            // model.rpcConsumer.add_ConsumerCancelled (fun _sender event ->
            //     asTask
            //         model
            //         event
            //         (fun _ ->
            //             if model.ignoreCallbacksWhileClosing then
            //                 async.Return ()
            //             else
            //                 failwith "Got ConsumerCancelled event on rpc channel"
            //         )
            // )

            model.rpcConsumer.Channel.BasicConsumeAsync(
                queue = queueName,
                autoAck = true, // Must be true for direct-reply-to
                consumerTag = consumerTag,
                noLocal = false,
                exclusive = false,
                arguments = null,
                consumer = model.rpcConsumer
            )
            |> Async.AwaitTask
            |> Async.map (fun _ -> Model model)

    let private createBasicReturnEventHandler: string
        -> System.Threading.Tasks.TaskCompletionSource<PublishResult>
        -> AsyncEventHandler<BasicReturnEventArgs> =
        fun messageId tcs ->
            AsyncEventHandler<BasicReturnEventArgs> (fun _ args ->
                task {
                    if args.BasicProperties.MessageId = messageId then
                        tcs.TrySetResult(
                            PublishResult.ReturnError
                                $"Failed to publish to queue: ReplyCode: %i{args.ReplyCode}, ReplyText: %s{args.ReplyText}, Exchange: %s{args.Exchange}, RoutingKey: %s{args.RoutingKey}"
                        )
                        |> ignore
                    else
                        ()
                })

    /// <summary>Returns true if the publishSeqNo is confirmed.</summary>
    /// <param name="publishSeqNo"></param>
    /// <param name="deliveredPublishSeqNo"></param>
    /// <param name="multipleConfirms">If false, only one message is confirmed, if true, all messages with a lower or equal sequence number are confirmed.</param>
    let private isConfirmed (publishSeqNo: uint64) (deliveredPublishSeqNo: uint64) (multipleConfirms: bool) : bool =
        publishSeqNo = deliveredPublishSeqNo
        || multipleConfirms
           && publishSeqNo < deliveredPublishSeqNo

    let private createBasicAckEventHandler: uint64
        -> System.Threading.Tasks.TaskCompletionSource<PublishResult>
        -> AsyncEventHandler<BasicAckEventArgs> =
        fun publishSeqNo tcs ->
            AsyncEventHandler<BasicAckEventArgs> (fun _ args ->
                task {
                    if isConfirmed publishSeqNo args.DeliveryTag args.Multiple then
                        tcs.TrySetResult PublishResult.Acked |> ignore
                    else
                        ()
                })

    let private createBasicNackEventHandler: uint64
        -> System.Threading.Tasks.TaskCompletionSource<PublishResult>
        -> AsyncEventHandler<BasicNackEventArgs> =
        fun publishSeqNo tcs ->
            AsyncEventHandler<BasicNackEventArgs> (fun _ args ->
                task {
                    if isConfirmed publishSeqNo args.DeliveryTag args.Multiple then
                        tcs.TrySetResult PublishResult.Nacked |> ignore
                    else
                        ()
                })

    let ackMessage: ReceivedMessage -> unit =
        fun (Message (event, model)) ->
            model
                .channelConsumer
                .Channel
                .BasicAckAsync(deliveryTag = event.DeliveryTag, multiple = false)
                .AsTask()
            |> Async.AwaitTask
            |> Async.RunSynchronously

    let nackMessage: ReceivedMessage -> unit =
        fun (Message (event, model)) ->
            model
                .channelConsumer
                .Channel
                .BasicNackAsync(deliveryTag = event.DeliveryTag, multiple = false, requeue = true)
                .AsTask()
            |> Async.AwaitTask
            |> Async.RunSynchronously

    let nackMessageWithoutRequeue: ReceivedMessage -> unit =
        fun (Message (event, model)) ->
            model
                .channelConsumer
                .Channel
                .BasicNackAsync(deliveryTag = event.DeliveryTag, multiple = false, requeue = false)
                .AsTask()
            |> Async.AwaitTask
            |> Async.RunSynchronously

    /// <summary>As <see cref="MqClient.ackMessage">ackMessage</see> but wrapped with Async</summary>
    let ackMessageAsync: ReceivedMessage -> Async<unit> = ackMessage >> Async.singleton

    /// <summary>As <see cref="MqClient.nackMessage">nackMessage</see> but wrapped with Async</summary>
    let nackMessageAsync: ReceivedMessage -> Async<unit> =
        nackMessage >> Async.singleton

    /// <summary>As <see cref="MqClient.nackMessageWithoutRequeue">nackMessageWithoutRequeue</see> but wrapped with Async</summary>
    let nackMessageWithoutRequeueAsync: ReceivedMessage -> Async<unit> =
        nackMessageWithoutRequeue >> Async.singleton

    let nackMessageWithDelay: System.TimeSpan -> ReceivedMessage -> Async<unit> =
        fun delay msg ->
            let clamp minValue maxValue value = value |> max minValue |> min maxValue

            delay.TotalMilliseconds
            |> round
            |> clamp 0.0 (float System.Int32.MaxValue)
            |> int
            |> Async.Sleep
            |> Async.bind (fun () -> nackMessageAsync msg)

    let messageBody: ReceivedMessage -> byte [] =
        fun (Message (event, _)) -> event.Body.ToArray()

    let messageBodyAsString: ReceivedMessage -> RawBody =
        messageBody >> System.Text.Encoding.UTF8.GetString

    /// <summary>Given a ReceivedMessage and a `key` tries to find the Header value and convert it to a string.</summary>
    /// <returns>GetHeaderResult</returns>
    let getHeaderAsString: ReceivedMessage -> string -> GetHeaderResult =
        fun receivedMessage key ->
            match receivedMessage with
            | Message (basicDeliverEventArgs, _) ->
                basicDeliverEventArgs.BasicProperties.Headers
                |> Seq.map (|KeyValue|)
                |> Map.ofSeq
                |> Map.tryFind key
                |> function
                    | Some (object: obj) ->
                        match object with
                        | :? array<byte> as byteArray ->
                            try
                                GetHeaderResult.StringValue(System.Text.Encoding.UTF8.GetString byteArray)
                            with
                            | ex ->
                                GetHeaderResult.ErrorConvertingHeaderValueToString
                                    $"Couldn't convert to string. Reason: %s{ex.Message}"
                        | _ -> GetHeaderResult.ErrorConvertingHeaderValueToString "Not a byte array"
                    | None -> GetHeaderResult.NotFound

    let messageId: ReceivedMessage -> string =
        fun (Message (event, _)) -> event.BasicProperties.MessageId

    let private uint16FromPrefetchConfig: PrefetchConfig -> Result<uint16, string> =
        fun prefetchCount ->
            match prefetchCount with
            | PrefetchConfig.DefaultToTen -> Ok(uint16 10)
            | PrefetchConfig.Count count ->
                if count >= 0 then
                    Ok(uint16 count)
                else
                    Error "PrefetchCount value must be a non-negative number"

    let init: ConnectionConfig -> PrefetchConfig -> (Model -> Topology) -> AsyncResult<Model * CloseConnection, string> =
        fun connectionConfig prefetchConfig getTopology ->

            prefetchConfig
            |> uint16FromPrefetchConfig
            |> AsyncResult.fromResult
            |> AsyncResult.bind (fun prefetchCount ->
                connect connectionConfig
                |> AsyncResult.bind (fun connection ->
                    createChannel
                        { withConfirmSelect = true
                          prefetchCount = prefetchCount }
                        connection
                    |> AsyncResult.bind (fun channel ->
                        createChannel
                            { withConfirmSelect = false
                              prefetchCount = prefetchCount }
                            connection
                        |> AsyncResult.map (fun rpcChannel ->
                            (connection,
                             Model
                                 { channelConsumer = AsyncEventingBasicConsumer channel

                                   rpcConsumer = AsyncEventingBasicConsumer rpcChannel

                                   pendingRequests =
                                       System.Collections.Concurrent.ConcurrentDictionary<string, Result<ReceivedMessage, string> System.Threading.Tasks.TaskCompletionSource>
                                           ()
                                   connection = connection
                                   ignoreCallbacksWhileClosing = false }))))
                |> AsyncResult.bind (fun (connection, model) ->
                    let declareAQueue = declareQueue model
                    let bindAQueue = bindQueueToExchange model

                    let consumeAQueue = consumeQueue model (System.Guid.NewGuid().ToString())

                    getTopology model
                    |> List.fold
                        (fun prevResult queueTopology ->
                            AsyncResult.bind
                                (fun _ ->
                                    declareAQueue queueTopology
                                    |> AsyncResult.bind bindAQueue
                                    |> AsyncResult.bind (consumeAQueue >> Async.map Ok))
                                prevResult)
                        (AsyncResult.singleton model)
                    |> AsyncResult.mapError (fun error ->
                        closeConnection 3000.0 connection
                        error)
                    |> AsyncResult.bind (
                        initReplyQueue
                        >> Async.map (fun model ->
                            Ok(
                                model,
                                (fun () ->
                                    connection.AbortAsync connectionCloseTimeout
                                    |> Async.AwaitTask
                                    |> Async.RunSynchronously)
                            ))
                    )))

    /// Will publish with confirm.
    let publishToQueue: Model -> System.TimeSpan -> string -> PublishMessage -> Async<PublishResult> =
        fun (Model model) timeout routingKey message ->
            async {
                let tcs = System.Threading.Tasks.TaskCompletionSource<PublishResult>()

                use ct = new System.Threading.CancellationTokenSource(timeout)

                use _ctr =
                    ct.Token.Register(
                        callback =
                            (fun () ->
                                tcs.SetResult(
                                    $"Publish to queue '%s{routingKey}' timedout after %s{timeout.TotalSeconds.ToString()}s"
                                    |> PublishResult.Timeout
                                )),
                        useSynchronizationContext = false
                    )

                let messageId = System.Guid.NewGuid().ToString()

                let basicReturnEventHandler = createBasicReturnEventHandler messageId tcs

                model.channelConsumer.Channel.add_BasicReturnAsync basicReturnEventHandler

                let! basicAckEventHandler, basicNackEventHandler =
                    lock model (fun () ->
                        async {
                            let! nextPublishSeqNo =
                                model
                                    .channelConsumer
                                    .Channel
                                    .GetNextPublishSequenceNumberAsync()
                                    .AsTask()
                                |> Async.AwaitTask

                            let basicAckEventHandler = createBasicAckEventHandler nextPublishSeqNo tcs

                            model.channelConsumer.Channel.add_BasicAcksAsync basicAckEventHandler

                            let basicNackEventHandler = createBasicNackEventHandler nextPublishSeqNo tcs

                            model.channelConsumer.Channel.add_BasicNacksAsync basicNackEventHandler

                            do!
                                model
                                    .channelConsumer
                                    .Channel
                                    .BasicPublishAsync(
                                        exchange = "",
                                        routingKey = routingKey,
                                        mandatory = true,
                                        basicProperties =
                                            BasicProperties(
                                                ContentType = contentTypeStringFromContent message.Content,
                                                Persistent = true,
                                                MessageId = messageId,
                                                CorrelationId =
                                                    (match message.CorrelationId with
                                                     | CorrelationId.Generate -> ""
                                                     | CorrelationId.Id correlationId -> correlationId),
                                                Headers =
                                                    (message.Headers
                                                     |> Map.map (fun _ v -> v :> obj)
                                                     |> (Map.toSeq >> dict))
                                            ),
                                        body = bodyFromContent message.Content
                                    )
                                    .AsTask()
                                |> Async.AwaitTask

                            return (basicAckEventHandler, basicNackEventHandler)
                        })

                let! publishResult = tcs.Task |> (Async.AwaitTask >> Async.Catch)

                model.channelConsumer.Channel.remove_BasicAcksAsync basicAckEventHandler

                model.channelConsumer.Channel.remove_BasicNacksAsync basicNackEventHandler

                model.channelConsumer.Channel.remove_BasicReturnAsync basicReturnEventHandler

                return
                    match publishResult with
                    | Choice1Of2 result -> result

                    | Choice2Of2 reason -> PublishResult.Unknown $"Task cancelled: %A{reason}"
            }

    let replyToMessage: Model -> ReceivedMessage -> Map<string, string> -> Content -> Async<PublishResult> =
        fun (Model model) receivedMessage headers content ->
            receivedMessage
            |> extractReplyProperties
            |> AsyncResult.fromResult
            |> Async.bind (function
                | Ok replyProperties ->
                    let headers = Map.add "sequence_end" "true" headers // sequence_end is required by Rabbot clients (https://github.com/arobson/rabbot/issues/76)

                    let contentType, body =
                        match content with
                        | Json jsonContent -> ("application/json", System.Text.Encoding.UTF8.GetBytes jsonContent)
                        | Binary bytes -> ("application/octet-stream", bytes)

                    let messageId = System.Guid.NewGuid().ToString()

                    model
                        .channelConsumer
                        .Channel
                        .BasicPublishAsync(
                            exchange = "",
                            routingKey = replyProperties.ReplyTo,
                            // mandatory must be false when publishing to direct-reply-to queue https://www.rabbitmq.com/direct-reply-to.html#limitations
                            mandatory = false,
                            basicProperties =
                                BasicProperties(
                                    ContentType = contentType,
                                    Persistent = true,
                                    MessageId = messageId,
                                    CorrelationId = replyProperties.CorrelationId,
                                    Headers =
                                        (headers
                                         |> Map.map (fun _ v -> v :> obj)
                                         |> (Map.toSeq >> dict))
                                ),
                            body = body
                        )
                        .AsTask()
                    |> Async.AwaitTask
                    |> Async.map (fun _ -> PublishResult.Acked)

                | Error errorMessage -> Async.singleton (PublishResult.Unknown errorMessage))

    /// <summary>Make an RPC-call to a RabbitMq queue.</summary>
    /// <param name="Model">MqClient model.</param>#pragma warning disable 3390
    /// <param name="timeout">Seconds before timing out request.</param>
    /// <param name="routingKey">Routing key to publish message to.</param>
    /// <param name="message">Message to be published.</param>
    /// <returns>Response from called RPC endpoint or error.</returns>
    let request: Model -> System.TimeSpan -> string -> PublishMessage -> AsyncResult<ReceivedMessage, string> =
        fun (Model model) timeout routingKey message ->
            async {
                let tcs =
                    System.Threading.Tasks.TaskCompletionSource<Result<ReceivedMessage, string>>()

                use ct = new System.Threading.CancellationTokenSource(timeout)

                let messageId = System.Guid.NewGuid().ToString()

                use _ctr =
                    ct.Token.Register(
                        callback =
                            (fun () ->
                                dictRemoveMutable messageId model.pendingRequests
                                |> ignore

                                tcs.TrySetResult(
                                    Error
                                        $"Publish to queue '%s{routingKey}' timedout after %s{timeout.TotalSeconds.ToString()}s"
                                )
                                |> ignore),
                        useSynchronizationContext = false
                    )

                try
                    if model.pendingRequests.TryAdd(messageId, tcs) then
                        do!
                            model
                                .rpcConsumer
                                .Channel
                                .BasicPublishAsync(
                                    exchange = "",
                                    routingKey = routingKey,
                                    mandatory = true,
                                    basicProperties =
                                        BasicProperties(
                                            ContentType = contentTypeStringFromContent message.Content,
                                            Persistent = false,
                                            MessageId = messageId,
                                            ReplyTo = "amq.rabbitmq.reply-to",
                                            Headers =
                                                (message.Headers
                                                 |> Map.map (fun _ v -> v :> obj)
                                                 |> (Map.toSeq >> dict))
                                        ),
                                    body = bodyFromContent message.Content
                                )
                                .AsTask()
                            |> Async.AwaitTask

                        let! result = tcs.Task |> (Async.AwaitTask >> Async.Catch)

                        return
                            match result with
                            | Choice1Of2 result -> result

                            | Choice2Of2 reason -> Error reason.Message
                    else
                        return Error $"Duplicate message id: %s{messageId}"

                with
                | :? System.ArgumentNullException as ex -> return Error ex.Message

                | :? System.OverflowException as ex -> return Error ex.Message
            }

    /// <summary>Wrap callbacks with default implementation for OnRegistered,
    /// OnUnregistered, OnConsumerCancelled, OnShutdown. If any message is received
    /// for OnConsumerCancelled or OnShutdown the system will exit with error code 9</summary>
    /// <param name="logger">Logger.</param>
    /// <param name="onReceived">Callback that's called when a new message is received.</param>
    /// <returns>Callbacks</returns>
    let terminateOnFailureWrapper: ILogger -> (ReceivedMessage -> Async<unit>) -> Callbacks =
        fun logger onReceived ->
            { OnReceived =
                fun message ->
                    // Something weird happens with exception handling when not
                    // using Async computational expression. Some exceptions are
                    // silently swallowed and never bubbles up.
                    async {
                        try
                            do! onReceived message
                        with
                        | exn ->
                            logger.LogError(exn, $"💥 Unexpected error. %A{exn}\nShutting down", ())
                            exit 9
                    }

              OnRegistered = fun _ -> Async.singleton ()

              OnUnregistered =

                  fun event ->
                      // ReplySuccess (200) is passed when we close the connection from the client side, and thus is expected.
                      if event.replyCode <> Constants.ReplySuccess then
                          logger.LogError(
                              "Got unexpected event `Unregistered` for consumer on queue {queueName}. {replyCode} - {replyText}. Will terminate the process",
                              event.queueName,
                              event.replyCode,
                              event.replyText
                          )

                          exit 11

              OnConsumerCancelled =
                  fun _ ->
                      logger.LogError("Got OnConsumerCancelled event", ())
                      exit 11

              OnShutdown =
                  fun event ->
                      // ReplySuccess (200) is passed when we close the connection from the client side, and thus is expected.
                      if event.replyCode <> Constants.ReplySuccess then
                          logger.LogError(
                              "Got unexpected event `Shutdown` for consumer on queue {queueName}. {replyCode} - {replyText}. Will terminate the process",
                              event.queueName,
                              event.replyCode,
                              event.replyText
                          )

                          exit 13

            }
