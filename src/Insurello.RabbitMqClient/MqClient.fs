namespace Insurello.RabbitMqClient

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
          OnUnregistered: ConsumerEventArgs Message -> Async<unit>
          OnConsumerCancelled: ConsumerEventArgs Message -> Async<unit>
          OnShutdown: ShutdownEventArgs Message -> Async<unit> }

    type Topology = QueueTopology list

    and QueueTopology =
        { Queue: string
          BindToExchange: string option
          ConsumeCallbacks: Callbacks
          MessageTimeToLive: int option }

    [<RequireQualifiedAccessAttribute>]
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

    let private consumeQueue: Model -> string -> QueueTopology -> Model =
        fun (Model model) uniqueTag queueTopology ->

            let consumerTag = queueTopology.Queue + "-consumer-" + uniqueTag

            let doNothingTask: unit -> System.Threading.Tasks.Task =
                (fun () -> async.Return() |> Async.StartAsTask :> System.Threading.Tasks.Task)

            model.channelConsumer.add_Received (fun _sender event ->
                if event.ConsumerTag = consumerTag then
                    asTask model event queueTopology.ConsumeCallbacks.OnReceived
                else
                    doNothingTask ())

            model.channelConsumer.add_Registered (fun _sender event ->
                if event.ConsumerTag = consumerTag then
                    asTask model event queueTopology.ConsumeCallbacks.OnRegistered
                else
                    doNothingTask ())

            model.channelConsumer.add_Unregistered (fun _sender event ->
                if event.ConsumerTag = consumerTag then
                    asTask model event queueTopology.ConsumeCallbacks.OnUnregistered
                else
                    doNothingTask ())

            model.channelConsumer.add_Shutdown (fun _sender event ->
                if not model.ignoreCallbacksWhileClosing then
                    asTask model event queueTopology.ConsumeCallbacks.OnShutdown
                else
                    doNothingTask ())

            model.channelConsumer.add_ConsumerCancelled (fun _sender event ->
                if event.ConsumerTag = consumerTag
                   && not model.ignoreCallbacksWhileClosing then
                    asTask model event queueTopology.ConsumeCallbacks.OnConsumerCancelled
                else
                    doNothingTask ())

            model.channelConsumer.Model.BasicConsume(
                queue = queueTopology.Queue,
                autoAck = false,
                consumerTag = consumerTag,
                noLocal = false,
                exclusive = false,
                arguments = null,
                consumer = model.channelConsumer
            )
            |> ignore

            Model model

    let private nonNullString: string -> string =
        fun str ->
            match System.String.IsNullOrEmpty str with
            | true -> ""
            | false -> str

    let private connectionFactory: System.Uri -> ConnectionFactory =
        fun uri -> ConnectionFactory(DispatchConsumersAsync = true, AutomaticRecoveryEnabled = false, Uri = uri)

    let private createConnection: string -> IConnectionFactory -> IConnection =
        fun clientProvidedName factory ->
            factory.CreateConnection(clientProvidedName = nonNullString clientProvidedName)

    let private connect: string -> System.Uri -> Result<IConnection, string> =
        fun nameOfClient uri ->
            try
                connectionFactory uri
                |> createConnection (nonNullString nameOfClient)
                |> Ok
            with
            | :? BrokerUnreachableException as ex -> Error ex.Message

            | :? System.ArgumentException as ex -> Error ex.Message

    let private closeConnection: int -> IConnection -> unit =
        fun timeout connection ->
            if connection.IsOpen then
                // Connection seems to be locked when this function is called from an exception raised in MqClient.
                // Make the call asynchrounous so this thread don't block the call.
                Async.Start(async { connection.Close(timeout) })
            else
                ()

    let private closeRpcConsumer: Model -> unit =
        fun (Model model) ->
            if model.rpcConsumer.Model.IsOpen then
                model.rpcConsumer.Model.Close()
            else
                ()

    let private closeChannelConsumer: Model -> unit =
        fun (Model model) ->
            if model.channelConsumer.Model.IsOpen then
                model.channelConsumer.Model.Close()
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
            closeConnection 0 model.connection

    let private createChannel: ChannelConfig -> ExceptionCallback -> IConnection -> Result<IModel, string> =
        fun config exCallback connection ->
            try
                let model = connection.CreateModel()
                model.BasicQos(uint32 0, config.prefetchCount, false)

                if config.withConfirmSelect then
                    model.ConfirmSelect()

                model.CallbackException
                |> Event.add (fun event ->
                    let (hasContext, context) = event.Detail.TryGetValue "context"

                    exCallback
                        event.Exception
                        (if hasContext then
                             context.ToString()
                         else
                             "")
                        connection)

                Ok model
            with
            | :? AlreadyClosedException as ex -> Error ex.Message

    let private declareQueue: Model -> QueueTopology -> Result<QueueTopology, string> =
        fun (Model model) queueTopology ->
            let name = nonNullString queueTopology.Queue

            let arguments =
                dict (
                    [ "x-queue-type",
                      (match queueTopology.BindToExchange with
                       | Some _ -> "quorum"
                       | None -> "classic")
                      :> obj ]
                    @ (queueTopology.MessageTimeToLive
                       |> Option.map (fun ttl -> [ ("x-message-ttl", ttl :> obj) ])
                       |> Option.defaultValue [])
                )

            try
                model.channelConsumer.Model.QueueDeclare(
                    queue = name,
                    durable = true,
                    exclusive = false,
                    autoDelete = false,
                    arguments = arguments
                )
                |> ignore

                Ok queueTopology
            with
            | :? OperationInterruptedException as ex -> Error ex.Message

    let private bindQueueToExchange: Model -> QueueTopology -> Result<QueueTopology, string> =
        fun (Model model) queueTopology ->
            try
                match queueTopology.BindToExchange with
                | Some exchangeName ->
                    model.channelConsumer.Model.QueueBind(
                        queue = queueTopology.Queue,
                        exchange = nonNullString exchangeName,
                        routingKey = "*",
                        arguments = null
                    )
                    |> ignore
                | None -> ()

                Ok queueTopology
            with
            | ex -> Error ex.Message

    let private dictRemoveMutable: 'key
        -> System.Collections.Concurrent.ConcurrentDictionary<'key, 'value>
        -> 'value option =
        fun key dict ->
            match dict.TryRemove key with
            | true, value -> Some value
            | _ -> None

    let private initReplyQueue: Model -> Model =
        fun (Model model) ->
            let queueName = "amq.rabbitmq.reply-to"

            let consumerTag =
                queueName
                + "-consumer-"
                + System.Guid.NewGuid().ToString()

            let onReceived: ReceivedMessage -> Async<unit> =
                (fun ((Message (event, _)) as message) ->
                    let correlationId = event.BasicProperties.CorrelationId

                    match dictRemoveMutable correlationId model.pendingRequests with
                    | Some tcs -> tcs.TrySetResult(Ok message) |> ignore

                    | None -> ()

                    async.Return())

            model.rpcConsumer.add_Received (fun _sender event -> asTask model event onReceived)

            model.rpcConsumer.add_Registered (fun _sender event -> asTask model event (fun _ -> async.Return()))

            model.rpcConsumer.add_Unregistered (fun _sender event ->
                asTask model event (fun _ ->
                    failwith "Got Unregistered event on rpc channel"
                    async.Return()))

            model.rpcConsumer.add_Shutdown (fun _sender event ->
                asTask model event (fun _ ->
                    if model.ignoreCallbacksWhileClosing then
                        async.Return()
                    else
                        failwith "Got Shutdown event on rpc channel"))

            model.rpcConsumer.add_ConsumerCancelled (fun _sender event ->
                asTask model event (fun _ ->
                    if model.ignoreCallbacksWhileClosing then
                        async.Return()
                    else
                        failwith "Got ConsumerCancelled event on rpc channel"))

            model.rpcConsumer.Model.BasicConsume(
                queue = queueName,
                autoAck = true, // Must be true for direct-reply-to
                consumerTag = consumerTag,
                noLocal = false,
                exclusive = false,
                arguments = null,
                consumer = model.rpcConsumer
            )
            |> ignore

            Model model

    let private createBasicReturnEventHandler: string
        -> System.Threading.Tasks.TaskCompletionSource<PublishResult>
        -> System.EventHandler<BasicReturnEventArgs> =
        fun messageId tcs ->
            System.EventHandler<BasicReturnEventArgs> (fun _ args ->
                if args.BasicProperties.MessageId = messageId then
                    tcs.TrySetResult(
                        PublishResult.ReturnError(
                            sprintf
                                "Failed to publish to queue: ReplyCode: %i, ReplyText: %s, Exchange: %s, RoutingKey: %s"
                                args.ReplyCode
                                args.ReplyText
                                args.Exchange
                                args.RoutingKey
                        )
                    )
                    |> ignore
                else
                    ())

    /// <summary>Returns true if the publishSeqNo is confirmed.</summary>
    /// <param name="multipleConfirms">If false, only one message is confirmed, if true, all messages with a lower or equal sequence number are confirmed.</param>
    let private isConfirmed (publishSeqNo: uint64) (deliveredPublishSeqNo: uint64) (multipleConfirms: bool) : bool =
        publishSeqNo = deliveredPublishSeqNo
        || multipleConfirms
           && publishSeqNo < deliveredPublishSeqNo

    let private createBasicAckEventHandler: uint64
        -> System.Threading.Tasks.TaskCompletionSource<PublishResult>
        -> System.EventHandler<BasicAckEventArgs> =
        fun publishSeqNo tcs ->
            System.EventHandler<BasicAckEventArgs> (fun _ args ->
                if isConfirmed publishSeqNo args.DeliveryTag args.Multiple then
                    tcs.TrySetResult PublishResult.Acked |> ignore
                else
                    ())

    let private createBasicNackEventHandler: uint64
        -> System.Threading.Tasks.TaskCompletionSource<PublishResult>
        -> System.EventHandler<BasicNackEventArgs> =
        fun publishSeqNo tcs ->
            System.EventHandler<BasicNackEventArgs> (fun _ args ->
                if isConfirmed publishSeqNo args.DeliveryTag args.Multiple then
                    tcs.TrySetResult PublishResult.Nacked |> ignore
                else
                    ())

    let ackMessage: ReceivedMessage -> unit =
        fun (Message (event, model)) ->
            model.channelConsumer.Model.BasicAck(deliveryTag = event.DeliveryTag, multiple = false)

    let nackMessage: ReceivedMessage -> unit =
        fun (Message (event, model)) ->
            model.channelConsumer.Model.BasicNack(deliveryTag = event.DeliveryTag, multiple = false, requeue = true)

    let nackMessageWithoutRequeue: ReceivedMessage -> unit =
        fun (Message (event, model)) ->
            model.channelConsumer.Model.BasicNack(deliveryTag = event.DeliveryTag, multiple = false, requeue = false)

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

    let messageBody: ReceivedMessage -> byte [] = fun (Message (event, _)) -> event.Body

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
                                GetHeaderResult.ErrorConvertingHeaderValueToString(
                                    $"Couldn't convert to string. Reason: %s{ex.Message}"
                                )
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

    let init: LogError -> string -> System.Uri -> PrefetchConfig -> (Model -> Topology) -> Result<Model, string> =
        fun logError nameOfClient uri prefetchConfig getTopology ->

            prefetchConfig
            |> uint16FromPrefetchConfig
            |> Result.bind (fun prefetchCount ->
                connect nameOfClient uri
                |> Result.bind (fun connection ->
                    let exCallback =
                        (fun ex context connection ->
                            logError (ex, "Unhandled exception on channel in context {$c}", context)
                            closeConnection 3000 connection)

                    createChannel
                        { withConfirmSelect = true
                          prefetchCount = prefetchCount }
                        exCallback
                        connection
                    |> Result.bind (fun channel ->
                        createChannel
                            { withConfirmSelect = false
                              prefetchCount = prefetchCount }
                            exCallback
                            connection
                        |> Result.map (fun rpcChannel ->
                            (connection,
                             Model
                                 { channelConsumer = AsyncEventingBasicConsumer channel

                                   rpcConsumer = AsyncEventingBasicConsumer rpcChannel

                                   pendingRequests =
                                       System.Collections.Concurrent.ConcurrentDictionary<string, Result<ReceivedMessage, string> System.Threading.Tasks.TaskCompletionSource>
                                           ()
                                   connection = connection
                                   ignoreCallbacksWhileClosing = false }))))
                |> Result.bind (fun (connection, model) ->
                    let declareAQueue = declareQueue model
                    let bindAQueue = bindQueueToExchange model

                    let consumeAQueue = consumeQueue model (System.Guid.NewGuid().ToString())

                    getTopology model
                    |> List.fold
                        (fun prevResult queueTopology ->
                            Result.bind
                                (fun _ ->
                                    declareAQueue queueTopology
                                    |> Result.bind bindAQueue
                                    |> Result.map consumeAQueue)
                                prevResult)
                        (Ok model)
                    |> Result.mapError (fun error ->
                        closeConnection 3000 connection
                        error)
                    |> Result.map initReplyQueue))

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
                                    (sprintf
                                        "Publish to queue '%s' timedout after %ss"
                                        routingKey
                                        (timeout.TotalSeconds.ToString()))
                                    |> PublishResult.Timeout
                                )
                                |> ignore),
                        useSynchronizationContext = false
                    )

                let messageId = System.Guid.NewGuid().ToString()

                let basicReturnEventHandler = createBasicReturnEventHandler messageId tcs

                model.channelConsumer.Model.BasicReturn.AddHandler basicReturnEventHandler

                let (basicAckEventHandler, basicNackEventHandler) =
                    lock model (fun () ->
                        let nextPublishSeqNo = model.channelConsumer.Model.NextPublishSeqNo

                        let basicAckEventHandler = createBasicAckEventHandler nextPublishSeqNo tcs

                        model.channelConsumer.Model.BasicAcks.AddHandler basicAckEventHandler

                        let basicNackEventHandler = createBasicNackEventHandler nextPublishSeqNo tcs

                        model.channelConsumer.Model.BasicNacks.AddHandler basicNackEventHandler

                        model.channelConsumer.Model.BasicPublish(
                            exchange = "",
                            routingKey = routingKey,
                            mandatory = true,
                            basicProperties =
                                model.channelConsumer.Model.CreateBasicProperties(
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

                        (basicAckEventHandler, basicNackEventHandler))

                let! publishResult = tcs.Task |> (Async.AwaitTask >> Async.Catch)

                model.channelConsumer.Model.BasicAcks.RemoveHandler basicAckEventHandler

                model.channelConsumer.Model.BasicNacks.RemoveHandler basicNackEventHandler

                model.channelConsumer.Model.BasicReturn.RemoveHandler basicReturnEventHandler

                return
                    match publishResult with
                    | Choice1Of2 result -> result

                    | Choice2Of2 reason -> PublishResult.Unknown(sprintf "Task cancelled: %A" reason)
            }


    let replyToMessage: Model -> ReceivedMessage -> Map<string, string> -> Content -> Async<PublishResult> =
        fun (Model model) receivedMessage headers content ->
            receivedMessage
            |> extractReplyProperties
            |> AsyncResult.fromResult
            |> Async.map (function
                | Ok replyProperties ->
                    let headers = Map.add "sequence_end" "true" headers // sequence_end is required by Rabbot clients (https://github.com/arobson/rabbot/issues/76)

                    let (contentType, body) =
                        match content with
                        | Json jsonContent -> ("application/json", System.Text.Encoding.UTF8.GetBytes jsonContent)
                        | Binary bytes -> ("application/octet-stream", bytes)

                    let messageId = System.Guid.NewGuid().ToString()

                    model.channelConsumer.Model.BasicPublish(
                        exchange = "",
                        routingKey = replyProperties.ReplyTo,
                        // mandatory must be false when publishing to direct-reply-to queue https://www.rabbitmq.com/direct-reply-to.html#limitations
                        mandatory = false,
                        basicProperties =
                            model.channelConsumer.Model.CreateBasicProperties(
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

                    PublishResult.Acked
                | Error errorMessage -> PublishResult.Unknown errorMessage)

    /// <summary>Make an RPC-call to a RabbitMq queue.</summary>
    /// <param name="Model">MqClient model.</param>
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
                                    Error(
                                        sprintf
                                            "Publish to queue '%s' timedout after %ss"
                                            routingKey
                                            (timeout.TotalSeconds.ToString())
                                    )
                                )
                                |> ignore),
                        useSynchronizationContext = false
                    )

                try
                    if model.pendingRequests.TryAdd(messageId, tcs) then
                        model.rpcConsumer.Model.BasicPublish(
                            exchange = "",
                            routingKey = routingKey,
                            mandatory = true,
                            basicProperties =
                                model.rpcConsumer.Model.CreateBasicProperties(
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

                        let! result = tcs.Task |> (Async.AwaitTask >> Async.Catch)

                        return
                            match result with
                            | Choice1Of2 result -> result

                            | Choice2Of2 reason -> Error reason.Message
                    else
                        return Error(sprintf "Duplicate message id: %s" messageId)

                with
                | :? System.ArgumentNullException as ex -> return Error ex.Message

                | :? System.OverflowException as ex -> return Error ex.Message
            }

    /// <summary>Wrap callbacks with default implementation for OnRegistered,
    /// OnUnregistered, OnConsumerCancelled, OnShutdown. If any message is recieved
    /// for OnConsumerCancelled or OnShutdown the system will exit with error code 9</summary>
    /// <param name="LogError">Logger.</param>
    /// <param name="ReceivedMessage">Callback that's called when a new message is recieved.</param>
    /// <returns>Callbacks</returns>
    let terminateOnFailureWrapper: LogError -> (ReceivedMessage -> Async<unit>) -> Callbacks =
        fun logError onReceived ->
            { OnReceived =
                fun message ->
                    // Somthing weird happens with exception handeling when not
                    // using Async computational expression. Some exepctions are
                    // silenty swollowed and never bubbles up.
                    async {
                        try
                            do! onReceived message
                        with
                        | exn ->
                            logError (exn, (sprintf "💥 Unexpected error. %A\nShutting down" exn), ())
                            exit 9
                    }

              OnRegistered = fun _ -> Async.singleton ()

              OnUnregistered =
                  fun _ ->
                      logError (null, "Got OnUnregistered event", ())
                      exit 10

              OnConsumerCancelled =
                  fun _ ->
                      logError (null, "Got OnConsumerCancelled event", ())
                      exit 11

              OnShutdown =
                  fun _ ->
                      logError (null, "Got OnShutdown event", ())
                      exit 12 }
