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
          pendingRequests: System.Collections.Concurrent.ConcurrentDictionary<string, Result<ReceivedMessage, string> System.Threading.Tasks.TaskCompletionSource> }

    and Message<'event> = private | Message of 'event * ModelData

    and ReceivedMessage = Message<BasicDeliverEventArgs>

    type RawBody = string

    type Model = private | Model of ModelData

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

    type PublishConfig =
        { Timeout: System.TimeSpan
          Endpoint: string
          ContentType: string
          CorrelationId: string
          Headers: Map<string, string>
          Body: byte [] }

    type ChannelConfig =
        { withConfirmSelect: bool }

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

    let extractReplyProperties: Message<BasicDeliverEventArgs> -> Result<{| ReplyTo: string; CorrelationId: string |}, string> =
        fun (Message(event, _)) ->
            event
            |> extractReplyTo
            |> Result.bind (fun replyTo ->
                extractMesssageId event |> Result.map (fun messageId ->
                                               {| ReplyTo = replyTo
                                                  CorrelationId = messageId |}))

    let routingKeyFromMessage: ReceivedMessage -> string = fun (Message(event, _)) -> event.RoutingKey

    let private asTask: ModelData -> 'event -> (Message<'event> -> Async<unit>) -> System.Threading.Tasks.Task =
        fun model event callback ->
            async { do! callback (Message(event, model)) } |> Async.StartAsTask :> System.Threading.Tasks.Task

    let private consumeQueue: Model -> string -> QueueTopology -> Model =
        fun (Model model) uniqueTag queueTopology ->

            let consumerTag = queueTopology.Queue + "-consumer-" + uniqueTag

            let doNothingTask: unit -> System.Threading.Tasks.Task =
                (fun () -> async.Return() |> Async.StartAsTask :> System.Threading.Tasks.Task)

            model.channelConsumer.add_Received (fun _sender event ->
                if event.ConsumerTag = consumerTag
                then asTask model event queueTopology.ConsumeCallbacks.OnReceived
                else doNothingTask())

            model.channelConsumer.add_Registered (fun _sender event ->
                if event.ConsumerTag = consumerTag
                then asTask model event queueTopology.ConsumeCallbacks.OnRegistered
                else doNothingTask())

            model.channelConsumer.add_Unregistered (fun _sender event ->
                if event.ConsumerTag = consumerTag
                then asTask model event queueTopology.ConsumeCallbacks.OnUnregistered
                else doNothingTask())

            model.channelConsumer.add_Shutdown
                (fun _sender event -> asTask model event queueTopology.ConsumeCallbacks.OnShutdown)

            model.channelConsumer.add_ConsumerCancelled (fun _sender event ->
                if event.ConsumerTag = consumerTag
                then asTask model event queueTopology.ConsumeCallbacks.OnConsumerCancelled
                else doNothingTask())

            model.channelConsumer.Model.BasicConsume
                (queue = queueTopology.Queue, autoAck = false, consumerTag = consumerTag, noLocal = false,
                 exclusive = false, arguments = null, consumer = model.channelConsumer) |> ignore

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

    let private closeConnection: IConnection -> unit =
        fun connection ->
            if connection.IsOpen then connection.Close() else ()

    let private closeConnectionAsync: System.TimeSpan -> IConnection -> unit =
        fun waitTimeout connection ->
            Async.Start
                (async { closeConnection connection }, (new System.Threading.CancellationTokenSource(waitTimeout)).Token)

    let private createChannel: ChannelConfig -> ExceptionCallback -> IConnection -> Result<IModel, string> =
        fun config exCallback connection ->
            try
                let model = connection.CreateModel()

                if config.withConfirmSelect then model.ConfirmSelect()

                model.CallbackException
                |> Event.add (fun event ->
                    let (hasContext, context) = event.Detail.TryGetValue "context"

                    exCallback event.Exception (if hasContext then context.ToString() else "") connection)

                Ok model
            with :? AlreadyClosedException as ex -> Error ex.Message

    let private declareQueue: Model -> QueueTopology -> Result<QueueTopology, string> =
        fun (Model model) queueTopology ->
            let name = nonNullString queueTopology.Queue

            let arguments =
                dict
                    (queueTopology.MessageTimeToLive
                     |> Option.map (fun ttl -> [ ("x-message-ttl", ttl :> obj) ])
                     |> Option.defaultValue [])
            try
                model.channelConsumer.Model.QueueDeclare
                    (queue = name, durable = true, exclusive = false, autoDelete = false, arguments = arguments)
                |> ignore

                Ok queueTopology
            with :? OperationInterruptedException as ex -> Error ex.Message

    let private bindQueueToExchange: Model -> QueueTopology -> Result<QueueTopology, string> =
        fun (Model model) queueTopology ->
            try
                match queueTopology.BindToExchange with
                | Some exchangeName ->
                    model.channelConsumer.Model.QueueBind
                        (queue = queueTopology.Queue, exchange = nonNullString exchangeName, routingKey = "*",
                         arguments = null) |> ignore
                | None -> ()
                Ok queueTopology
            with ex -> Error ex.Message

    let private dictRemoveMutable: 'key -> System.Collections.Concurrent.ConcurrentDictionary<'key, 'value> -> 'value option =
        fun key dict ->
            match dict.TryRemove key with
            | true, value -> Some value
            | _ -> None

    let private initReplyQueue: Model -> Model =
        fun (Model model) ->
            let queueName = "amq.rabbitmq.reply-to"
            let consumerTag = queueName + "-consumer-" + System.Guid.NewGuid().ToString()

            let onReceived: ReceivedMessage -> Async<unit> =
                (fun ((Message(event, _)) as message) ->
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
                    failwith "Got Shutdown event on rpc channel"
                    async.Return()))

            model.rpcConsumer.add_ConsumerCancelled (fun _sender event ->
                asTask model event (fun _ ->
                    failwith "Got ConsumerCancelled event on rpc channel"
                    async.Return()))

            model.rpcConsumer.Model.BasicConsume
                (queue = queueName, autoAck = true, consumerTag = consumerTag, noLocal = false, exclusive = false,
                 arguments = null, consumer = model.rpcConsumer) |> ignore // Must be true for direct-reply-to

            Model model

    let private createBasicReturnEventHandler: string -> System.Threading.Tasks.TaskCompletionSource<PublishResult> -> System.EventHandler<BasicReturnEventArgs>
        =
        fun messageId tcs ->
            System.EventHandler<BasicReturnEventArgs>(fun _ args ->
                if args.BasicProperties.MessageId = messageId then
                    tcs.TrySetResult
                        (PublishResult.ReturnError
                            (sprintf
                                "Failed to publish to queue: ReplyCode: %i, ReplyText: %s, Exchange: %s, RoutingKey: %s"
                                 args.ReplyCode args.ReplyText args.Exchange args.RoutingKey)) |> ignore
                else
                    ())

    let private createBasicAckEventHandler: uint64 -> System.Threading.Tasks.TaskCompletionSource<PublishResult> -> System.EventHandler<BasicAckEventArgs>
        =
        fun publishSeqNo tcs ->
            System.EventHandler<BasicAckEventArgs>(fun _ args ->
                if args.DeliveryTag = publishSeqNo
                then tcs.TrySetResult(PublishResult.Acked) |> ignore
                else ())

    let private createBasicNackEventHandler: uint64 -> System.Threading.Tasks.TaskCompletionSource<PublishResult> -> System.EventHandler<BasicNackEventArgs>
        =
        fun publishSeqNo tcs ->
            System.EventHandler<BasicNackEventArgs>(fun _ args ->
                if args.DeliveryTag = publishSeqNo
                then tcs.TrySetResult(PublishResult.Nacked) |> ignore
                else ())

    let ackMessage: ReceivedMessage -> unit =
        fun (Message(event, model)) ->
            model.channelConsumer.Model.BasicAck(deliveryTag = event.DeliveryTag, multiple = false)

    let nackMessage: ReceivedMessage -> unit =
        fun (Message(event, model)) ->
            model.channelConsumer.Model.BasicNack(deliveryTag = event.DeliveryTag, multiple = false, requeue = true)

    let nackMessageWithoutRequeue: ReceivedMessage -> unit =
        fun (Message(event, model)) ->
            model.channelConsumer.Model.BasicNack(deliveryTag = event.DeliveryTag, multiple = false, requeue = false)

    let messageBody: ReceivedMessage -> byte [] = fun (Message(event, _)) -> event.Body

    let messageBodyAsString: ReceivedMessage -> RawBody = messageBody >> System.Text.Encoding.UTF8.GetString

    let messageId: ReceivedMessage -> string = fun (Message(event, _)) -> event.BasicProperties.MessageId

    let init: LogError -> string -> System.Uri -> (Model -> Topology) -> Result<Model, string> =
        fun logError nameOfClient uri getTopology ->
            connect nameOfClient uri
            |> Result.bind (fun connection ->
                let exCallback =
                    (fun ex context connection ->
                        logError (ex, "Unhandled exception on channel in context {$c}", context)
                        closeConnectionAsync (System.TimeSpan.FromSeconds 3.0) connection)

                createChannel { withConfirmSelect = true } exCallback connection
                |> Result.bind (fun channel ->
                    createChannel { withConfirmSelect = false } exCallback connection
                    |> Result.map (fun rpcChannel ->
                        Model
                            { channelConsumer = AsyncEventingBasicConsumer channel

                              rpcConsumer = AsyncEventingBasicConsumer rpcChannel

                              pendingRequests =
                                  System.Collections.Concurrent.ConcurrentDictionary<string, Result<ReceivedMessage, string> System.Threading.Tasks.TaskCompletionSource>
                                      () })))
            |> Result.bind (fun model ->
                let declareAQueue = declareQueue model
                let bindAQueue = bindQueueToExchange model
                let consumeAQueue = consumeQueue model (System.Guid.NewGuid().ToString())

                getTopology model
                |> List.fold (fun prevResult queueTopology ->
                    Result.bind (fun _ ->
                        declareAQueue queueTopology
                        |> Result.bind bindAQueue
                        |> Result.map consumeAQueue) prevResult) (Ok model)
                |> Result.map initReplyQueue)

    /// Will publish with confirm.
    let publishToQueue: Model -> PublishConfig -> Async<PublishResult> =
        fun (Model model) config ->
            async {
                let tcs = System.Threading.Tasks.TaskCompletionSource<PublishResult>()
                use ct = new System.Threading.CancellationTokenSource(config.Timeout)
                use _ctr =
                    ct.Token.Register
                        (callback =
                            (fun () ->
                                tcs.SetResult
                                    ((sprintf "Publish to queue '%s' timedout after %ss" config.Endpoint
                                          (config.Timeout.TotalSeconds.ToString())) |> PublishResult.Timeout) |> ignore),
                         useSynchronizationContext = false)

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

                        model.channelConsumer.Model.BasicPublish
                            (exchange = "", routingKey = config.Endpoint, mandatory = true,
                             basicProperties =
                                 model.channelConsumer.Model.CreateBasicProperties
                                     (ContentType = config.ContentType, Persistent = true, MessageId = messageId,
                                      CorrelationId = config.CorrelationId,
                                      Headers =
                                          (config.Headers
                                           |> Map.map (fun _ v -> v :> obj)
                                           |> (Map.toSeq >> dict))), body = config.Body)

                        (basicAckEventHandler, basicNackEventHandler))

                let! publishResult = tcs.Task |> (Async.AwaitTask >> Async.Catch)

                model.channelConsumer.Model.BasicAcks.RemoveHandler basicAckEventHandler
                model.channelConsumer.Model.BasicNacks.RemoveHandler basicNackEventHandler
                model.channelConsumer.Model.BasicReturn.RemoveHandler basicReturnEventHandler

                return match publishResult with
                       | Choice1Of2 result -> result

                       | Choice2Of2 reason -> PublishResult.Unknown(sprintf "Task cancelled: %A" reason)
            }

    type Content =
        | Json of string
        | Binary of byte array

    let replyToMessage: Model -> ReceivedMessage -> (string * string) list -> Content -> Async<PublishResult> =
        fun (Model model) receivedMessage headers content ->
            receivedMessage
            |> extractReplyProperties
            |> AsyncResult.fromResult
            |> Async.map (function
                | Ok replyProperties ->
                    let headers = Map([ ("sequence_end", "true") ] @ headers) // sequence_end is required by Rabbot clients (https://github.com/arobson/rabbot/issues/76)

                    let (contentType, body) =
                        match content with
                        | Json jsonContent -> ("application/json", System.Text.Encoding.UTF8.GetBytes jsonContent)
                        | Binary bytes -> ("application/octet-stream", bytes)

                    let messageId = System.Guid.NewGuid().ToString()

                    model.channelConsumer.Model.BasicPublish
                        (exchange = "", routingKey = replyProperties.ReplyTo,
                         // mandatory must be false when publishing to direct-reply-to queue https://www.rabbitmq.com/direct-reply-to.html#limitations
                         mandatory = false,
                         basicProperties =
                             model.channelConsumer.Model.CreateBasicProperties
                                 (ContentType = contentType, Persistent = true, MessageId = messageId,
                                  CorrelationId = replyProperties.CorrelationId,
                                  Headers =
                                      (headers
                                       |> Map.map (fun _ v -> v :> obj)
                                       |> (Map.toSeq >> dict))), body = body)

                    PublishResult.Acked
                | Error errorMessage -> PublishResult.Unknown errorMessage)

    /// <summary>Make an RPC-call to a RabbitMq queue.</summary>
    /// <param name="Model">MqClient model.</param>
    /// <param name="config">Config for where to publish and what to publish.</param>
    /// <returns>Response from called RPC endpoint or error.</returns>
    let request: Model -> PublishConfig -> AsyncResult<ReceivedMessage, string> =
        fun (Model model) config ->
            async {
                let tcs = System.Threading.Tasks.TaskCompletionSource<Result<ReceivedMessage, string>>()
                use ct = new System.Threading.CancellationTokenSource(config.Timeout)

                let messageId = System.Guid.NewGuid().ToString()

                use _ctr =
                    ct.Token.Register
                        (callback =
                            (fun () ->
                                dictRemoveMutable messageId model.pendingRequests |> ignore

                                tcs.TrySetResult
                                    (Error
                                        (sprintf "Publish to queue '%s' timedout after %ss" config.Endpoint
                                             (config.Timeout.TotalSeconds.ToString()))) |> ignore),
                         useSynchronizationContext = false)

                try
                    if model.pendingRequests.TryAdd(messageId, tcs) then
                        model.rpcConsumer.Model.BasicPublish
                            (exchange = "", routingKey = config.Endpoint, mandatory = true,
                             basicProperties =
                                 model.rpcConsumer.Model.CreateBasicProperties
                                     (ContentType = config.ContentType, Persistent = false, MessageId = messageId,
                                      ReplyTo = "amq.rabbitmq.reply-to",
                                      Headers =
                                          (config.Headers
                                           |> Map.map (fun _ v -> v :> obj)
                                           |> (Map.toSeq >> dict))), body = config.Body)

                        let! result = tcs.Task |> (Async.AwaitTask >> Async.Catch)

                        return match result with
                               | Choice1Of2 result -> result

                               | Choice2Of2 reason -> Error reason.Message
                    else
                        return Error(sprintf "Duplicate message id: %s" messageId)

                with
                | :? System.ArgumentNullException as ex -> return Error ex.Message

                | :? System.OverflowException as ex -> return Error ex.Message
            }

    let failwithWrapper: LogError -> (ReceivedMessage -> Async<unit>) -> Callbacks =
        fun logError onReceived ->
            { OnReceived =
                  fun message ->
                      onReceived message
                      |> Async.Catch
                      |> Async.map (function
                          | Choice1Of2 _ -> ()
                          | Choice2Of2 err ->
                              logError
                                  (err,
                                   (sprintf "💥 Unexpected error. %A\nShutting down" err), ())

                              exit 2)

              OnRegistered = fun _ -> Async.singleton()

              OnUnregistered =
                  fun _ ->
                      failwith "Got OnUnregistered event"
                      Async.singleton()

              OnConsumerCancelled =
                  fun _ ->
                      failwith "Got OnConsumerCancelled event"
                      Async.singleton()

              OnShutdown =
                  fun _ ->
                      failwith "Got OnShutdown event"
                      Async.singleton() }
