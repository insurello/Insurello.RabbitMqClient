module Insurello.RabbitMqClient.Tests.ConnectionClosedTest

open Insurello.RabbitMqClient
open System.Threading.Tasks
open Xunit

type private RequestResult = Result<RabbitMqClient.RPC.ResponseMessage, RabbitMqClient.RPC.RequestError>

let private managementApiClient =
    ManagementApiClient.init Helpers.managementPort Helpers.nodeEndpoints

/// Nothing consumes the queue, so the request will never get a reply.
let private requestWithoutReply (rpcClient: RabbitMqClient.RPC.Client) : Async<RequestResult> =
    rpcClient
    |> RabbitMqClient.RPC.request {
        queue = Helpers.uniqueName "no-consumer"
        headers = []
        body = RabbitMqClient.RPC.Json "{}"
        timeout = System.TimeSpan.FromSeconds 30.
    }

let inline private expectConnectionInRecoveryMode (expectedReplyCode: int) (result: RequestResult) : unit =
    match result with
    | Error (RabbitMqClient.RPC.ConnectionInRecoveryMode details) -> Assert.Equal (expectedReplyCode, details.replyCode)

    | other -> Assert.Fail $"Expected ConnectionInRecoveryMode, got %A{other}"

let inline private expectWithin (limit: System.TimeSpan) (stopwatch: System.Diagnostics.Stopwatch) : unit =
    Assert.True (stopwatch.Elapsed < limit, $"Expected to take less than %O{limit}, took %O{stopwatch.Elapsed}")

let private publish (publishClient: RabbitMqClient.Publish.Client) (queueName: string) : Async<unit> =
    async {
        let! result =
            publishClient
            |> RabbitMqClient.Publish.publish {
                queue = queueName
                headers = []
                body = RabbitMqClient.Publish.Json "{}"
                timeout = System.TimeSpan.FromSeconds 5.
            }

        match result with
        | Ok () -> ()
        | Error error -> Assert.Fail (RabbitMqClient.Publish.publishErrorToString error)
    }

let private consume
    (queueName: string)
    (onMessageReceived: RabbitMqClient.Consumer.ReceivedMessage -> Async<unit>)
    (connection: RabbitMqClient.Connection)
    : Async<unit> =
    connection
    |> RabbitMqClient.Consumer.init {
        queueName = queueName
        bindings = []
        messageTimeToLive = None
        prefetchCount = 1us
        queueType = RabbitMqClient.Consumer.Quorum
        onMessageReceived = onMessageReceived
    }
    |> Async.Ignore

[<Fact>]
let ``Pending RPC request fails immediately when the broker forces the connection to close`` () =
    async {
        let connectionName = Helpers.uniqueName "rpc-forced-close"

        use! connection =
            Helpers.connectionConfig connectionName
            |> RabbitMqClient.Connection.init Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance

        let! rpcClient = RabbitMqClient.RPC.init "TestRpcClient" connection

        let! pendingRequest = requestWithoutReply rpcClient |> Async.StartChild

        do! managementApiClient.forceCloseConnection connectionName

        let stopwatch = System.Diagnostics.Stopwatch.StartNew ()

        let! result = pendingRequest

        result |> expectConnectionInRecoveryMode 320

        stopwatch |> expectWithin (System.TimeSpan.FromSeconds 5.)
    }

[<Fact>]
let ``Closing the connection while a message handler waits for an RPC reply doesn't block or throw`` () =
    let connectionName = Helpers.uniqueName "graceful-close"
    let queueName = Helpers.uniqueName "graceful-close"

    async {
        try
            let! connection =
                Helpers.connectionConfig connectionName
                |> RabbitMqClient.Connection.init Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance

            let! rpcClient = RabbitMqClient.RPC.init "TestRpcClient" connection
            let! publishClient = RabbitMqClient.Publish.init "TestPublishClient" connection

            let handlerStarted =
                TaskCompletionSource<unit> TaskCreationOptions.RunContinuationsAsynchronously

            let handlerResult =
                TaskCompletionSource<RequestResult> TaskCreationOptions.RunContinuationsAsynchronously

            do!
                connection
                |> consume
                    queueName
                    (fun _ ->
                        async {
                            handlerStarted.TrySetResult () |> ignore

                            let! result = requestWithoutReply rpcClient

                            handlerResult.TrySetResult result |> ignore
                        }
                    )

            do! publish publishClient queueName

            do!
                handlerStarted.Task.WaitAsync (System.TimeSpan.FromSeconds 10.)
                |> Async.AwaitTask

            // Must not wait for the pending RPC request to time out (30 s), which also made closing throw `TaskCanceledException`.
            let stopwatch = System.Diagnostics.Stopwatch.StartNew ()

            RabbitMqClient.Connection.close connection

            stopwatch |> expectWithin (System.TimeSpan.FromSeconds 5.)

            let! result = handlerResult.Task.WaitAsync (System.TimeSpan.FromSeconds 5.) |> Async.AwaitTask

            result |> expectConnectionInRecoveryMode 200
        finally
            managementApiClient.deleteQueue queueName |> Async.RunSynchronously
    }

[<Fact>]
let ``isConnectionStillOpen is false while the connection is closed and true again after recovery`` () =
    let connectionName = Helpers.uniqueName "recovery"
    let queueName = Helpers.uniqueName "recovery"

    async {
        try
            use! connection =
                Helpers.connectionConfig connectionName
                |> RabbitMqClient.Connection.init Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance

            let! publishClient = RabbitMqClient.Publish.init "TestPublishClient" connection

            let deliveries = ref 0

            let firstDeliveryStarted =
                TaskCompletionSource<unit> TaskCreationOptions.RunContinuationsAsynchronously

            let firstDeliverySawClosed =
                TaskCompletionSource<bool> TaskCreationOptions.RunContinuationsAsynchronously

            let redeliverySawOpen =
                TaskCompletionSource<bool> TaskCreationOptions.RunContinuationsAsynchronously

            let rec waitUntilClosed
                (message: RabbitMqClient.Consumer.ReceivedMessage)
                (attemptsLeft: int)
                : Async<unit> =
                async {
                    if RabbitMqClient.Consumer.isConnectionStillOpen message && attemptsLeft > 0 then
                        do! Async.Sleep 50
                        return! waitUntilClosed message (attemptsLeft - 1)
                }

            do!
                connection
                |> consume
                    queueName
                    (fun message ->
                        async {
                            // The redelivery can arrive while the first delivery's handler is still running, so count atomically.
                            if System.Threading.Interlocked.Increment &deliveries.contents = 1 then
                                firstDeliveryStarted.TrySetResult () |> ignore

                                // Don't ack, the broker requeues the message when it closes the connection.
                                do! waitUntilClosed message 400

                                firstDeliverySawClosed.TrySetResult (
                                    not (RabbitMqClient.Consumer.isConnectionStillOpen message)
                                )
                                |> ignore

                            else
                                redeliverySawOpen.TrySetResult (RabbitMqClient.Consumer.isConnectionStillOpen message)
                                |> ignore

                                do! RabbitMqClient.Consumer.ack message
                        }
                    )

            do! publish publishClient queueName

            do!
                firstDeliveryStarted.Task.WaitAsync (System.TimeSpan.FromSeconds 10.)
                |> Async.AwaitTask

            do! managementApiClient.forceCloseConnection connectionName

            let! sawClosed =
                firstDeliverySawClosed.Task.WaitAsync (System.TimeSpan.FromSeconds 25.)
                |> Async.AwaitTask

            Assert.True (sawClosed, "Expected isConnectionStillOpen to be false after the connection was closed")

            let! sawOpen =
                redeliverySawOpen.Task.WaitAsync (System.TimeSpan.FromSeconds 30.)
                |> Async.AwaitTask

            Assert.True (
                sawOpen,
                "Expected isConnectionStillOpen to be true for the message redelivered after recovery"
            )
        finally
            managementApiClient.deleteQueue queueName |> Async.RunSynchronously
    }
