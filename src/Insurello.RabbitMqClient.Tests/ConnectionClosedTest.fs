module Insurello.RabbitMqClient.Tests.ConnectionClosedTest

open Insurello.RabbitMqClient
open System.Threading.Tasks
open Xunit

type private RequestResult = Result<RabbitMqClient.RPC.ResponseMessage, RabbitMqClient.RPC.RequestError>

let private managementApiClient =
    ManagementApiClient.init Helpers.managementPort Helpers.nodeEndpoints

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

/// Consumes RPC requests without ever replying, so they stay pending until the requester's connection closes.
/// Pass a separate connection, so closing the requester's connection doesn't affect it.
/// The returned task completes when a request has arrived, i.e. it has been published and is waiting for a reply.
let private startSilentResponder
    (requestQueueName: string)
    (connection: RabbitMqClient.Connection)
    : Async<Task<unit>> =
    async {
        let requestReceived =
            TaskCompletionSource<unit> TaskCreationOptions.RunContinuationsAsynchronously

        do!
            connection
            |> consume
                requestQueueName
                (fun message ->
                    async {
                        requestReceived.TrySetResult () |> ignore

                        do! RabbitMqClient.Consumer.ack message
                    }
                )

        return requestReceived.Task
    }

let private requestWithoutReply
    (requestQueueName: string)
    (rpcClient: RabbitMqClient.RPC.Client)
    : Async<RequestResult> =
    rpcClient
    |> RabbitMqClient.RPC.request {
        queue = requestQueueName
        headers = []
        body = RabbitMqClient.RPC.Json "{}"
        timeout = System.TimeSpan.FromSeconds 30.
    }

[<Fact>]
let ``Pending RPC request fails immediately when the broker forces the connection to close`` () =
    let connectionName = Helpers.uniqueName "rpc-forced-close"
    let requestQueueName = Helpers.uniqueName "rpc-forced-close-requests"

    async {
        try
            use! responderConnection =
                Helpers.connectionConfig (Helpers.uniqueName "silent-responder")
                |> RabbitMqClient.Connection.init Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance

            let! requestReceived = responderConnection |> startSilentResponder requestQueueName

            use! connection =
                Helpers.connectionConfig connectionName
                |> RabbitMqClient.Connection.init Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance

            let! rpcClient = RabbitMqClient.RPC.init "TestRpcClient" connection

            let! pendingRequest = rpcClient |> requestWithoutReply requestQueueName |> Async.StartChild

            do! requestReceived.WaitAsync (System.TimeSpan.FromSeconds 10.) |> Async.AwaitTask

            do! managementApiClient.forceCloseConnection connectionName

            let stopwatch = System.Diagnostics.Stopwatch.StartNew ()

            let! result = pendingRequest

            result |> expectConnectionInRecoveryMode 320

            stopwatch |> expectWithin (System.TimeSpan.FromSeconds 5.)
        finally
            managementApiClient.deleteQueue requestQueueName |> Async.RunSynchronously
    }

[<Fact>]
let ``Closing the connection while a message handler waits for an RPC reply doesn't block or throw`` () =
    let connectionName = Helpers.uniqueName "graceful-close"
    let queueName = Helpers.uniqueName "graceful-close"
    let requestQueueName = Helpers.uniqueName "graceful-close-requests"

    async {
        try
            use! responderConnection =
                Helpers.connectionConfig (Helpers.uniqueName "silent-responder")
                |> RabbitMqClient.Connection.init Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance

            let! requestReceived = responderConnection |> startSilentResponder requestQueueName

            let! connection =
                Helpers.connectionConfig connectionName
                |> RabbitMqClient.Connection.init Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance

            let! rpcClient = RabbitMqClient.RPC.init "TestRpcClient" connection
            let! publishClient = RabbitMqClient.Publish.init "TestPublishClient" connection

            let handlerResult =
                TaskCompletionSource<RequestResult> TaskCreationOptions.RunContinuationsAsynchronously

            do!
                connection
                |> consume
                    queueName
                    (fun _ ->
                        async {
                            let! result = rpcClient |> requestWithoutReply requestQueueName

                            handlerResult.TrySetResult result |> ignore
                        }
                    )

            do! publish publishClient queueName

            do! requestReceived.WaitAsync (System.TimeSpan.FromSeconds 10.) |> Async.AwaitTask

            // Must not wait for the pending RPC request to time out (30 s), which also made closing throw `TaskCanceledException`.
            let stopwatch = System.Diagnostics.Stopwatch.StartNew ()

            RabbitMqClient.Connection.close connection

            stopwatch |> expectWithin (System.TimeSpan.FromSeconds 5.)

            let! result = handlerResult.Task.WaitAsync (System.TimeSpan.FromSeconds 5.) |> Async.AwaitTask

            result |> expectConnectionInRecoveryMode 200
        finally
            managementApiClient.deleteQueue queueName |> Async.RunSynchronously
            managementApiClient.deleteQueue requestQueueName |> Async.RunSynchronously
    }

[<Fact>]
let ``Nack is skipped while the connection is closed and nacks again after recovery`` () =
    let connectionName = Helpers.uniqueName "recovery"
    let queueName = Helpers.uniqueName "recovery"

    async {
        try
            // Stay closed long enough for the handler to nack before the connection is recovered.
            use! connection =
                {
                    Helpers.connectionConfig connectionName with
                        recoveryInterval = System.TimeSpan.FromSeconds 5.
                }
                |> RabbitMqClient.Connection.init Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance

            let! publishClient = RabbitMqClient.Publish.init "TestPublishClient" connection

            let deliveries = ref 0

            let firstDeliveryStarted =
                TaskCompletionSource<unit> TaskCreationOptions.RunContinuationsAsynchronously

            let whileClosed =
                TaskCompletionSource<bool * RabbitMqClient.Consumer.NackOutcome>
                    TaskCreationOptions.RunContinuationsAsynchronously

            let afterRecovery =
                TaskCompletionSource<bool * RabbitMqClient.Consumer.NackOutcome>
                    TaskCreationOptions.RunContinuationsAsynchronously

            let nackedMessageRedelivered =
                TaskCompletionSource<unit> TaskCreationOptions.RunContinuationsAsynchronously

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
                            let delivery = System.Threading.Interlocked.Increment &deliveries.contents

                            if delivery = 1 then
                                firstDeliveryStarted.TrySetResult () |> ignore

                                // Don't ack, the broker requeues the message when it closes the connection.
                                do! waitUntilClosed message 400

                                let isOpen = RabbitMqClient.Consumer.isConnectionStillOpen message
                                let! nackResult = RabbitMqClient.Consumer.tryNack message

                                whileClosed.TrySetResult ((isOpen, nackResult)) |> ignore

                            else if delivery = 2 then
                                let isOpen = RabbitMqClient.Consumer.isConnectionStillOpen message
                                let! nackResult = RabbitMqClient.Consumer.tryNack message

                                afterRecovery.TrySetResult ((isOpen, nackResult)) |> ignore

                            else
                                nackedMessageRedelivered.TrySetResult () |> ignore

                                do! RabbitMqClient.Consumer.ack message
                        }
                    )

            do! publish publishClient queueName

            do!
                firstDeliveryStarted.Task.WaitAsync (System.TimeSpan.FromSeconds 10.)
                |> Async.AwaitTask

            do! managementApiClient.forceCloseConnection connectionName

            let! isOpenWhileClosed, nackResultWhileClosed =
                whileClosed.Task.WaitAsync (System.TimeSpan.FromSeconds 25.) |> Async.AwaitTask

            Assert.False (
                isOpenWhileClosed,
                "Expected isConnectionStillOpen to be false while the connection is closed"
            )

            Assert.Equal (RabbitMqClient.Consumer.NackOutcome.SkippedDueToConnectionClosed, nackResultWhileClosed)

            let! isOpenAfterRecovery, nackResultAfterRecovery =
                afterRecovery.Task.WaitAsync (System.TimeSpan.FromSeconds 30.)
                |> Async.AwaitTask

            Assert.True (isOpenAfterRecovery, "Expected isConnectionStillOpen to be true after recovery")
            Assert.Equal (RabbitMqClient.Consumer.NackOutcome.Nacked, nackResultAfterRecovery)

            do!
                nackedMessageRedelivered.Task.WaitAsync (System.TimeSpan.FromSeconds 10.)
                |> Async.AwaitTask
        finally
            managementApiClient.deleteQueue queueName |> Async.RunSynchronously
    }

[<Fact>]
let ``Cancelling the caller doesn't cancel a delayed nack`` () =
    let connectionName = Helpers.uniqueName "delayed-nack-cancelled"
    let queueName = Helpers.uniqueName "delayed-nack-cancelled"

    async {
        try
            use! connection =
                Helpers.connectionConfig connectionName
                |> RabbitMqClient.Connection.init Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance

            let! publishClient = RabbitMqClient.Publish.init "TestPublishClient" connection

            let deliveries = ref 0

            let delayedNackOutcome =
                TaskCompletionSource<RabbitMqClient.Consumer.DelayedNackOutcome>
                    TaskCreationOptions.RunContinuationsAsynchronously

            let nackedMessageRedelivered =
                TaskCompletionSource<unit> TaskCreationOptions.RunContinuationsAsynchronously

            do!
                connection
                |> consume
                    queueName
                    (fun message ->
                        async {
                            if System.Threading.Interlocked.Increment &deliveries.contents = 1 then
                                use cancellationTokenSource = new System.Threading.CancellationTokenSource ()

                                let! outcome =
                                    Async.StartAsTask (
                                        RabbitMqClient.Consumer.tryNackWithDelayNonBlocking
                                            (System.TimeSpan.FromSeconds 2.)
                                            message,
                                        cancellationToken = cancellationTokenSource.Token
                                    )
                                    |> Async.AwaitTask

                                cancellationTokenSource.Cancel ()

                                delayedNackOutcome.TrySetResult outcome |> ignore

                            else
                                nackedMessageRedelivered.TrySetResult () |> ignore

                                do! RabbitMqClient.Consumer.ack message
                        }
                    )

            do! publish publishClient queueName

            let! outcome =
                delayedNackOutcome.Task.WaitAsync (System.TimeSpan.FromSeconds 10.)
                |> Async.AwaitTask

            Assert.Equal (RabbitMqClient.Consumer.DelayedNackOutcome.NackScheduled, outcome)

            // Only redelivered if the nack still happened after the caller was cancelled.
            do!
                nackedMessageRedelivered.Task.WaitAsync (System.TimeSpan.FromSeconds 10.)
                |> Async.AwaitTask
        finally
            managementApiClient.deleteQueue queueName |> Async.RunSynchronously
    }

[<Fact>]
let ``A delayed nack logs a warning when the connection closes during the delay`` () =
    let connectionName = Helpers.uniqueName "delayed-nack-closed"
    let queueName = Helpers.uniqueName "delayed-nack-closed"

    async {
        try
            let logger = CapturingLogger.CapturingLogger ()

            // Stay closed until after the delayed nack, which runs 2 seconds after the message is received.
            use! connection =
                {
                    Helpers.connectionConfig connectionName with
                        recoveryInterval = System.TimeSpan.FromSeconds 5.
                }
                |> RabbitMqClient.Connection.init logger

            let! publishClient = RabbitMqClient.Publish.init "TestPublishClient" connection

            let delayedNackOutcome =
                TaskCompletionSource<RabbitMqClient.Consumer.DelayedNackOutcome>
                    TaskCreationOptions.RunContinuationsAsynchronously

            do!
                connection
                |> consume
                    queueName
                    (fun message ->
                        async {
                            let! outcome =
                                RabbitMqClient.Consumer.tryNackWithDelayNonBlocking
                                    (System.TimeSpan.FromSeconds 2.)
                                    message

                            delayedNackOutcome.TrySetResult outcome |> ignore
                        }
                    )

            do! publish publishClient queueName

            let! outcome =
                delayedNackOutcome.Task.WaitAsync (System.TimeSpan.FromSeconds 10.)
                |> Async.AwaitTask

            Assert.Equal (RabbitMqClient.Consumer.DelayedNackOutcome.NackScheduled, outcome)

            do! managementApiClient.forceCloseConnection connectionName

            do! logger |> CapturingLogger.waitForWarning "Unable to nack message"

            do! logger |> CapturingLogger.waitForWarning "because the connection is closed"
        finally
            managementApiClient.deleteQueue queueName |> Async.RunSynchronously
    }
