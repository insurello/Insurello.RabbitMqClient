/// A logger that records its log entries, for asserting on what was logged.
module Insurello.RabbitMqClient.Tests.CapturingLogger

open Microsoft.Extensions.Logging

type CapturingLogger() =
    let entries = System.Collections.Concurrent.ConcurrentQueue<LogLevel * string> ()

    member _.Entries: List<LogLevel * string> = List.ofSeq entries

    interface ILogger with
        member _.BeginScope<'TState>(_state: 'TState) : System.IDisposable =
            { new System.IDisposable with
                member _.Dispose() = ()
            }

        member _.IsEnabled(_logLevel: LogLevel) : bool = true

        member _.Log<'TState>
            (
                logLevel: LogLevel,
                _eventId: EventId,
                state: 'TState,
                exn: exn,
                formatter: System.Func<'TState, exn, string>
            ) : unit =
            entries.Enqueue ((logLevel, formatter.Invoke (state, exn)))

/// Waits until a warning containing the text has been logged. Fails after 10 seconds.
let waitForWarning (containing: string) (logger: CapturingLogger) : Async<unit> =
    let rec poll (attemptsLeft: int) : Async<unit> =
        async {
            let found =
                logger.Entries
                |> List.exists (fun (level, message) -> level = LogLevel.Warning && message.Contains containing)

            if not found then
                if attemptsLeft = 0 then
                    return failwith $"No warning containing \"%s{containing}\" was logged. Logged: %A{logger.Entries}"
                else
                    do! Async.Sleep 100
                    return! poll (attemptsLeft - 1)
        }

    poll 100
