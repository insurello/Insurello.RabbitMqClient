module Insurello.RabbitMqClient.Tests.Helpers

open Insurello.RabbitMqClient

let inline private envOrDefault (name: string) (defaultValue: string) : string =
    match System.Environment.GetEnvironmentVariable name with
    | null
    | "" -> defaultValue
    | value -> value

/// Defaults to the local development cluster. Override with environment variables, e.g. in CI.
let nodeEndpoints: RabbitMqClient.Connection.NodeEndpointsConfig = {
    username = envOrDefault "RABBITMQ_USERNAME" "guest"
    password = envOrDefault "RABBITMQ_PASSWORD" "guest"
    vhost = envOrDefault "RABBITMQ_VHOST" "quorum-vhost"
    endpoints = [
        {
            host = envOrDefault "RABBITMQ_HOST" "localhost"
            port = envOrDefault "RABBITMQ_PORT" "5672" |> int
        }
    ]
}

let managementPort: int = int (envOrDefault "RABBITMQ_MANAGEMENT_PORT" "15672")

let connectionConfig (connectionName: string) : RabbitMqClient.Connection.Config = {
    name = connectionName
    nodeEndpoints = nodeEndpoints
    connectionTimeout = System.TimeSpan.FromSeconds 10.
    heartbeat = System.TimeSpan.FromSeconds 15.
    recoveryInterval = System.TimeSpan.FromSeconds 1.
    closeTimeout = System.TimeSpan.FromSeconds 10.
    onUnexpectedEvent = fun _ -> async.Return ()
}

/// Unique names prevent tests from affecting each other, and leftovers from previous runs.
let uniqueName (prefix: string) : string =
    prefix + "-" + System.Guid.NewGuid().ToString "N"
