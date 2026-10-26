/// The RabbitMQ management HTTP API, for things the client library can't do.
module Insurello.RabbitMqClient.Tests.ManagementApiClient

open Insurello.RabbitMqClient

type Client = {
    /// Makes the broker close the connection with 320 CONNECTION_FORCED, the same as when a node is restarted.
    forceCloseConnection: string -> Async<unit>
    deleteQueue: string -> Async<unit>
}

let private forceCloseConnection (httpClient: System.Net.Http.HttpClient) (connectionName: string) : Async<unit> =
    let tryFindServerConnectionName (json: string) : Option<string> =
        use document = System.Text.Json.JsonDocument.Parse json

        document.RootElement.EnumerateArray ()
        |> Seq.tryPick (fun connection ->
            match connection.TryGetProperty "client_properties" with
            | true, clientProperties ->
                match clientProperties.TryGetProperty "connection_name" with
                | true, name ->
                    if name.GetString () = connectionName then
                        Some (connection.GetProperty("name").GetString ())
                    else
                        None

                | _ -> None

            | _ -> None
        )

    // The management API lists new connections with a delay, so poll until the connection shows up.
    let rec findServerConnectionName (attemptsLeft: int) : Async<string> =
        async {
            let! json =
                httpClient.GetStringAsync "connections?columns=name,client_properties"
                |> Async.AwaitTask

            match tryFindServerConnectionName json with
            | Some serverConnectionName -> return serverConnectionName

            | None ->
                if attemptsLeft = 0 then
                    return failwith $"Connection %s{connectionName} was not found in the management API"
                else
                    do! Async.Sleep 200
                    return! findServerConnectionName (attemptsLeft - 1)
        }

    async {
        let! serverConnectionName = findServerConnectionName 100

        let! response =
            httpClient.DeleteAsync $"connections/%s{System.Uri.EscapeDataString serverConnectionName}"
            |> Async.AwaitTask

        response.EnsureSuccessStatusCode () |> ignore
    }

let private deleteQueue (httpClient: System.Net.Http.HttpClient) (vhost: string) (queueName: string) : Async<unit> =
    async {
        let! response =
            httpClient.DeleteAsync
                $"queues/%s{System.Uri.EscapeDataString vhost}/%s{System.Uri.EscapeDataString queueName}"
            |> Async.AwaitTask

        response.EnsureSuccessStatusCode () |> ignore
    }

/// Uses the same node, vhost and credentials as the connections.
let init (managementPort: int) (nodeEndpoints: RabbitMqClient.Connection.NodeEndpointsConfig) : Client =
    let endpoint = List.head nodeEndpoints.endpoints

    let httpClient =
        new System.Net.Http.HttpClient (BaseAddress = System.Uri $"http://%s{endpoint.host}:%d{managementPort}/api/")

    httpClient.DefaultRequestHeaders.Authorization <-
        System.Net.Http.Headers.AuthenticationHeaderValue (
            "Basic",
            System.Convert.ToBase64String (
                System.Text.Encoding.UTF8.GetBytes $"%s{nodeEndpoints.username}:%s{nodeEndpoints.password}"
            )
        )

    {
        forceCloseConnection = forceCloseConnection httpClient
        deleteQueue = deleteQueue httpClient nodeEndpoints.vhost
    }
