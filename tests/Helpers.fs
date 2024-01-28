[<AutoOpen>]
module Helpers

open PostgresMQ
open Npgsql
open Dapper

let messageQueue () =
  let connectionString =
    "Host=localhost;Username=postgres;Password=postgres;Database=postgres"

  let maxDeliveryCount = 1
  let jsonSerializerOptions = Json.options

  let messageQueue =
    MessageQueue(connectionString, maxDeliveryCount, jsonSerializerOptions)

  let connection = new NpgsqlConnection(connectionString)
  connection.Execute "TRUNCATE TABLE message;" |> ignore
  messageQueue
