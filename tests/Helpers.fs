[<AutoOpen>]
module Helpers

open PostgresMQ
open Npgsql
open Dapper

let messageQueue maxDeliveryCount =
  let connectionString =
    "Host=localhost;Username=postgres;Password=postgres;Database=postgres"

  let jsonSerializerOptions = Json.options

  let messageQueue =
    MessageQueue(connectionString, maxDeliveryCount, jsonSerializerOptions)

  let connection = new NpgsqlConnection(connectionString)
  connection.Execute "TRUNCATE TABLE message;" |> ignore
  messageQueue
