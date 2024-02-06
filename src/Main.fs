namespace PostgresMQ

open Dapper
open Npgsql
open System
open System.Text.Json
open System.Threading.Tasks

type Message<'T> =
  { Id: string
    DeliveryCount: int
    PartitionKey: string
    Body: 'T }

type MessageQueue(connectionString: string, maxDeliveryCount: int, ?jsonSerializerOptions: JsonSerializerOptions) =
  let connection () = new NpgsqlConnection(connectionString)

  let jsonSerializerOptions =
    JsonSerializerOptions() |> defaultArg jsonSerializerOptions

  do
    use connection = connection ()

    connection.Execute
      """
    CREATE TABLE IF NOT EXISTS message (
      body jsonb NOT NULL,
      id CHAR(36) NOT NULL,
      deliverycount INT NOT NULL,
      partitionKey VARCHAR,
      completed TIMESTAMP,
      scheduledEnqueueTimeUtc TIMESTAMP,
      created TIMESTAMP NOT NULL
    );
    """
    |> ignore

    connection.Execute "CREATE UNIQUE INDEX IF NOT EXISTS idx_message_id ON message (id);"
    |> ignore

    connection.Execute "CREATE INDEX IF NOT EXISTS idx_message_partitionKey ON message (partitionKey);"
    |> ignore

    connection.Execute "CREATE INDEX IF NOT EXISTS idx_message_completed ON message (completed);"
    |> ignore

    connection.Execute
      "CREATE INDEX IF NOT EXISTS idx_message_scheduledEnqueueTimeUtc ON message (scheduledEnqueueTimeUtc);"
    |> ignore

    connection.Execute "CREATE INDEX IF NOT EXISTS idx_message_created ON message (created);"
    |> ignore

    connection.Execute "CREATE INDEX IF NOT EXISTS idx_message_deliveryCount ON message (deliveryCount);"
    |> ignore

  member _.Enqueue(messageId: string, body: 'T, ?partitionKey: string, ?scheduledEnqueueTime: DateTimeOffset) =
    task {
      let partitionKey = defaultArg partitionKey ""

      let scheduledEnqueueTimeUtc =
        scheduledEnqueueTime
        |> Option.map (fun dateTime -> dateTime.UtcDateTime)
        |> Option.toNullable

      use connection = connection ()
      let json = JsonSerializer.Serialize(body, jsonSerializerOptions)

      try
        do!
          connection.ExecuteAsync(
            """
            INSERT INTO
            message (
              id,
              partitionKey,
              scheduledEnqueueTimeUtc,
              created,
              deliveryCount,
              body
            )
          VALUES
            (
              @id,
              @partitionKey,
              @scheduledEnqueueTimeUtc,
              @created,
              0,
              @body :: jsonb
            )
            """,
            {| id = messageId
               partitionKey = partitionKey
               scheduledEnqueueTimeUtc = scheduledEnqueueTimeUtc
               created = DateTime.UtcNow
               body = json |}
          )
          :> Task

        return true
      with
      | :? PostgresException as ex when ex.SqlState = "23505" -> return false
      | ex -> return raise ex
    }

  member _.Dequeue<'T>(?partitionKey: string, ?messageCount: int, ?autoComplete: bool) =
    task {
      let partitionKey = defaultArg partitionKey ""
      let messageCount = defaultArg messageCount 1
      let autoComplete = defaultArg autoComplete false
      use connection = connection ()

      let sql =
        """
        UPDATE
          message
        SET
          completed = @completed,
          deliveryCount = deliveryCount + 1
        WHERE
          ctid IN (
            SELECT
              ctid
            FROM
              message
            WHERE
              partitionKey = @partitionKey 
              AND completed IS NULL
              AND deliveryCount < @maxDeliveryCount
              AND (scheduledEnqueueTimeUtc IS NULL OR scheduledEnqueueTimeUtc <= @now)
            ORDER BY
              created ASC, scheduledEnqueueTimeUtc ASC
            FOR UPDATE SKIP LOCKED
            LIMIT
              @messageCount
          ) RETURNING id, deliveryCount, partitionKey, body
"""

      let! messages =
        connection.QueryAsync<Message<string>>(
          sql,
          {| partitionKey = partitionKey
             maxDeliveryCount = maxDeliveryCount
             now = DateTime.UtcNow
             messageCount = messageCount
             completed =
              if autoComplete then
                Nullable DateTime.UtcNow
              else
                Nullable() |}
        )

      return
        messages
        |> Seq.map (fun message ->
          let body = JsonSerializer.Deserialize<'T>(message.Body, jsonSerializerOptions)

          { Id = message.Id
            DeliveryCount = message.DeliveryCount
            PartitionKey = message.PartitionKey
            Body = body })
        |> Seq.toArray
    }

  member _.Complete(messageId: string) =
    task {
      use connection = connection ()

      let sql = "UPDATE message SET completed = @completed WHERE id = @messageId"

      do!
        connection.ExecuteAsync(
          sql,
          {| messageId = messageId
             completed = DateTime.UtcNow |}
        )
        :> Task
    }
    :> Task

  member _.Abandon(messageId: string, ?scheduledEnqueueTime: DateTimeOffset) =
    task {
      use connection = connection ()

      let scheduledEnqueueTimeUtc =
        scheduledEnqueueTime
        |> Option.map (fun dateTime -> dateTime.UtcDateTime)
        |> Option.toNullable

      let sql =
        """
        UPDATE
          message
        SET
          scheduledEnqueueTimeUtc = @scheduledEnqueueTimeUtc,
          completed = NULL
        WHERE
          id = @messageId
        """

      do!
        connection.ExecuteAsync(
          sql,
          {| messageId = messageId
             scheduledEnqueueTimeUtc = scheduledEnqueueTimeUtc |}
        )
        :> Task
    }
    :> Task

  member _.Purge(completedBefore: DateTimeOffset) =
    task {
      use connection = connection ()

      do!
        connection.ExecuteAsync(
          "DELETE FROM message WHERE completed <= @completedBefore",
          {| completedBefore = completedBefore.UtcDateTime |}
        )
        :> Task
    }
    :> Task

  member _.DeadLetterCount(?partitionKey: string) =
    task {
      use connection = connection ()

      let sql =
        "SELECT COUNT(*) FROM message WHERE completed IS NULL AND deliveryCount >= @maxDeliveryCount"

      let sql =
        match partitionKey with
        | Some _ -> sql + " AND partitionKey = @partitionKey"
        | _ -> sql

      return!
        match partitionKey with
        | Some partitionKey ->
          connection.ExecuteScalarAsync<int>(
            sql,
            {| partitionKey = partitionKey
               maxDeliveryCount = maxDeliveryCount |}
          )
        | _ -> connection.ExecuteScalarAsync<int>(sql, {| maxDeliveryCount = maxDeliveryCount |})
    }

  member _.ActiveMessageCount(?partitionKey: string) =
    task {
      use connection = connection ()

      let sql =
        "SELECT COUNT(*) FROM message WHERE completed IS NULL AND deliveryCount < @maxDeliveryCount"

      let sql =
        match partitionKey with
        | Some _ -> sql + " AND partitionKey = @partitionKey"
        | _ -> sql

      return!
        match partitionKey with
        | Some partitionKey ->
          connection.ExecuteScalarAsync<int>(
            sql,
            {| maxDeliveryCount = maxDeliveryCount
               partitionKey = partitionKey |}
          )
        | _ -> connection.ExecuteScalarAsync<int>(sql, {| maxDeliveryCount = maxDeliveryCount |})
    }

  member _.CompletedMessageCount(?partitionKey: string) =
    task {
      use connection = connection ()
      let sql = "SELECT COUNT(*) FROM message WHERE completed IS NOT NULL"

      let sql =
        match partitionKey with
        | Some _ -> sql + " AND partitionKey = @partitionKey"
        | _ -> sql

      return!
        match partitionKey with
        | Some partitionKey -> connection.ExecuteScalarAsync<int>(sql, {| partitionKey = partitionKey |})
        | _ -> connection.ExecuteScalarAsync<int> sql
    }

  member _.ScheduledMessageCount(?partitionKey: string) =
    task {
      use connection = connection ()

      let sql =
        "SELECT COUNT(*) FROM message WHERE completed IS NULL AND deliveryCount < @maxDeliveryCount AND scheduledEnqueueTimeUtc IS NOT NULL"

      let sql =
        match partitionKey with
        | Some _ -> sql + " AND partitionKey = @partitionKey"
        | _ -> sql

      return!
        match partitionKey with
        | Some partitionKey ->
          connection.ExecuteScalarAsync<int>(
            sql,
            {| maxDeliveryCount = maxDeliveryCount
               partitionKey = partitionKey |}
          )
        | _ -> connection.ExecuteScalarAsync<int>(sql, {| maxDeliveryCount = maxDeliveryCount |})
    }
