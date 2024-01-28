module Complete

open Expecto
open System

[<Tests>]
let tests =
  test "Complete" {
    let messageQueue = messageQueue ()
    let messageId = Guid.NewGuid().ToString()
    let event = {| name = "Complete" |}
    let result = messageQueue.Enqueue(messageId, event).Result
    Expect.isTrue result ""

    let duplicateMessageResult = messageQueue.Enqueue(messageId, event).Result
    Expect.isFalse duplicateMessageResult ""

    let activeMessageCount = messageQueue.ActiveMessageCount().Result
    Expect.equal activeMessageCount 1 ""

    let dequeued = messageQueue.Dequeue().Result |> Seq.head
    Expect.equal dequeued.Id messageId ""
    Expect.equal dequeued.Body event ""

    messageQueue.Complete(messageId).Wait()
    let activeMessageCount = messageQueue.ActiveMessageCount().Result
    Expect.equal activeMessageCount 0 ""

    let dequeued = messageQueue.Dequeue().Result
    Expect.isEmpty dequeued ""

    let completedMessageCount = messageQueue.CompletedMessageCount().Result
    Expect.equal completedMessageCount 1 ""

    messageQueue.Purge(DateTimeOffset.UtcNow).Wait()
    let completedMessageCount = messageQueue.CompletedMessageCount().Result
    Expect.equal completedMessageCount 0 ""
  }
