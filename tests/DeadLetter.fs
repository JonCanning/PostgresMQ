module DeadLetter

open Expecto
open System

[<Tests>]
let tests =
  test "DeadLetter" {
    let messageQueue = messageQueue 2
    let messageId = Guid.NewGuid().ToString()
    let event = {| name = "DeadLetter" |}
    let result = messageQueue.Enqueue(messageId, event).Result
    Expect.isTrue result ""

    let activeMessageCount = messageQueue.ActiveMessageCount().Result
    Expect.equal activeMessageCount 1 ""

    let dequeued = messageQueue.Dequeue().Result |> Seq.head
    Expect.equal dequeued.Id messageId ""
    Expect.equal dequeued.Body event ""

    let scheduledEnqueueTime = DateTimeOffset.UtcNow
    messageQueue.Abandon(messageId, scheduledEnqueueTime).Wait()
    let scheduledMessageCount = messageQueue.ScheduledMessageCount().Result
    Expect.equal scheduledMessageCount 1 ""

    let _ = messageQueue.Dequeue().Result |> Seq.head
    messageQueue.Abandon(messageId).Wait()
    let deadLetterCount = messageQueue.DeadLetterCount().Result
    Expect.equal deadLetterCount 1 ""

    let dequeued = messageQueue.Dequeue().Result
    Expect.isEmpty dequeued ""

    messageQueue.Purge(DateTimeOffset.UtcNow).Wait()
    let deadLetterCount = messageQueue.DeadLetterCount().Result
    Expect.equal deadLetterCount 1 ""
  }
