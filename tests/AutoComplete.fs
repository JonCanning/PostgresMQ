module AutoComplete

open Expecto
open System

[<Tests>]
let tests =
  test "AutoComplete" {
    let messageQueue = messageQueue 1
    let messageId = Guid.NewGuid().ToString()
    let event = {| name = "AutoComplete" |}
    let result = messageQueue.Enqueue(messageId, event).Result
    Expect.isTrue result ""

    let dequeued = messageQueue.Dequeue(autoComplete = true).Result |> Seq.head
    Expect.equal dequeued.Id messageId ""
    Expect.equal dequeued.Body event ""

    let activeMessageCount = messageQueue.ActiveMessageCount().Result
    Expect.equal activeMessageCount 0 ""

    messageQueue.Abandon(messageId).Wait()
    let deadLetterCount = messageQueue.DeadLetterCount().Result
    Expect.equal deadLetterCount 1 ""
  }
