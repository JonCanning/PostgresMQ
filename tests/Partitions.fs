module Partitions

open Expecto
open System

[<Tests>]
let tests =
  test "Partitions" {
    let messageQueue = messageQueue ()
    let p1MessageId = Guid.NewGuid().ToString()
    let p1Message = {| name = "p1" |}
    let result = messageQueue.Enqueue(p1MessageId, p1Message, "p1").Result
    Expect.isTrue result ""

    let p2MessageId = Guid.NewGuid().ToString()
    let p2Message = {| name = "p2" |}
    let result = messageQueue.Enqueue(p2MessageId, p2Message, "p2").Result
    Expect.isTrue result ""

    let activeMessageCount = messageQueue.ActiveMessageCount("p1").Result
    Expect.equal activeMessageCount 1 ""

    let activeMessageCount = messageQueue.ActiveMessageCount("p2").Result
    Expect.equal activeMessageCount 1 ""

    let p1DequeuedMessage = messageQueue.Dequeue("p1").Result |> Seq.head

    Expect.equal p1DequeuedMessage.Id p1MessageId ""
    Expect.equal p1DequeuedMessage.Body p1Message ""

    let p2DequeuedMessage = messageQueue.Dequeue("p2").Result |> Seq.head

    Expect.equal p2DequeuedMessage.Id p2MessageId ""
    Expect.equal p2DequeuedMessage.Body p2Message ""

    messageQueue.Complete(p1MessageId).Wait()
    let activeMessageCount = messageQueue.ActiveMessageCount("p1").Result
    Expect.equal activeMessageCount 0 ""

    messageQueue.Abandon(p2MessageId).Wait()
    let activeMessageCount = messageQueue.ActiveMessageCount("p2").Result
    Expect.equal activeMessageCount 0 ""

    let deadLetterCount = messageQueue.DeadLetterCount().Result
    Expect.equal deadLetterCount 1 ""

    let dequeued = messageQueue.Dequeue("p1").Result
    Expect.isEmpty dequeued ""

    let dequeued = messageQueue.Dequeue("p2").Result
    Expect.isEmpty dequeued ""

    let completedMessageCount = messageQueue.CompletedMessageCount().Result
    Expect.equal completedMessageCount 1 ""
  }
