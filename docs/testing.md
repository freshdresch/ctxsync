# Testing with synctest

These rules matter only when you use `ctxsync` types inside a
[`testing/synctest`](https://pkg.go.dev/testing/synctest) bubble. A bubble is the isolated group of
goroutines that `synctest.Test` runs, with its own fake clock. A goroutine in a bubble is durably
blocked when only another goroutine in the same bubble can wake it.

Create each value inside the bubble that uses it. Never share one across a bubble boundary, whether
between two bubbles or between a bubble and code outside every bubble.

`Event`, `Gate`, `Latch`, `Cond`, and `Barrier` create channels as they run, so any use can tie one
of them to a bubble.

A `Future` belongs to the bubble that called `NewPromise`. From outside that bubble, calling `Wait`,
`Value`, `IsResolved`, `Result`, or `Resolve`, or receiving from the channel that `Done` returns,
ends the process.

Using a channel created inside a bubble from outside it ends the process. Blocking inside a bubble
on a channel created outside it does not count as durably blocked. In that case, `synctest.Wait`
hangs, and a leaked goroutine does not fail the test.
