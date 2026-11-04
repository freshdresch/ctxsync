# Latch

A `Latch` counts down from a starting number and opens when it reaches zero. Goroutines wait for
it with a context. Use it to wait until a fixed number of tasks have finished. It is like a
`sync.WaitGroup` whose count is fixed up front, with a wait that takes a context.

`NewLatch(count)` returns a `Latch` whose count starts at `count`. It panics if `count` is
negative. The zero value is an open `Latch` with a count of 0.

## Example

```go
latch := ctxsync.NewLatch(3)
for range 3 {
	go latch.CountDown()
}

if err := latch.Wait(ctx); err != nil {
	return err
}
fmt.Println("all done")
```

## API

| Method | Description |
|---|---|
| `CountDown()` | Decrements the count. When the count reaches zero, the latch opens and wakes every waiter. At zero, it does nothing. |
| `Count()` | Returns the remaining count. It returns at once without waiting for the count to reach zero and does not allocate. |
| `Released()` | Returns a channel that closes once the count reaches zero. Every call returns the same channel. |
| `Wait(ctx)` | Blocks until the count reaches zero or `ctx` is done. It returns nil if the count is zero, even when `ctx` is done too. |

## Rules

The count only goes down. A `Latch` cannot be reset or reused. Create a new one for each round, or
use a [`Barrier`](barrier.md) for repeated rounds.

Extra `CountDown` calls at zero do nothing. Unlike `sync.WaitGroup`, a latch does not catch a task
that counts down twice. That extra call can open the latch early.
