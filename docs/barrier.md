# Barrier

A `Barrier` makes a fixed number of goroutines, called parties, wait for each other. When the last
party arrives, every party is released and the next round starts. Use it for work done in steps,
where no party may start a step until all have finished the one before. Each round is called a
generation.

`NewBarrier(parties)` returns a `Barrier` for `parties` parties. It panics if `parties` is less
than 1. Always create a `Barrier` with `NewBarrier`. The zero value is not usable, and `Await` on it
panics.

## Example

```go
barrier := ctxsync.NewBarrier(2)
var passed atomic.Int32
var parties sync.WaitGroup
for range barrier.Parties() {
	parties.Go(func() {
		if err := barrier.Await(ctx); err != nil {
			fmt.Println(err)
			return
		}
		passed.Add(1)
	})
}
parties.Wait()
fmt.Println("passed:", passed.Load()) // passed: 2
```

## API

| Method | Description |
|---|---|
| `Await(ctx)` | Arrives at the barrier and blocks until every party of this generation has arrived, the barrier breaks, or `ctx` is done. It returns nil when the generation completes. |
| `Parties()` | Returns the number of parties that `NewBarrier` was given. |
| `Waiting()` | Returns how many parties are blocked in the current generation, as a snapshot. It returns 0 when the barrier is broken. |
| `IsBroken()` | Reports whether the barrier is broken. |

## Rules

### A cancellation breaks the barrier for everyone

If a party's context ends before its generation completes, the barrier breaks, and every party
waiting in that generation is released. The party whose cancellation broke it gets `ctx.Err()`.
Every other party gets `ErrBrokenBarrier`. This holds even when every party shares the same
canceled context, as in an `errgroup`. A caller whose context is already done when it calls
`Await` does not arrive, but it still breaks the barrier.

`Await` returns `ErrBrokenBarrier` unwrapped, so `errors.Is` and `==` both match it.

### A broken barrier stays broken

Every later `Await` returns `ErrBrokenBarrier`. To continue, create a new `Barrier` and give the
parties a way to find it, such as a shared struct or a channel. Parties of an earlier generation
that already completed still return nil, even if they return after the break.

### Arrivals are visible to every party

Whatever a party wrote before it arrived is visible to every party whose `Await` of that generation
returns nil.

### The barrier counts arrivals, not goroutines

It does not track which goroutine is which. An arrival beyond the last party of a generation
starts the next generation.
