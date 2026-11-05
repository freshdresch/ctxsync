# ctxsync

`ctxsync` provides synchronization primitives for Go whose waits take a `context.Context`. A wait
returns when the thing it waits for happens or when its context ends. You can bound every wait with
a timeout or cancel it.

```sh
go get github.com/freshdresch/ctxsync
```

## Which type do I need?

| Type | Use it for | Closest standard tool |
|---|---|---|
| [`Event`](docs/event.md) | A flag that is set once and stays set, such as "ready" or "shutting down". | A channel you close once |
| [`Gate`](docs/gate.md) | A flag that opens and shuts again, such as "connected" or "not paused". | None |
| [`Latch`](docs/latch.md) | Waiting until a fixed number of things have finished. | `sync.WaitGroup` |
| [`Cond`](docs/cond.md) | Waiting for a condition on state that a mutex protects. | `sync.Cond` |
| [`Barrier`](docs/barrier.md) | A fixed number of goroutines that wait for each other, round after round. | None |
| [`Future`](docs/future.md) | A value and an error that one goroutine produces and others wait for. `Go` runs work in a goroutine and returns a `Future` for its result. | A value plus a channel you close once |

## Example

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/freshdresch/ctxsync"
)

func main() {
	var ready ctxsync.Event
	go ready.Set()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ready.Wait(ctx); err != nil {
		fmt.Println("gave up:", err)
		return
	}
	fmt.Println("ready")
}
```

Each type's page has an example, and versions of them run as tests in
[example_test.go](example_test.go).

## Rules for every type

### What a wait returns

- A wait returns nil when the thing it waits for happened, and `ctx.Err()` when the context ended
  first. `Barrier.Await` can also return `ErrBrokenBarrier`, and `Future.Value` also returns the
  producer's error.
- A canceled wait returns `ctx.Err()`, not `context.Cause(ctx)`. Call `context.Cause` yourself if
  you need the cause.
- If the thing a wait waits for happened and its context also ended, the wait reports the thing
  that happened. A wait on an event that is already set returns nil even if its context is already
  canceled. `Cond` is the one exception, because its wakeup can be handed to another waiter. Its
  page explains this.
- Passing a nil context panics. Pass `context.Background()` when you do not need cancellation.

### Sharing data between goroutines

- Every method is safe for concurrent use. `Cond.Wait` also needs its caller to hold `Cond.L`.
- Whatever a goroutine wrote before it set, opened, counted down, or resolved something is visible
  to every goroutine whose wait then returns nil.
- Whatever the work passed to `Go` wrote before it returned is visible to every goroutine whose wait
  on its `Future` then returns nil.
- Whatever a `Barrier` party wrote before it arrived is visible to every party whose `Await` of
  that generation returns nil.
- Whatever a goroutine wrote before it called `Signal` or `Broadcast` is visible to a waiter that
  the call woke, once its `Wait` returns nil.
- Do not copy an `Event`, `Gate`, `Latch`, `Cond`, or `Barrier` after first use, because each holds
  a mutex. `go vet` reports such copies. A `Future` and its `Resolver` are handles that you can
  copy freely.

### Zero values

`Event` and `Gate` work as zero values. A zero `Cond` works once you set its `L`. The zero `Latch`
is already open, so create a `Latch` with `NewLatch`. `Await` on a zero `Barrier` panics, and so
does every method on a zero `Future` or `Resolver`. Create them with `NewBarrier`, `NewPromise`,
and `Go`.

### Goroutines

Only `Go` starts a goroutine. It runs until its work returns or calls `runtime.Goexit`, even after
every waiter gives up. Cancel the context passed to `Go` to ask it to stop, and wait on its `Future`
to know it finished. No other function starts a goroutine. A panic in its work ends the program,
with the exceptions that the `Go` doc names.

### Tests

If you use these types in `testing/synctest` tests, read [docs/testing.md](docs/testing.md).
