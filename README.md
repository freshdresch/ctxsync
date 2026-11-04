# ctxsync

`ctxsync` provides synchronization primitives whose waits take a `context.Context`. Use `Event`
for a one-shot flag, `Gate` for a flag that opens and shuts again, `Latch` for a countdown that
opens at zero, `Cond` for a condition variable, `Barrier` for a meeting point of a fixed number
of parties, and `Promise` for a value and an error that one producer resolves once and any number
of goroutines wait for. A wait stops when its outcome happens or its context ends. The outcome
depends on the type:

- `Event`: the event is set.
- `Gate`: the gate opens.
- `Latch`: the count reaches zero.
- `Cond`: a `Signal` or `Broadcast` wakes the waiter.
- `Barrier`: the wait's generation trips. A generation is one round of arrivals, and it trips
  when its last party arrives. A `Barrier` wait also stops when the barrier breaks.
- `Promise`: the promise is resolved.

```sh
go get github.com/freshdresch/ctxsync
```

## Example

```go
package main

import (
	"context"
	"fmt"

	"github.com/freshdresch/ctxsync"
)

func main() {
	var ready ctxsync.Event
	go ready.Set()

	if err := ready.Wait(context.Background()); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("ready")
}
```

Output:

```text
ready
```

## Usage rules

- Every wait panics if its context is nil. The check runs before anything else, so the panic does
  not depend on whether the wait would block. Pass `context.Background()` when you do not want
  cancellation.
- A wait decides its result after it wakes, or when it finds its state already reached or its
  context already done. A `Cond` wait decides after it locks `L` again.
- A wait returns nil when it has observed its outcome by the time it decides. It returns
  `ErrBrokenBarrier` when it waits on a broken `Barrier`, and `ctx.Err()` when the context has
  ended and the wait has not observed its outcome. `Future.Value` is the exception. It also
  returns the producer's error, because returning the producer's outcome is its purpose.
- When a cancellation breaks a `Barrier`, a party can return `ErrBrokenBarrier` or `ctx.Err()`.
  The `Barrier` section says which party returns which.
- A wait that reports cancellation returns `ctx.Err()`, not `context.Cause(ctx)`.
- Each type's doc comment names which calls happen before a wait returns nil and before a
  non-blocking query reports a positive result.
- When the outcome and the cancellation are both ready, `Event`, `Gate`, `Latch`, `Barrier`, and
  `Future` report the outcome. A wait on an `Event`, `Gate`, `Latch`, or `Future` whose state is
  already reached returns its outcome at once, even if its context is done.
- `Cond` is the exception. It reports the cancellation, because a signal can be handed to another
  waiter. A `Cond` waiter whose context is done when it decides passes any signal it received to
  the next waiter.
- No method starts a goroutine or registers a `context.AfterFunc`. A blocked wait selects on
  `ctx.Done()` directly.
- Every type except `Future` and `Resolver` holds an internal mutex, so do not copy a value after
  first use. The `copylocks` check of `go vet` reports such copies. `Future` and `Resolver` are
  handles that hold only a pointer, so you can copy them freely.
- Every method of every type is safe for concurrent use, except where `Cond` requires its caller
  to hold `Cond.L`.
- Run a linter that reports unchecked errors, such as `errcheck`. A `Cond.Wait` loop that ignores
  its error keeps calling `Wait` after the context ends, and each call returns at once, so the
  loop spins.
- Construct each primitive inside the `testing/synctest` bubble that uses it. Never share a
  primitive across a bubble boundary, whether between two bubbles or between a bubble and code
  outside every bubble. `Event`, `Gate`, `Latch`, `Cond`, and `Barrier` create channels as they
  run, so any use can tie one of them to a bubble. A promise is tied to the bubble that ran
  `NewPromise`, which creates its channel, so resolving it from outside that bubble ends the
  process.
- Using a channel created inside a bubble from outside it ends the process. Blocking inside a
  bubble on a channel created outside it is not durable blocking, so `synctest.Wait` hangs, and a
  leaked goroutine does not fail the bubble.

## API

`ErrBrokenBarrier` is the error that `Barrier.Await` returns when the barrier is broken. `Await`
returns it unwrapped, so `errors.Is` and `==` both match it.

### Event

The zero value is an unset `Event` that is ready to use.

| Method | Description |
|---|---|
| `Set()` | Sets the event and wakes every waiter. Calls after the first do nothing. |
| `IsSet()` | Reports whether `Set` has been called. It does not wait or allocate. |
| `Done()` | Returns a channel that closes once `Set` has been called. Every call returns the same channel. |
| `Wait(ctx)` | Blocks until the event is set or `ctx` is done. It returns nil if the event is set by the time it decides, even when `ctx` is done too. |

### Gate

The zero value is a shut `Gate` that is ready to use.

| Method | Description |
|---|---|
| `Open()` | Opens the gate and wakes every waiter. On an open gate, it does nothing. |
| `Shut()` | Shuts the gate. On a shut gate, it does nothing. |
| `IsOpen()` | Reports whether the gate is open. It does not wait or allocate. |
| `Opened()` | Returns the channel of one opening. While the gate is open, it returns the already closed channel of the current opening. While the gate is shut, it returns the channel that the next `Open` closes. Calls during one opening, or during one shut period, return the same channel. |
| `Wait(ctx)` | Blocks until the gate opens or `ctx` is done. It returns nil if the gate is open at some moment between when it reads the gate's state and when it decides, even when `ctx` is done too. |

A channel from `Opened` stays closed after a later `Shut`, so call `Opened` again after each
wakeup. A loop that selects only on `Opened()` spins while the gate stays open, because every call
returns a closed channel. A nil from `Wait` does not mean that the gate is still open when the
caller acts on it.

### Latch

`NewLatch(count)` returns a `Latch` whose count starts at `count`. It panics if `count` is
negative. The zero value is an open `Latch` with count 0.

| Method | Description |
|---|---|
| `CountDown()` | Decrements the count. When the count reaches zero, the latch opens and wakes every waiter. At count zero, `CountDown` does nothing. |
| `Count()` | Returns the remaining count. It does not wait or allocate. |
| `Released()` | Returns a channel that closes once the count reaches zero. Every call returns the same channel. |
| `Wait(ctx)` | Blocks until the count reaches zero or `ctx` is done. It returns nil if the count is zero by the time it decides, even when `ctx` is done too. |

### Cond

`NewCond(locker)` returns a `Cond` whose `L` is `locker`, as `sync.NewCond` does. The exported
field `L` is the `sync.Locker` that a caller holds while it observes or changes the condition. A
`Cond` works once `L` is set, by `NewCond` or by a struct literal. Do not assign `L` while the
`Cond` is in use, because the assignment is a data race. `Wait` on a `Cond` whose `L` is nil
panics.

Waiters form a FIFO queue in the order in which they join it inside `Wait`. `Signal` wakes the
waiter at the front. A signal that a canceled waiter passes on goes to the new front.

| Method | Description |
|---|---|
| `Wait(ctx)` | The caller must hold `L`. `Wait` unlocks `L` and blocks until a signal or broadcast wakes it or `ctx` is done. It locks `L` again before it returns. It returns nil if it was woken and `ctx` was live when it decided, and `ctx.Err()` otherwise. |
| `Signal()` | Wakes the waiter at the front of the queue, if there is one. When no waiter is queued, it does nothing. It does not close the channel that `Changed` returned. |
| `Broadcast()` | Wakes every queued waiter. It also closes the channel that `Changed` returned, if one exists. |
| `Changed()` | Returns a channel that the next `Broadcast` closes. Calls between two broadcasts return the same channel. Only `Broadcast` closes it, and `Signal` does not. |

Locking `L` again cannot be interrupted, so a canceled `Wait` can block as long as another
goroutine holds `L`.

Recheck the condition in a loop, and end the loop when `Wait` returns an error. If you get nil
from `Wait` but leave without acting on the condition, call `Signal` first so that another waiter
receives the wakeup. For example, `ctx` can end after `Wait` returns:

```go
c.L.Lock()
defer c.L.Unlock()
for !ready() {
	if err := c.Wait(ctx); err != nil {
		return err
	}
}
if err := ctx.Err(); err != nil {
	c.Signal()
	return err
}
consume()
```

Use `Signal` only when every waiter on the `Cond` waits for the same condition. Otherwise, use
`Broadcast` or a separate `Cond` for each condition.

To wait for the condition alongside other channels, read `Changed` while you hold `L`, after you
check the condition. Then unlock `L` and select on the channel together with the other channels.
Any change made under `L` and followed by `Broadcast` then closes the channel:

```go
c.L.Lock()
for !ready() {
	changed := c.Changed()
	c.L.Unlock()
	select {
	case <-changed:
	case <-other:
		return
	}
	c.L.Lock()
}
c.L.Unlock()
```

### Barrier

`NewBarrier(parties)` returns a `Barrier` for `parties` parties. It panics if `parties` is less
than 1. Create every `Barrier` with `NewBarrier`. The zero value is not usable, and `Await` on it
panics.

The barrier does not track which goroutine is which. A caller beyond the `parties`-th arrival of
a generation starts the next generation.

A party whose context ends before its generation trips breaks the barrier. A broken barrier stays
broken. To continue after a break, create a new `Barrier`. The parties need a way outside the
barrier, such as a shared struct or a channel, to learn about the new `Barrier`. A caller whose
context is already done when it calls `Await` does not arrive, but it breaks the barrier.

Within the generation that broke, only the party whose cancellation broke the barrier returns
`ctx.Err()`. Every other party of that generation returns `ErrBrokenBarrier`. When those parties
share one context, the first of them to take the barrier's internal mutex after the cancellation
returns `ctx.Err()`. Parties of an earlier generation that already tripped return nil, even if
they return after the break.

| Method | Description |
|---|---|
| `Await(ctx)` | Arrives at the barrier. It blocks until its generation trips, the barrier breaks, or `ctx` is done. It returns nil on a trip. It returns `ErrBrokenBarrier` when another party breaks the barrier first. It returns `ctx.Err()` when its own cancellation breaks the barrier. |
| `Parties()` | Returns the number of parties that `NewBarrier` was given. |
| `Waiting()` | Returns a snapshot of the number of parties blocked in the current generation. It returns 0 when no generation is filling or the barrier is broken. |
| `IsBroken()` | Reports whether the barrier is broken. A true result stays true, because a broken barrier stays broken. |

### Promise

`NewPromise[T]()` returns the two ends of one unresolved promise: a `Future[T]` that reads it and
a `Resolver[T]` that resolves it. Both are handles that hold only a pointer. You can copy them,
pass them by value, and store them in maps. Two handles of the same kind are equal under `==`
when they refer to the same promise. Neither handle has a usable zero value, and every method on
a zero handle panics.

| Method | Description |
|---|---|
| `Resolver.Resolve(value, err)` | Resolves the promise with `value` and `err`, wakes every waiter, and returns true. If the promise is already resolved, it returns false and the stored value and error do not change. |
| `Future.Done()` | Returns a channel that closes once the promise is resolved. Every call returns the same channel. |
| `Future.IsResolved()` | Reports whether the promise is resolved. It does not wait or allocate. |
| `Future.Wait(ctx)` | Blocks until the promise is resolved or `ctx` is done. It returns nil if the promise is resolved by the time it decides, even when `ctx` is done too. It never returns the producer's error. |
| `Future.Result()` | Returns the stored value, true, and the stored error without waiting. Before resolution, it returns the zero value, false, and nil. |
| `Future.Value(ctx)` | Waits as `Wait` does. If `Wait` returns an error, it returns the zero value and that error. Otherwise it returns the stored value and error. |

The promise stores the value and the error as given. A nil error is a success, and a non-nil
error is a failure that still carries the value. When `T` is a pointer, slice, map, or other
reference type, every reader shares what it refers to.

Check `ok` from `Result`. On an unresolved promise, `v, _, err := f.Result()` gives a zero value
and a nil error, which looks like success. Discarding `ok` is safe right after `Wait` returned
nil. `Value` cannot tell you whether a `context.Canceled` came from your context or from the
producer. If you need the distinction, call `Wait` and then `Result`.

A waiter's cancellation does not reach the producer. When every waiter gives up, the producer
keeps working, and its `Resolve` still succeeds. Give the producer its own context if it should
stop.

Every path through the producer must resolve the promise, including early returns on errors,
panics, and `runtime.Goexit`. A promise that is never resolved blocks every wait whose context
never ends. Put the work in a function that returns `(T, error)`, resolve once with its result,
and defer an abandonment call in front of it:

```go
go func() {
	defer r.Resolve(0, errAbandoned)
	r.Resolve(work(ctx))
}()
```

Here `errAbandoned` is an error you define, and `0` stands for the zero value of `T`. The first
`Resolve` wins, so the deferred call does nothing after the real one. If `work` panics, and a
deferred `recover` in the producer goroutine stops the panic, or if `work` calls `runtime.Goexit`,
waiters get `errAbandoned` instead of a false success.

Resolution is final, so call `Resolve` after all work and cleanup that can fail. Put cleanup that
can fail inside `work`. Cleanup deferred in the producer goroutine runs after `Resolve`, and if it
fails, waiters are left with a success.

Do not write `defer r.Resolve(v, err)`. It evaluates `v` and `err` when the `defer` statement
runs, so it resolves with the values they held then, usually zero and nil, which reads as
success. Do not resolve from a deferred closure over named results either. On a panic or
`runtime.Goexit`, the named results keep whatever they held, so waiters see success.
