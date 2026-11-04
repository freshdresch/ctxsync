# Cond

A `Cond` is a condition variable whose `Wait` takes a context. Use it as you would `sync.Cond`,
when goroutines wait for a condition on state that a mutex protects. Unlike with `sync.Cond`, those
waits can end on cancellation or a timeout.

`NewCond(locker)` returns a `Cond` whose `L` is `locker`, as `sync.NewCond` does. A struct literal
that sets `L` also works.

## Example

This example keeps a queue of tokens. `take` waits for a token or for its context to end:

```go
var mu sync.Mutex
cond := ctxsync.NewCond(&mu)
var tokens []int

take := func(ctx context.Context) (int, error) {
	mu.Lock()
	defer mu.Unlock()
	for len(tokens) == 0 {
		if err := cond.Wait(ctx); err != nil {
			return 0, err
		}
	}
	if err := ctx.Err(); err != nil {
		// This caller leaves without taking a token, so it hands the wakeup to another waiter.
		cond.Signal()
		return 0, err
	}
	token := tokens[0]
	tokens = tokens[1:]
	return token, nil
}

put := func(token int) {
	mu.Lock()
	defer mu.Unlock()
	tokens = append(tokens, token)
	cond.Signal()
}

go func() {
	for token := 1; token <= 3; token++ {
		put(token)
	}
}()
for range 3 {
	token, err := take(ctx)
	if err != nil {
		return err
	}
	fmt.Println(token)
}
```

## API

| Method | Description |
|---|---|
| `Wait(ctx)` | The caller must hold `L`. `Wait` unlocks `L`, blocks until it is woken or `ctx` is done, and locks `L` again before it returns. It returns nil if it was woken and `ctx` was still live, and `ctx.Err()` otherwise. |
| `Signal()` | Wakes the longest-waiting waiter, if there is one. |
| `Broadcast()` | Wakes every waiter, and closes the channel that `Changed` returned. |
| `Changed()` | Returns a channel that the next `Broadcast` closes. `Signal` does not close it. |

## Rules

### Always wait in a loop, and stop on an error

Check the condition in a loop, and return when `Wait` returns an error, as the example does. A loop
that ignores the error keeps calling `Wait` after the context ends. Each call returns at once, so
the loop spins. A linter that reports unchecked errors, such as `errcheck`, catches this.

### Cancellation wins over a wakeup

`Cond` is the one type where cancellation wins. `Wait` checks the context once it has locked `L`
again. If the context is done at that point, `Wait` returns `ctx.Err()` even if it was also woken.
If a `Signal` woke it, it passes that `Signal` on, so another waiter gets it if one is waiting.

A nil from `Wait` means the waiter was woken and its context was live at that moment. The context
can still end right after. If you get nil but leave without acting on the condition, call `Signal`
first so another waiter gets the wakeup. The example does this with its `ctx.Err()` check.

### Signal or Broadcast

Waiters wake in the order they started waiting. Use `Signal` only when every waiter waits for the
same condition. Waiters can wait for different conditions on one `Cond`, such as "not empty" and
"not full". Then a `Signal` can wake a waiter whose condition is still false, while the waiter it
was meant for stays blocked. In that case, use `Broadcast`, or a separate `Cond` for each
condition.

### A canceled wait still relocks L

Locking `L` again cannot be interrupted, so a canceled `Wait` blocks for as long as another
goroutine holds `L`.

### Waiting alongside other channels

To wait for the condition together with other channels, read `Changed` while you hold `L`, after
you check the condition. Then unlock `L` and select on the channel. Any change made under `L` and
followed by `Broadcast` closes the channel:

```go
cond.L.Lock()
for !ready {
	changed := cond.Changed()
	cond.L.Unlock()
	select {
	case <-changed:
	case <-stop:
		return
	}
	cond.L.Lock()
}
cond.L.Unlock()
```

### Setting L

Set `L` before the `Cond` is used. Do not change it while the `Cond` is in use, because the
assignment is a data race. `Wait` on a `Cond` whose `L` is nil panics.
