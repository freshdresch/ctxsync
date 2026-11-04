# Gate

A `Gate` is a flag that opens and shuts again. Goroutines wait for it to be open, with a context.
Use it for state that comes and goes, such as "connected" or "not paused". Unlike an
[`Event`](event.md), a `Gate` can shut after it opens.

The zero value is a shut `Gate` that is ready to use.

## Example

```go
var gate ctxsync.Gate
gate.Open()
if err := gate.Wait(ctx); err != nil {
	return err
}
fmt.Println("open")

gate.Shut()
fmt.Println("IsOpen after Shut:", gate.IsOpen()) // IsOpen after Shut: false
```

## API

| Method | Description |
|---|---|
| `Open()` | Opens the gate and wakes every waiter. On an open gate, it does nothing. |
| `Shut()` | Shuts the gate. On a shut gate, it does nothing. |
| `IsOpen()` | Reports whether the gate is open. It returns at once without waiting for `Open` and does not allocate. |
| `Opened()` | Returns a channel for one opening. While the gate is open, the channel is already closed. While it is shut, `Open` closes it. |
| `Wait(ctx)` | Blocks until the gate is open or `ctx` is done. It returns nil if the gate opens while it waits, even if the gate shuts again before `Wait` returns, and even when `ctx` is done too. |

## Rules

### The gate can shut again at any time

A nil from `Wait` means the gate was open at some point while you waited. It may be shut again by
the time you act on it. If the gate must stay open while you act, your code and the code that calls
`Shut` must share a lock of your own. Checking `IsOpen` again does not help, because the gate can
shut right after the check.

### Channels from Opened belong to one opening

A channel from `Opened` stays closed after a later `Shut`, so call `Opened` again after each
wakeup. A loop that selects only on `Opened()` spins while the gate stays open, because every call
returns a closed channel:

```go
for {
	select {
	case <-gate.Opened():
		doWork() // runs again at once while the gate stays open
	case <-ctx.Done():
		return
	}
}
```

To act once per opening, do not select on `Opened()` again until your own logic knows the gate has
shut, for example because the code that calls `Shut` also tells you.

### When a wait counts an opening

`Wait` counts an opening from the moment it takes its channel from `Opened`, which it does under
the gate's lock. If it sees the gate already open before that, it returns nil at once. An `Open`
and a `Shut` that both finish in the short window before `Wait` takes its channel are not counted.
This matters only when the gate flips open and shut again within that window.
