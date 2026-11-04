# Event

An `Event` is a flag that is set once and stays set. Goroutines wait for it with a context. Use it
for one-time signals such as "the server is ready" or "shutdown has started". It is like a channel
that you close once, except that `Set` is safe to call more than once and every wait takes a
context.

The zero value is an unset `Event` that is ready to use.

## Example

```go
var ready ctxsync.Event
go ready.Set()

if err := ready.Wait(ctx); err != nil {
	return err
}
fmt.Println("ready")
```

## API

| Method | Description |
|---|---|
| `Set()` | Sets the event and wakes every waiter. Calls after the first do nothing. |
| `IsSet()` | Reports whether `Set` has been called. It returns at once without waiting for `Set` and does not allocate. |
| `Done()` | Returns a channel that closes once `Set` has been called. Every call returns the same channel. Use it to wait for the event in a `select` alongside other channels. |
| `Wait(ctx)` | Blocks until the event is set or `ctx` is done. It returns nil if the event is set, even when `ctx` is done too. |
