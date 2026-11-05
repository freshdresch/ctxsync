# Future

A `Future` holds a value and an error that one goroutine, the producer, resolves once. Any number of
goroutines can wait for them with a context. Use it to hand the result of background work to the
code that needs it.

A `Future` usually comes from `Go`. `Go(ctx, work)` runs `work` in a new goroutine and returns a
`Future` for the value and error that `work` returns. Pass the `Future` to the code that waits for
the result. It can wait and read, but it cannot resolve.

`NewPromise[T]()` is for producers that do not fit `Go`. It returns an unresolved `Future[T]` and
its `Resolver[T]`. The producer keeps the `Resolver` and calls `Resolve` when its work is done.

## Example

Two HTTP requests run at the same time, and the caller waits for both results:

```go
// get returns the body of a GET request for url.
func get(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get %q: status %d", url, response.StatusCode)
	}
	return io.ReadAll(response.Body)
}

func showUser(ctx context.Context, client *http.Client, baseURL string) error {
	// An early return cancels ctx, which asks the request still running to stop.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Both requests start now and run at the same time.
	profile := ctxsync.Go(ctx, func(ctx context.Context) ([]byte, error) {
		return get(ctx, client, baseURL+"/users/42")
	})
	orders := ctxsync.Go(ctx, func(ctx context.Context) ([]byte, error) {
		return get(ctx, client, baseURL+"/users/42/orders")
	})

	profileBody, err := profile.Value(ctx)
	if err != nil {
		return fmt.Errorf("profile: %w", err)
	}
	ordersBody, err := orders.Value(ctx)
	if err != nil {
		return fmt.Errorf("orders: %w", err)
	}
	fmt.Printf("profile: %s\norders: %s\n", profileBody, ordersBody)
	return nil
}
```

`Go` returns without waiting for its request, so `showUser` has both requests in flight before it
waits for either. Each `Value` call waits for its own request, or returns early if `ctx` ends.

If the profile request fails, `showUser` returns early, and the deferred `cancel` asks the orders
request to stop. `showUser` does not wait for that request to stop. A caller that needs to wait for
it calls `Wait` on the orders `Future` after `cancel`.

## API

| Function or method | Description |
|---|---|
| `Go(ctx, work)` | Runs `work(ctx)` in a new goroutine and returns a `Future` that resolves with its result. A panic in `work` ends the program. |
| `Resolver.Resolve(value, err)` | Stores `value` and `err`, wakes every waiter, and returns true. Only the first call counts. Later calls return false and change nothing. |
| `Future.Wait(ctx)` | Blocks until the `Future` is resolved or `ctx` is done. It returns nil if it is resolved, even when `ctx` is done too. It never returns the producer's error. |
| `Future.Value(ctx)` | Waits as `Wait` does, then returns the stored value and error. If the wait ends first, it returns the zero value and `ctx.Err()`. |
| `Future.Result()` | Returns the stored value, true, and the stored error, without waiting. Before resolution, it returns the zero value, false, and nil. It does not lock or allocate. |
| `Future.IsResolved()` | Reports whether the `Future` is resolved. It does not lock or allocate. |
| `Future.Done()` | Returns a channel that closes once the `Future` is resolved. |

## Rules

### Resolve on every path

`Go` resolves the `Future` whenever `work` returns or calls `runtime.Goexit`. A producer that uses
`NewPromise` must call `Resolve` on every path where it returns. If it may panic or call
`runtime.Goexit`, it should use `Go`.

### The lifetime of the work

- The goroutine that `Go` starts runs until `work` returns or calls `runtime.Goexit`, even after
  every waiter has given up. A wait that returns `ctx.Err()` does not stop it.
- To ask the work to stop, cancel the context passed to `Go`. That works only if `work` honors its
  context. Canceling does not wait for the work to stop.
- To wait for the work to finish, wait on its `Future`. Once the `Future` is resolved, `work` has
  returned or called `runtime.Goexit`, and every deferred call inside `work` has run. The goroutine
  may still be finishing for a moment after that, but it touches nothing the caller owns. A strict
  count of goroutines right after a wait can still see it.
- A waiter's own context does not reach the work, unless the caller passes the same context to
  `Go`.

A panic in `work` ends the program, with the exceptions that the `Go` doc names.

### Telling a canceled wait from a failed producer

`Value` returns `context.Canceled` both when your context ends and when the producer resolved with
`context.Canceled`. When you need to tell them apart, call `Wait` and then `Result`. `Wait` reports
only your own cancellation:

```go
if err := future.Wait(ctx); err != nil {
	return err // your context ended
}
value, _, err := future.Result() // ok is true, because Wait returned nil
if err != nil {
	return err // the producer failed
}
fmt.Println(value)
```

### Check ok from Result

On an unresolved `Future`, `v, _, err := f.Result()` gives a zero value and a nil error, which
looks like success. Discarding `ok` is safe only once you know the `Future` is resolved, for
example right after `Wait` returned nil.

### The producer does not see your cancellation

A waiter's cancellation does not reach the producer. When every waiter gives up, the producer keeps
working, and its `Resolve` still succeeds. With `Go`, the work sees only the context passed to
`Go`. A caller that wants its own cancellation to stop the work passes the same context to both,
as the example does.

### Values are shared, not copied deeply

The `Future` stores the value and the error as given. A non-nil error with a value is a failure that
still carries the value. When `T` is a pointer, slice, map, or other reference type, every reader
shares what it points to.

### Handles

The `Future` and the `Resolver` hold only a pointer. You can copy them, pass them by value, and use
them as map keys. Two handles of the same kind are equal when they come from the same `NewPromise`
or `Go` call. Neither has a usable zero value, and every method on a zero handle panics.
