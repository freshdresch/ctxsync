# Future

A `Future` holds a value and an error that one goroutine, the producer, resolves once. Any number of
goroutines can wait for them with a context. Use it to hand the result of background work to the
code that needs it.

`NewPromise[T]()` returns an unresolved `Future[T]` and its `Resolver[T]`. Pass the `Future` to the
code that waits for the result. It can wait and read, but it cannot resolve. The producer keeps the
`Resolver` and calls `Resolve` when its work is done.

## Example

Two HTTP requests run at the same time, and the caller waits for both results:

```go
// fetch starts a GET request for url and returns a Future for the response body.
func fetch(ctx context.Context, client *http.Client, url string) ctxsync.Future[[]byte] {
	future, resolver := ctxsync.NewPromise[[]byte]()
	go func() {
		resolver.Resolve(get(ctx, client, url))
	}()
	return future
}

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
	// Both requests start now and run at the same time.
	profile := fetch(ctx, client, baseURL+"/users/42")
	orders := fetch(ctx, client, baseURL+"/users/42/orders")

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

`fetch` returns as soon as the request starts, so `showUser` has both requests in flight before it
waits for either. Each `Value` call waits for its own request, or returns early if `ctx` ends.
`get` returns its result on every path, so the one `Resolve` in `fetch` always runs. If the
producer may run under code that recovers panics, see
[Resolve on every path](#resolve-on-every-path).

## API

| Method | Description |
|---|---|
| `Resolver.Resolve(value, err)` | Stores `value` and `err`, wakes every waiter, and returns true. Only the first call counts. Later calls return false and change nothing. |
| `Future.Wait(ctx)` | Blocks until the `Future` is resolved or `ctx` is done. It returns nil if it is resolved, even when `ctx` is done too. It never returns the producer's error. |
| `Future.Value(ctx)` | Waits as `Wait` does, then returns the stored value and error. If the wait ends first, it returns the zero value and `ctx.Err()`. |
| `Future.Result()` | Returns the stored value, true, and the stored error, without waiting. Before resolution, it returns the zero value, false, and nil. It does not lock or allocate. |
| `Future.IsResolved()` | Reports whether the `Future` is resolved. It does not lock or allocate. |
| `Future.Done()` | Returns a channel that closes once the `Future` is resolved. |

## Rules

### Resolve on every path

A `Future` that is never resolved blocks every wait whose context never ends. Every path through
the producer must call `Resolve`, including early returns, panics, and `runtime.Goexit`.

The example's pattern does this. Put the work in a function that returns `(T, error)`. Resolve once
with its result, and defer an abandonment call in front of it. The first `Resolve` wins, so the
deferred call does nothing after the real one. Suppose `work` panics and a `recover` in the
producer goroutine stops the panic, or `work` calls `runtime.Goexit`. Then waiters get the
abandonment error instead of a false success.

Resolution is final, so call `Resolve` after all work and cleanup that can fail. Put that cleanup
inside `work`. Cleanup deferred in the producer goroutine runs after `Resolve`, and if it fails,
waiters are left with a success.

Two patterns look right but report success when the producer panics:

- Do not write `defer r.Resolve(v, err)`. It evaluates `v` and `err` when the `defer` statement
  runs, so it resolves with the values they held then, usually zero and nil.
- Do not resolve from a deferred closure over named results `v` and `err`:

  ```go
  func produce(ctx context.Context, r ctxsync.Resolver[int]) (v int, err error) {
  	defer func() { r.Resolve(v, err) }()
  	return work(ctx)
  }
  ```

  On a normal return, this resolves with the real outcome. On a panic or `runtime.Goexit`, the
  function never sets its named results. They keep whatever they held, usually zero and nil, so
  waiters see success.

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

When every waiter gives up, the producer keeps working, and its `Resolve` still succeeds. Give the
producer its own context if it should stop.

### Values are shared, not copied deeply

The `Future` stores the value and the error as given. A non-nil error with a value is a failure that
still carries the value. When `T` is a pointer, slice, map, or other reference type, every reader
shares what it points to.

### Handles

The `Future` and the `Resolver` hold only a pointer. You can copy them, pass them by value, and use
them as map keys. Two handles of the same kind are equal when they come from the same `NewPromise`
call. Neither has a usable zero value, and every method on a zero handle panics.
