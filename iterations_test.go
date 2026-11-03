package ctxsync

// raceAwareIterations is how many times a test whose result depends on goroutine scheduling
// repeats. The race detector slows every iteration, so the count is lower under it.
var raceAwareIterations = func() int {
	if raceEnabled {
		return 50
	}
	return 200
}()
