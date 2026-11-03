package ctxsync

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCondWaitNilContext uses a Cond with a nil L, so the nil-context message shows that Wait
// checks the context before it reads L.
func TestCondWaitNilContext(t *testing.T) {
	var nilContext context.Context
	var cond Cond
	assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
		_ = cond.Wait(nilContext)
	})
}

func TestCondWaitPreCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	locker := newRecordingLocker()
	cond := NewCond(locker)

	locker.Lock()
	err := cond.Wait(ctx)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []string{"lock", "unlock", "lock"}, locker.events())
	assert.Equal(t, 0, queueLen(cond))
	assert.False(t, locker.TryLock(), "Wait returned without holding L")
}

func TestCondNilLocker(t *testing.T) {
	t.Run("LiveContext", func(t *testing.T) {
		var cond Cond
		assert.Panics(t, func() {
			_ = cond.Wait(t.Context())
		})
	})

	t.Run("CanceledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var cond Cond
		assert.Panics(t, func() {
			_ = cond.Wait(ctx)
		})
	})

	t.Run("SignalBroadcastChanged", func(t *testing.T) {
		var cond Cond
		assert.NotPanics(t, func() {
			cond.Signal()
			cond.Broadcast()
			_ = cond.Changed()
		})
	})
}

func TestCondWaitDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
		defer cancel()
		cond := NewCond(make(chanLock, 1))
		results := startWait(func() error {
			cond.L.Lock()
			defer cond.L.Unlock()
			return cond.Wait(ctx)
		})
		synctest.Wait()
		require.Empty(t, results, "Wait returned before the deadline")

		time.Sleep(waitTimeout)
		synctest.Wait()
		assertWaitResult(t, results, cancel, context.DeadlineExceeded)
	})
}
