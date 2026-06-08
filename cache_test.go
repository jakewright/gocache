package gocache

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestLoad_CacheMissHit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cacheCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := New[string, int](cacheCtx)

		task := func(ctx context.Context) (int, time.Time, error) {
			return 123, time.Now().Add(time.Hour), nil
		}

		loadCtx := context.Background()

		got, hit, err := c.LoadOrStore(loadCtx, "key", task)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 123 {
			t.Fatalf("got %v, want %v", got, 123)
		}
		if hit {
			t.Fatalf("unexpected cache hit")
		}

		got, hit, err = c.LoadOrStore(loadCtx, "key", task)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 123 {
			t.Fatalf("got %v, want %v", got, 123)
		}
		if !hit {
			t.Fatalf("expected cache hit")
		}
	})
}

func blockingTask[V any](task Task[V]) (Task[V], func()) {
	block := make(chan struct{})
	return func(ctx context.Context) (V, time.Time, error) {
		<-block
		return task(ctx)
	}, func() { close(block) }
}

func TestLoad_CoalesceUnitsOfWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cacheCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := New[string, int](cacheCtx)

		taskA, releaseA := blockingTask(func(ctx context.Context) (int, time.Time, error) {
			return 123, time.Now().Add(time.Hour), nil
		})

		taskB, _ := blockingTask(func(ctx context.Context) (int, time.Time, error) {
			return 456, time.Now().Add(time.Hour), nil
		})

		var resultA, resultB int
		var cacheHitA, cacheHitB bool
		var errA, errB error

		go func() {
			resultA, cacheHitA, errA = c.LoadOrStore(context.Background(), "key", taskA)
		}()

		// Wait for the first Load call to start doing the work
		synctest.Wait()

		go func() {
			resultB, cacheHitB, errB = c.LoadOrStore(context.Background(), "key", taskB)
		}()

		// Wait for the second Load call to become blocked
		synctest.Wait()

		// Release the first task
		releaseA()

		// Wait for (hopefully) both Load calls to return
		synctest.Wait()

		if resultA != 123 {
			t.Fatalf("resultA: got %v, want %v", resultA, 123)
		}
		if cacheHitA {
			t.Fatalf("cacheHitA: unexpected cache hit")
		}
		if errA != nil {
			t.Fatalf("errA: unexpected error: %v", errA)
		}

		if resultB != 123 {
			t.Fatalf("resultB: got %v, want %v", resultB, 123)
		}
		if !cacheHitB {
			t.Fatalf("cacheHitB: expected cache hit")
		}
		if errB != nil {
			t.Fatalf("errB: unexpected error: %v", errB)
		}

	})
}

func TestLoad_ContextCancellations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cacheCtx, cacheCtxCancel := context.WithCancel(context.Background())
		defer cacheCtxCancel()

		c := New[string, int](cacheCtx)

		taskA := func(ctx context.Context) (int, time.Time, error) {
			// Block until the context is cancelled
			<-ctx.Done()
			return 123, time.Now().Add(time.Hour), nil
		}

		loadCtx, cancel := context.WithCancel(context.Background())

		var v int
		var hit bool
		var err error

		go func() {
			v, hit, err = c.LoadOrStore(loadCtx, "key", taskA)
		}()

		// Wait for Load to block
		synctest.Wait()

		cancel()

		// Wait for Load to return
		synctest.Wait()

		if v != 0 {
			t.Fatalf("v: got %v, want %v", v, 0)
		}
		if hit {
			t.Fatalf("hit: unexpected cache hit")
		}
		if err == nil {
			t.Fatalf("err: expected error, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err: got %v, want %v", err, context.Canceled)
		}

	})
}

func TestLoad_Error(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cacheCtx, cacheCtxCancel := context.WithCancel(context.Background())
		defer cacheCtxCancel()

		c := New[string, int](cacheCtx)

		taskA, releaseA := blockingTask(func(ctx context.Context) (int, time.Time, error) {
			return 0, time.Time{}, errors.New("error")
		})
		taskB := func(ctx context.Context) (int, time.Time, error) {
			return 123, time.Now().Add(time.Hour), nil
		}

		var resultA, resultB int
		var cacheHitA, cacheHitB bool
		var errA, errB error

		go func() {
			resultA, cacheHitA, errA = c.LoadOrStore(context.Background(), "key", taskA)
		}()

		// Wait for task A to block so we know that it has executed first
		synctest.Wait()

		// Now start task B and wait for it to block on task A
		go func() {
			resultB, cacheHitB, errB = c.LoadOrStore(context.Background(), "key", taskB)
		}()
		synctest.Wait()

		// Double check that task B hasn't returned yet
		if resultB != 0 {
			t.Fatalf("resultB: got %v, want %v", resultB, 0)
		}

		// When we release task A, it should return an error, but
		// the second call to Load should not observe the error.
		releaseA()

		synctest.Wait()
		if resultA != 0 {
			t.Fatalf("resultA: got %v, want %v", resultA, 0)
		}
		if cacheHitA {
			t.Fatalf("cacheHitA: unexpected cache hit")
		}
		if errA == nil || errA.Error() != "error" {
			t.Fatalf("errA: got %v, want error with message \"error\"", errA)
		}

		if resultB != 123 {
			t.Fatalf("resultB: got %v, want %v", resultB, 123)
		}
		if cacheHitB {
			t.Fatalf("cacheHitB: unexpected cache hit")
		}
		if errB != nil {
			t.Fatalf("errB: unexpected error: %v", errB)
		}

	})
}

func TestLoad_ContextCancelThenError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {

		// Test that if the initiating goroutine is cancelled,
		// but the work goes on to return an error, we don't
		// get deadlock.

		cacheCtx, cacheCtxCancel := context.WithCancel(context.Background())
		defer cacheCtxCancel()

		c := New[string, int](cacheCtx)

		task, release := blockingTask(func(ctx context.Context) (int, time.Time, error) {
			return 0, time.Time{}, errors.New("error")
		})

		loadCtx, cancel := context.WithCancel(context.Background())

		var v int
		var hit bool
		var err error
		go func() {
			v, hit, err = c.LoadOrStore(loadCtx, "key", task)
		}()

		// Wait for Load to block
		synctest.Wait()

		// Cancel the context so that Load returns immediately,
		// before the task has completed.
		cancel()

		// Signal the task to complete
		release()

		synctest.Wait()

		if v != 0 {
			t.Fatalf("v: got %v, want %v", v, 0)
		}
		if hit {
			t.Fatalf("hit: unexpected cache hit")
		}
		if err == nil {
			t.Fatalf("err: expected error, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err: got %v, want %v", err, context.Canceled)
		}
	})
}

func TestLoad_Expired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {

		cacheCtx, cacheCtxCancel := context.WithCancel(context.Background())
		defer cacheCtxCancel()

		c := New[string, int](cacheCtx)

		task := func(ctx context.Context) (int, time.Time, error) {
			return 123, time.Now().Add(time.Hour), nil
		}

		v, hit, err := c.LoadOrStore(context.Background(), "key", task)
		if v != 123 {
			t.Fatalf("v: got %v, want %v", v, 123)
		}
		if hit {
			t.Fatalf("hit: unexpected cache hit")
		}
		if err != nil {
			t.Fatalf("err: unexpected error: %v", err)
		}

		// Wait until the cache expires
		time.Sleep(time.Hour * 2)

		task = func(ctx context.Context) (int, time.Time, error) {
			return 456, time.Now().Add(time.Hour), nil
		}

		v, hit, err = c.LoadOrStore(context.Background(), "key", task)
		if v != 456 {
			t.Fatalf("v: got %v, want %v", v, 456)
		}
		if hit {
			t.Fatalf("hit: unexpected cache hit")
		}
		if err != nil {
			t.Fatalf("err: unexpected error: %v", err)
		}
	})
}
