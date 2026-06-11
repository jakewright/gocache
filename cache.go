package gocache

import (
	"context"
	"sync"
	"time"
)

// DefaultJanitorFrequency is the default frequency at which the cache will
// evict expired entries. This can be overridden with OptionJanitorFrequency.
const DefaultJanitorFrequency = 30 * time.Second

// Cache is a thread-safe in-memory cache with request coalescing.
// Use New() to create a new cache instance.
type Cache[K comparable, V any] struct {
	entries  map[K]*entry[V]
	inFlight map[K]*inFlight[V]
	maxSize  int
	mutex    sync.RWMutex
	ctx      context.Context
}

type options struct {
	MaxSize          int
	JanitorFrequency time.Duration
}

// New returns a new Cache configured using the provided options.
// The janitor is started in a separate goroutine (unless JanitorFrequency is
// set to zero).
// When the provided context is canceled, the janitor will be stopped and all
// items will be evicted from the cache. Subsequent calls to load functions will
// bypass the cache.
func New[K comparable, V any](ctx context.Context, opts ...func(*options)) *Cache[K, V] {
	o := &options{
		JanitorFrequency: DefaultJanitorFrequency,
	}

	for _, opt := range opts {
		opt(o)
	}

	cache := &Cache[K, V]{
		entries:  make(map[K]*entry[V]),
		inFlight: make(map[K]*inFlight[V]),
		maxSize:  o.MaxSize,
		ctx:      ctx,
	}

	go cache.janitor(o.JanitorFrequency)

	return cache
}

// OptionMaxSize sets a limit on how many items the cache can hold.
// A value less than or equal to zero disables the limit.
func OptionMaxSize(n int) func(*options) {
	return func(c *options) {
		c.MaxSize = n
	}
}

// OptionJanitorFrequency overrides the default janitor frequency.
// A duration less than or equal to zero disables the janitor.
func OptionJanitorFrequency(d time.Duration) func(*options) {
	return func(o *options) {
		o.JanitorFrequency = d
	}
}

// Task is a function that performs the work if the key is
// not present in the cache, or the entry has expired.
type Task[V any] func(ctx context.Context) (result V, expiry time.Time, err error)

// Load returns the cached value if present.
// The boolean indicates whether a value was found.
// If there is currently in-flight work for the key, Load will wait for it to
// complete. Note that if the in-flight work returns a non-nil error, the result
// will not be cached and thus not returned to the caller of Load.
func (c *Cache[K, V]) Load(ctx context.Context, key K) (V, bool, error) {
	return c.LoadOrStore(ctx, key, nil)
}

// Store will execute the task and cache the result.
// If the task returns a non-nil error, the result will not be cached.
// Store will wait for any existing in-flight work for the key to complete
// before executing the task.
func (c *Cache[K, V]) Store(ctx context.Context, key K, task Task[V]) (V, error) {
	for {
		c.mutex.Lock()

		// To maintain the invariant of one in-flight task per key at a time,
		// we wait for any existing in-flight work to complete.
		inFlightTask, ok := c.inFlight[key]
		if ok {
			c.mutex.Unlock()

			select {
			case <-inFlightTask.ctx.Done():
				continue

			// If the caller's context expires, return early.
			case <-ctx.Done():
				return *new(V), ctx.Err()
			}
		}

		// No in-flight work. Evict any existing entry.
		if _, ok := c.entries[key]; ok {
			delete(c.entries, key)
		}

		inFlightTask = newInFlight[V](ctx)
		c.inFlight[key] = inFlightTask

		c.addWatcher(ctx, key)
		c.mutex.Unlock()

		// Execute the task in a separate goroutine
		c.doTask(key, inFlightTask, task)

		select {
		case <-inFlightTask.ctx.Done():
		case <-ctx.Done():
			return *new(V), ctx.Err()
		}

		return inFlightTask.result, inFlightTask.err
	}
}

// LoadOrStore returns the cached value if present and not expired. In this
// case, cacheHit will be true. Otherwise, it executes the task and returns the
// result. If the task returns without error, the result is cached for future
// callers.
//
// For any given key, only one task function will be in-flight at a time.
// If a concurrent request comes in for the same key, the concurrent caller
// waits for the original to complete. If the original task completes
// without error, all callers will receive the same result.
//
// If the task returns a non-nil error, then the original caller will receive
// the error. A second caller waiting on the result will behave as though the
// key was not present in the cache (i.e., its own task func will be executed).
func (c *Cache[K, V]) LoadOrStore(ctx context.Context, key K, task Task[V]) (result V, cacheHit bool, err error) {
	// If the cache's context is closed, bypass the cache.
	if c.ctx.Err() != nil {
		if task == nil {
			return *new(V), false, nil
		}
		v, _, err := task(ctx)
		return v, false, err
	}

	now := time.Now()

	for {
		// Fast path: Cache hit and entry not expired
		c.mutex.RLock()
		if entry, ok := c.entries[key]; ok && entry.expires.After(now) {
			c.mutex.RUnlock()
			return entry.value, true, nil
		}
		c.mutex.RUnlock()

		// Slow path: Cache miss or entry expired
		c.mutex.Lock()

		// Since we temporarily gave up the lock, we must check the cache again.
		if entry, ok := c.entries[key]; ok {
			if entry.expires.After(now) {
				c.mutex.Unlock()
				return entry.value, true, nil
			}

			// Evict the expired item
			delete(c.entries, key)
		}

		// Check for in-flight work
		inFlightTask, ok := c.inFlight[key]
		if ok {
			// Register ourselves as a watcher so this task does not get canceled.
			// We must do this before giving up the lock.
			c.addWatcher(ctx, key)
			c.mutex.Unlock()

			select {
			case <-inFlightTask.ctx.Done():
				// The next iteration will pick up the cached value if the task
				// was successful.
				continue

			// If the caller's context expires, return early.
			// The watcher will unregister us from the in-flight task.
			case <-ctx.Done():
				return *new(V), false, ctx.Err()
			}
		}

		// No cached entry and no in-flight work;
		// it is our responsibility to perform the task.

		// Return early if task is nil
		if task == nil {
			c.mutex.Unlock()
			return *new(V), false, nil
		}

		inFlightTask = newInFlight[V](ctx)
		c.inFlight[key] = inFlightTask

		c.addWatcher(ctx, key)
		c.mutex.Unlock()

		// Execute the task in a separate goroutine
		c.doTask(key, inFlightTask, task)

		select {
		case <-inFlightTask.ctx.Done():
		case <-ctx.Done():
			return *new(V), false, ctx.Err()
		}

		// Since we started the work, we must return the error to the caller.
		// It's important to signal that this was not a cache hit
		return inFlightTask.result, false, inFlightTask.err
	}
}

func (c *Cache[K, V]) doTask(key K, inFlightTask *inFlight[V], task Task[V]) {
	go func() {
		v, expires, err := task(inFlightTask.ctx)

		c.mutex.Lock()
		defer func() {
			// Remove the in-flight task from the map
			delete(c.inFlight, key)

			// Cancel the task's context to signal to watchers that the task has completed
			inFlightTask.cancel()

			c.mutex.Unlock()
		}()

		inFlightTask.result = v
		inFlightTask.err = err

		// Return early if there was an error; do not cache the result.
		if err != nil {
			return
		}

		if c.maxSize > 0 && len(c.entries) == c.maxSize {
			c.evictRandomUnsafe()
		}

		c.entries[key] = &entry[V]{
			value:   v,
			expires: expires,
		}
	}()
}

type entry[V any] struct {
	value V

	// The time at which the value expires. If time.Now()
	// is equal or greater than this value, then the value
	// is considered expired and will not be returned.
	expires time.Time
}

type inFlight[V any] struct {
	ctx    context.Context
	cancel func()

	watchCount int

	// Set when the work has completed:
	result V
	err    error
}

func newInFlight[V any](ctx context.Context) *inFlight[V] {
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &inFlight[V]{
		ctx:    ctx,
		cancel: cancel,
	}
}

// Callers must hold a write lock on the cache
func (c *Cache[K, V]) addWatcher(ctx context.Context, key K) {
	inFlightTask, ok := c.inFlight[key]
	if !ok {
		panic("inFlight should exist")
	}

	inFlightTask.watchCount++

	go func() {
		// Wait for the watcher's context or the task's context to be cancelled
		select {
		case <-ctx.Done():
		case <-inFlightTask.ctx.Done():
		}

		c.mutex.Lock()
		defer c.mutex.Unlock()

		inFlightTask.watchCount--

		// If there are no watchers left, cancel the task.
		if inFlightTask.watchCount == 0 {
			inFlightTask.cancel()
			delete(c.inFlight, key)
		}
	}()
}

func (c *Cache[K, V]) janitor(frequency time.Duration) {
	context.AfterFunc(c.ctx, c.purge)

	if frequency <= 0 {
		return
	}

	ticker := time.NewTicker(frequency)

	for {
		select {
		case <-ticker.C:
		case <-c.ctx.Done():
			return
		}

		c.evictExpired()
	}
}

// evictExpired evicts all entries that have expired
func (c *Cache[K, V]) evictExpired() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Evict all expired entries
	for k, v := range c.entries {
		// A zero time means the entry never expires
		if v.expires.IsZero() {
			continue
		}

		if !v.expires.After(time.Now()) {
			delete(c.entries, k)
		}
	}
}

// evictRandomUnsafe removes a single random entry from the cache.
// Unsafe refers to the fact that the caller must hold a write lock on the cache.
func (c *Cache[K, V]) evictRandomUnsafe() {
	// Evict a random entry
	for k := range c.entries {
		delete(c.entries, k)
		return
	}
}

// purge removes all entries from the cache
func (c *Cache[K, V]) purge() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for k := range c.entries {
		delete(c.entries, k)
	}
}
