package gocache

import (
	"context"
	"sync"
	"time"
)

// DefaultJanitorFrequency is the default frequency at which the cache will
// evict expired entries. This can be overridden with OptionJanitorFrequency.
const DefaultJanitorFrequency = 30 * time.Second

const (
	EvictionReasonExpired   = "expired"
	EvictionReasonCacheFull = "full"
)

// Cache is a thread-safe in-memory cache with request coalescing.
// Use New() to create a new cache instance.
type Cache[K comparable, V any] struct {
	entries  map[K]*entry[V]
	inFlight map[K]*inFlight[V]
	maxSize  int
	mutex    sync.RWMutex
	ctx      context.Context

	onEviction      func(key K, value V, reason string)
	onEvictionMutex sync.RWMutex
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

// OnEviction sets a function that is called when an entry is evicted.
// Eviction can be due to expiration or the cache being full (if OptionMaxSize is set).
func (c *Cache[K, V]) OnEviction(f func(key K, value V, reason string)) {
	c.onEvictionMutex.Lock()
	c.onEviction = f
	c.onEvictionMutex.Unlock()
}

// Task is a function that performs the work if the key is
// not present in the cache, or the entry has expired.
type Task[V any] func(ctx context.Context) (result V, expiry time.Time, err error)

// Load returns the cached value if present.
// The boolean indicates whether a value was found.
// If there is currently in-flight work for the key, Load will wait for it to
// complete. Note that if the in-flight work returns a non-nil error, the result
// will not be cached and thus not returned to the caller of Load.
func (c *Cache[K, V]) Load(ctx context.Context, key K) (value V, ok bool, err error) {
	return c.LoadOrStore(ctx, key, nil)
}

// Store executes the task. If the task returns no error, it caches the result.
// Existing in-flight work for the key will be awaited upon before executing the task.
// Since we are not interested in the result of existing in-flight work, this
// function is not registered as a watcher. That is, this function will not
// keep the in-flight work alive if there are no other watchers.
func (c *Cache[K, V]) Store(ctx context.Context, key K, task Task[V]) (V, error) {
	_, _, result, err := c.swap(ctx, key, task, false)
	return result, err
}

// Clear deletes all cached entries, resulting in an empty cache.
func (c *Cache[K, V]) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for k := range c.entries {
		delete(c.entries, k)
	}
}

// LoadOrStore returns the cached value if present and not expired. In this
// case, ok will be true. Otherwise, it executes the task and returns the
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
func (c *Cache[K, V]) LoadOrStore(ctx context.Context, key K, task Task[V]) (actual V, loaded bool, err error) {
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
		if entry, ok := c.entries[key]; ok && entry.expires.After(now) {
			c.mutex.Unlock()
			return entry.value, true, nil
		}

		// Check for in-flight work
		if lockReleased, err := c.waitForInFlightUnsafe(ctx, key, true); err != nil {
			return *new(V), false, err
		} else if lockReleased {
			// The next iteration will pick up the cached value if the task was successful
			continue
		}

		// No cached entry and no in-flight work;
		// it is our responsibility to perform the task.

		// Return early if task is nil
		if task == nil {
			c.mutex.Unlock()
			return *new(V), false, nil
		}

		_, _, result, err := c.doTaskUnsafe(ctx, key, task)

		// Since we started the work, we must return the error to the caller.
		// It's important to signal that this was not a cache hit
		return result, false, err
	}
}

func (c *Cache[K, V]) LoadAndDelete(ctx context.Context, key K) (value V, loaded bool, err error) {
	now := time.Now()

	for {
		c.mutex.Lock()

		if lockReleased, err := c.waitForInFlightUnsafe(ctx, key, true); err != nil {
			return *new(V), false, err
		} else if lockReleased {
			continue
		}

		if entry, ok := c.entries[key]; ok && entry.expires.After(now) {
			value = entry.value
			loaded = true
		}

		delete(c.entries, key)
		c.mutex.Unlock()

		return value, loaded, nil
	}
}

// Delete deletes the cached value for a key.
// If the key is not in the map, Delete does nothing.
// Delete will wait for any in-flight work to complete.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	for {
		c.mutex.Lock()

		if lockReleased, err := c.waitForInFlightUnsafe(ctx, key, false); err != nil {
			return err
		} else if lockReleased {
			continue
		}

		delete(c.entries, key)
		c.mutex.Unlock()

		return nil
	}
}

// Swap executes the task. If the task returns no error, it caches the result.
// Existing in-flight work for the key will be awaited upon before executing the
// task. The previous value, if any, is returned. The loaded bool reports
// whether the key was present.
func (c *Cache[K, V]) Swap(ctx context.Context, key K, task Task[V]) (previousValue V, loaded bool, newValue V, err error) {
	return c.swap(ctx, key, task, true)
}

type entry[V any] struct {
	value V

	// The time at which the value expires. If time.Now()
	// is equal or greater than this value, then the value
	// is considered expired and will not be returned.
	expires time.Time
}

type keyValue[K comparable, V any] struct {
	key   K
	value V
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

func (c *Cache[K, V]) swap(ctx context.Context, key K, task Task[V], watch bool) (previousValue V, loaded bool, newValue V, err error) {
	// If the cache's context is closed, bypass the cache.
	if c.ctx.Err() != nil {
		v, _, err := task(ctx)
		return *new(V), false, v, err
	}

	for {
		c.mutex.Lock()

		// Check for in-flight work
		if lockReleased, err := c.waitForInFlightUnsafe(ctx, key, watch); err != nil {
			return *new(V), false, *new(V), err
		} else if lockReleased {
			// The next iteration will pick up the cached value if the task was successful
			continue
		}

		// No in-flight work

		return c.doTaskUnsafe(ctx, key, task)
	}
}

// doTaskUnsafe registers a new in-flight task for the key and waits for the
// task to complete.
// Callers must hole a write lock.
// [!] This function releases the lock.
func (c *Cache[K, V]) doTaskUnsafe(ctx context.Context, key K, task Task[V]) (prevValue V, prevOk bool, newValue V, err error) {
	if _, ok := c.inFlight[key]; ok {
		panic("gocache: in-flight task already exists")
	}

	inFlightTask := newInFlight[V](ctx)
	c.inFlight[key] = inFlightTask

	c.addWatcher(ctx, key)

	c.mutex.Unlock()

	var existing *entry[V]
	var existingSet bool

	go func() {
		v, expires, err := task(inFlightTask.ctx)

		var evicted *keyValue[K, V]

		c.mutex.Lock()

		defer func() {
			// Remove the in-flight task from the map
			delete(c.inFlight, key)

			// Cancel the task's context to signal to watchers that the task has completed
			inFlightTask.cancel()

			c.mutex.Unlock()

			// Process eviction after releasing the main lock
			c.onEvictionMutex.RLock()
			if evicted != nil && c.onEviction != nil {
				c.onEviction(evicted.key, evicted.value, EvictionReasonCacheFull)
			}
			c.onEvictionMutex.RUnlock()
		}()

		existing, existingSet = c.entries[key]

		inFlightTask.result = v
		inFlightTask.err = err

		// Return early if there was an error; do not cache the result.
		if err != nil {
			return
		}

		// Evict an entry if necessary
		if !existingSet && c.maxSize > 0 && len(c.entries) == c.maxSize {
			evicted = c.evictRandomUnsafe()
		}

		c.entries[key] = &entry[V]{
			value:   v,
			expires: expires,
		}
	}()

	select {
	case <-inFlightTask.ctx.Done():
		if existingSet {
			return existing.value, existingSet, inFlightTask.result, inFlightTask.err
		}

		return *new(V), false, inFlightTask.result, inFlightTask.err

	case <-ctx.Done():
		return *new(V), false, *new(V), ctx.Err()
	}
}

// addWatcher registers the context as a watcher of the in-flight work.
// As long as the context is not canceled, it will keep the task alive.
// Callers must hold a write lock and an in-flight task must exist.
func (c *Cache[K, V]) addWatcher(ctx context.Context, key K) {
	inFlightTask, ok := c.inFlight[key]
	if !ok {
		panic("gocache: in-flight task should exist")
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

// Callers must hold a write lock.
// [!] This function releases the lock if there is in-flight work.
func (c *Cache[K, V]) waitForInFlightUnsafe(ctx context.Context, key K, watch bool) (lockReleased bool, err error) {
	inFlightTask, ok := c.inFlight[key]
	if !ok {
		return false, nil
	}

	if watch {
		c.addWatcher(ctx, key)
	}

	c.mutex.Unlock()

	select {
	case <-inFlightTask.ctx.Done():
		return true, nil

	// If the caller's context expires, return false.
	// The watcher will unregister us from the in-flight task.
	case <-ctx.Done():
		return true, ctx.Err()
	}
}

func (c *Cache[K, V]) janitor(frequency time.Duration) {
	context.AfterFunc(c.ctx, c.Clear)

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
	now := time.Now()
	var evicted []keyValue[K, V]

	// Write lock so we can safely iterate and delete
	c.mutex.Lock()

	// Read lock so the function can't be swapped
	c.onEvictionMutex.RLock()

	// Evict all expired entries
	for k, v := range c.entries {
		// A zero time means the entry never expires
		if v.expires.IsZero() {
			continue
		}

		if v.expires.After(now) {
			continue
		}

		delete(c.entries, k)

		// Only collect evicted entries if there is a callback
		if c.onEviction != nil {
			evicted = append(evicted, keyValue[K, V]{key: k, value: v.value})
		}
	}

	// Release the main lock to unblock other cache users but also to avoid
	// deadlock if the onEviction function tries to read or write from the cache.
	c.mutex.Unlock()

	for _, e := range evicted {
		c.onEviction(e.key, e.value, EvictionReasonExpired)
	}

	c.onEvictionMutex.RUnlock()
}

// evictRandomUnsafe removes a single random entry from the cache.
// Unsafe refers to the fact that the caller must hold a write lock on the cache.
func (c *Cache[K, V]) evictRandomUnsafe() *keyValue[K, V] {
	// Evict a random entry
	for k, v := range c.entries {
		delete(c.entries, k)
		return &keyValue[K, V]{key: k, value: v.value}
	}
	return nil
}
