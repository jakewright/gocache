# gocache

A thread-safe in-memory cache with request coalescing.

## Installation

```sh
go get github.com/jakewright/gocache
```

## Usage

[View the full documentation on pkg.go.dev](https://pkg.go.dev/github.com/jakewright/gocache)

```go
ctx, cancel := context.WithCancel(context.Background())

// Create a new cache, specifying the key and value types.
cache := gocache.New[string, *http.Response](ctx,

	// Optionally set the maximum number of entries in the cache 
	gocache.OptionMaxSize(1000),

	// The janitor runs periodically to clean up expired entries.
	// Use this option to override the default frequency of 30 seconds.
	// A value less than or equal to zero disables the janitor.
	gocache.OptionJanitorFrequency(time.Second*10),
)

// Construct a task that returns a result, an expiry time, and optionally an error.
task := func(ctx context.Context) (result *http.Response, expiry time.Time, err error) {
	rsp, err := http.Get("http://example.com")
	return rsp, time.Now().Add(time.Hour), err
}

// Load the cached value if present and not expired, otherwise execute the task.
rsp, cacheHit, err := cache.LoadOrStore(ctx, "example.com", task)
if err != nil {
	// If this invocation of LoadOrStore caused the task to 
	// execute, any non-nil error from the task will be returned.
	panic(err)
}

// The cache will shut down and all data will be purged when the context is cancelled.
// Any subsequent calls to `Load` will bypass the cache.
cancel()
```

## Request coalescing

For any given key, only one task function will be in-flight at a time.
If a concurrent request comes in for the same key, the concurrent caller waits for the original to complete.
If the original task completes successfully (i.e., returns without error), all callers will receive the same result.

If the task returns a non-nil error, then the original caller will receive the error. A second caller waiting on the result will behave as though the key was not present in the cache (i.e., its own task function will be executed).
