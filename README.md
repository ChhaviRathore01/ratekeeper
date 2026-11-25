# ratekeeper

[![CI](https://github.com/ChhaviRathore01/ratekeeper/actions/workflows/ci.yml/badge.svg)](https://github.com/ChhaviRathore01/ratekeeper/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ChhaviRathore01/ratekeeper.svg)](https://pkg.go.dev/github.com/ChhaviRathore01/ratekeeper)
[![Go Report Card](https://goreportcard.com/badge/github.com/ChhaviRathore01/ratekeeper)](https://goreportcard.com/report/github.com/ChhaviRathore01/ratekeeper)

A distributed rate limiter for Go with pluggable storage backends. Enforce the
same limit on a single process or across every instance of your service.

> **Status:** early development (v0.2). The API may change before v1.0.

## Features

- **Distributed:** Redis store shares limits across every instance, one atomic round-trip per check
- **Two algorithms, one behaviour:** token bucket (in-memory) and GCRA (Redis), verified identical by a shared conformance suite
- Uses the Redis server clock by default, so clock skew between app servers cannot affect limits
- Redis Cluster safe (single key per operation); idle keys expire automatically
- Configurable rate, period, and burst
- Atomic `AllowN` (all-or-nothing) for weighted requests
- Rich results: `Remaining`, `RetryAfter`, `ResetAt` — ready for `X-RateLimit-*` / `Retry-After` headers
- Configurable failure policy (fail open / fail closed) when the backend is unavailable
- Key prefixes so multiple limiters can share one store
- Zero-allocation in-memory store with automatic eviction of idle keys
- Injectable clock for deterministic tests

## Install

```sh
go get github.com/ChhaviRathore01/ratekeeper
```

## Quick start

```go
// Shared across all instances:
store := redisstore.New(redis.NewClient(&redis.Options{Addr: "localhost:6379"}))

// Or single-process:
// store := memory.New()
// defer store.Close()

limiter, err := ratekeeper.New(store, ratekeeper.PerMinute(100).WithBurst(20),
    ratekeeper.WithFailurePolicy(ratekeeper.FailOpen),
)
if err != nil {
    log.Fatal(err)
}

res, err := limiter.Allow(ctx, "user:42")
if err != nil {
    return err // invalid arguments only
}
if !res.Allowed {
    w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(res.RetryAfter.Seconds()))))
    http.Error(w, "too many requests", http.StatusTooManyRequests)
    return
}
```

## Design

```
            ┌──────────────┐
request ──► │   Limiter    │  validates input, applies key prefix,
            │              │  handles store failures per FailurePolicy
            └──────┬───────┘
                   │ Take(key, limit, n, now)
            ┌──────▼───────┐
            │    Store     │  applies the algorithm atomically
            ├──────────────┤
            │ memory       │  token bucket, single process
            │ redisstore   │  GCRA in Lua, shared across instances
            └──────────────┘
```

The algorithm lives in the store so that shared backends can run it
atomically server-side (e.g. a Redis Lua script) in a single round-trip,
avoiding read-modify-write races between instances.

### Token bucket (memory store)

Each key has a bucket holding up to `Capacity` tokens (`Burst`, or `Rate` if
unset). Tokens refill continuously at `Rate / Period`. A request for `n`
tokens succeeds only if at least `n` are available; denied requests consume
nothing.

### GCRA (Redis store)

[GCRA](https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm) stores a
single number per key: the *theoretical arrival time* (TAT), the moment the
bucket would be full again. A request for `n` tokens is allowed if
`TAT + n·interval − burst·interval ≤ now`. That makes it mathematically
equivalent to a token bucket, but it needs only one `GET` and one `SET`, and
the key's TTL is simply `TAT − now`.

The algorithm runs as a [Lua script](store/redisstore/gcra.lua), so the
read-check-write sequence is atomic in Redis: concurrent requests from any
number of instances can never over-admit.

**Floating point gotcha:** Lua numbers are 64-bit floats, and a Unix
timestamp in microseconds (~1.7×10¹⁵) has only ~0.25µs resolution — enough
to make `RetryAfter` come out shorter than the true wait. The script keeps
timestamps relative to a 2025 epoch and does all arithmetic on small
offsets from `now`.

### Conformance suite

[`store/storetest`](store/storetest/storetest.go) is a shared test suite
every store must pass (refill, burst, weighted requests, clock skew,
concurrency, `RetryAfter` correctness). It runs against the memory store,
miniredis, and a real Redis in CI — proving the backends are interchangeable.

## Benchmarks

Apple M-series, Go 1.21, Redis 8 on localhost:

| Store | Benchmark | ns/op | allocs/op |
|---|---|---|---|
| memory | single goroutine, one key | ~16 | 0 |
| memory | 12 goroutines, one hot key | ~104 | 0 |
| redisstore | 12 goroutines, one hot key | ~12,100 (~82k checks/s) | 21 |

The Redis cost is dominated by the network round-trip, which is what the
planned hybrid mode (v0.4) addresses.

Run them yourself:

```sh
go test -run xxx -bench . ./store/memory
REDIS_ADDR=localhost:6379 go test -run xxx -bench . ./store/redisstore
```

## Roadmap

- [x] v0.1 — core API, in-memory token bucket store
- [x] v0.2 — Redis store (GCRA in atomic Lua), shared store conformance suite
- [ ] v0.2.x — sliding-window algorithm
- [ ] v0.3 — `net/http` and gRPC middleware, standard rate limit headers
- [ ] v0.4 — hybrid local/global mode (local token leasing to cut backend round-trips)
- [ ] v1.0 — Prometheus/OpenTelemetry metrics, docs, stable API

## Development

```sh
go test -race ./...                                  # unit tests (miniredis)
REDIS_ADDR=localhost:6379 go test -race ./...        # + real Redis integration tests
```

## License

[MIT](LICENSE)
