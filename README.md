# ratekeeper

[![CI](https://github.com/ChhaviRathore01/ratekeeper/actions/workflows/ci.yml/badge.svg)](https://github.com/ChhaviRathore01/ratekeeper/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ChhaviRathore01/ratekeeper.svg)](https://pkg.go.dev/github.com/ChhaviRathore01/ratekeeper)
[![Go Report Card](https://goreportcard.com/badge/github.com/ChhaviRathore01/ratekeeper)](https://goreportcard.com/report/github.com/ChhaviRathore01/ratekeeper)

A distributed rate limiter for Go with pluggable storage backends. Enforce the
same limit on a single process or across every instance of your service.

> **Status:** early development (v0.4). The API may change before v1.0.

## Features

- **Distributed:** Redis store shares limits across every instance, one atomic round-trip per check
- **Two algorithms, one behaviour:** token bucket (in-memory) and GCRA (Redis), verified identical by a shared conformance suite
- Uses the Redis server clock by default, so clock skew between app servers cannot affect limits
- Redis Cluster safe (single key per operation); idle keys expire automatically
- **Hybrid mode:** lease tokens from Redis in batches and serve from memory — up to **100× fewer Redis calls and 32× lower latency**, with a proof-by-simulation that it never over-admits
- **Drop-in middleware** for `net/http` and gRPC (unary + streaming), with standard rate limit headers
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

// Or with local token leasing for high-volume keys:
// store := hybrid.New(redisstore.New(client), hybrid.WithBatchSize(20))

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

## HTTP middleware

```go
limiter, _ := ratekeeper.New(store, ratekeeper.PerMinute(60))

handler := httplimit.Middleware(limiter,
    httplimit.WithKeyFunc(httplimit.ByForwardedFor(1)), // behind one load balancer
    httplimit.WithSkip(func(r *http.Request) bool { return r.URL.Path == "/healthz" }),
)(mux)
```

Responses carry `X-RateLimit-Limit`, `X-RateLimit-Remaining` and
`X-RateLimit-Reset` (Unix seconds). Denied requests get
`429 Too Many Requests` with `Retry-After`.

| Option | Purpose |
|---|---|
| `WithKeyFunc` | `ByIP` (default), `ByForwardedFor(n)`, `ByHeader("X-API-Key")`, or your own |
| `WithCost` | make expensive endpoints consume more tokens |
| `WithSkip` | exempt health checks, internal traffic, … |
| `WithDeniedHandler` / `WithErrorHandler` | custom 429 / error responses (e.g. JSON) |

> **Why `ByForwardedFor(n)` and not just the first `X-Forwarded-For` entry?**
> Clients can send any `X-Forwarded-For` value they like, so keying on the
> leftmost IP lets an attacker get a fresh limit on every request. Each
> trusted proxy *appends* the address it saw, so the real client is the
> entry `n` positions from the right.

Try it:

```sh
go run ./examples/httpserver            # or: -redis localhost:6379
for i in $(seq 1 6); do curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/hello; done
```

## gRPC interceptors

```go
srv := grpc.NewServer(
    grpc.ChainUnaryInterceptor(grpclimit.UnaryServerInterceptor(limiter,
        grpclimit.WithKeyFunc(grpclimit.ByMetadata("x-api-key")))),
    grpc.ChainStreamInterceptor(grpclimit.StreamServerInterceptor(limiter)),
)
```

Denied calls fail with `codes.ResourceExhausted`; response metadata carries
`x-ratelimit-*` and `retry-after`. Key by peer IP (default), metadata, or
`PerMethod(...)` to give every RPC its own limit. Streams count once when
opened.

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
            │ hybrid       │  wraps any store: local leases + batching
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

### Hybrid mode (token leasing)

At high volume, one Redis round-trip per request becomes the bottleneck.
[`store/hybrid`](store/hybrid/hybrid.go) wraps any store and, on a miss,
takes a *batch* of tokens in one call and serves the following requests for
that key from memory.

```
           instance A                     instance B
  req ─► [lease: 7 tokens] ─┐    ┌─ [lease: 2 tokens] ◄─ req
  req ─►        │           │    │          │          ◄─ req
            (empty)         ▼    ▼      (empty)
                       ┌─────────────┐
                       │   Redis     │  one Take(n=batch) per lease
                       └─────────────┘
```

**Correctness argument.** Every token used locally was granted by the
backend first, so the cluster can never admit more than the limit. The cost
runs the other way: tokens leased by one instance can't be used by another,
and unused tokens are dropped when the lease expires (default 1s). Worst case
is `instances × (batch − 1)` *fewer* admissions than the limit.

Two refinements keep that cost low:

- **Adaptive leasing** — when the backend reports fewer tokens left than one
  batch, the instance stops leasing and asks for exactly what each request
  needs, so behaviour near the limit is exact.
- **Negative caching** — when the backend says "no tokens for 40ms", the
  instance denies that key locally for 40ms. Other instances can only consume
  tokens, never create them, so this is always safe.

Leased tokens also keep serving traffic through short backend outages.

**Simulation results** (5 instances sharing one backend, 10s of simulated
traffic, batch 20 — see `TestSimulation*`):

| Load | Admitted vs exact limiter | Backend calls |
|---|---|---|
| 50% of limit | 100% (no false denials) | **20× fewer** |
| 500% of limit | **100.00%** (109,990 vs 109,990) | 3.6× fewer |

Under overload, the saving comes from negative caching; leasing deliberately
switches off near the limit.

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
| redisstore | 12 goroutines, one hot key | ~12,200 (~82k checks/s) | 22 |
| hybrid (batch 10) over redisstore | 12 goroutines, one hot key | ~3,200 — 0.1 Redis calls/op | 2 |
| hybrid (batch 100) over redisstore | 12 goroutines, one hot key | **~380 — 0.01 Redis calls/op** | 0 |

The Redis cost is dominated by the network round-trip; hybrid mode removes
most of them.

Run them yourself:

```sh
go test -run xxx -bench . ./store/memory
REDIS_ADDR=localhost:6379 go test -run xxx -bench . ./store/redisstore ./store/hybrid
```

## Roadmap

- [x] v0.1 — core API, in-memory token bucket store
- [x] v0.2 — Redis store (GCRA in atomic Lua), shared store conformance suite
- [ ] v0.2.x — sliding-window algorithm
- [x] v0.3 — `net/http` and gRPC middleware, standard rate limit headers
- [x] v0.4 — hybrid mode: local token leasing, adaptive batching, negative caching
- [ ] v1.0 — Prometheus/OpenTelemetry metrics, docs, stable API

## Development

```sh
go test -race ./...                                  # unit tests (miniredis)
REDIS_ADDR=localhost:6379 go test -race ./...        # + real Redis integration tests
```

## License

[MIT](LICENSE)
