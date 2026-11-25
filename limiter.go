// Package ratekeeper provides rate limiting with pluggable storage backends,
// so the same limit can be enforced on a single process (in-memory store) or
// across many instances of a service (shared store such as Redis).
package ratekeeper

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Limit describes how many requests are allowed over a period.
//
// Rate requests are replenished evenly over Period. Burst is the maximum
// number of requests that can be made at once; if zero it defaults to Rate.
type Limit struct {
	Rate   int
	Period time.Duration
	Burst  int
}

// PerSecond returns a Limit of rate requests per second.
func PerSecond(rate int) Limit { return Limit{Rate: rate, Period: time.Second} }

// PerMinute returns a Limit of rate requests per minute.
func PerMinute(rate int) Limit { return Limit{Rate: rate, Period: time.Minute} }

// PerHour returns a Limit of rate requests per hour.
func PerHour(rate int) Limit { return Limit{Rate: rate, Period: time.Hour} }

// WithBurst returns a copy of l with the given burst size.
func (l Limit) WithBurst(burst int) Limit {
	l.Burst = burst
	return l
}

// Capacity is the effective bucket size (Burst, or Rate when Burst is unset).
func (l Limit) Capacity() int {
	if l.Burst > 0 {
		return l.Burst
	}
	return l.Rate
}

// Interval is the time it takes to replenish a single request.
func (l Limit) Interval() time.Duration {
	return l.Period / time.Duration(l.Rate)
}

func (l Limit) validate() error {
	switch {
	case l.Rate <= 0:
		return fmt.Errorf("%w: rate must be > 0, got %d", ErrInvalidLimit, l.Rate)
	case l.Period <= 0:
		return fmt.Errorf("%w: period must be > 0, got %s", ErrInvalidLimit, l.Period)
	case l.Burst < 0:
		return fmt.Errorf("%w: burst must be >= 0, got %d", ErrInvalidLimit, l.Burst)
	case l.Interval() <= 0:
		return fmt.Errorf("%w: rate %d is too high for period %s", ErrInvalidLimit, l.Rate, l.Period)
	}
	return nil
}

// Result is the outcome of a rate limit check.
type Result struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the bucket capacity that was applied.
	Limit int
	// Remaining is the number of requests still available right now.
	Remaining int
	// RetryAfter is how long to wait before the request would be allowed.
	// Zero when Allowed is true.
	RetryAfter time.Duration
	// ResetAt is when the bucket will be completely full again.
	ResetAt time.Time
}

// Store persists rate limit state and applies the limiting algorithm
// atomically. Implementations must be safe for concurrent use.
//
// Take attempts to consume n tokens for key at time now. It must not consume
// anything when the request is denied.
type Store interface {
	Take(ctx context.Context, key string, limit Limit, n int, now time.Time) (Result, error)
}

// FailurePolicy decides what happens when the Store returns an error
// (for example, the shared backend is unreachable).
type FailurePolicy int

const (
	// FailClosed denies requests when the store fails. This is the default.
	FailClosed FailurePolicy = iota
	// FailOpen allows requests when the store fails, favouring availability.
	FailOpen
)

var (
	// ErrInvalidLimit is returned when a Limit is misconfigured.
	ErrInvalidLimit = errors.New("ratekeeper: invalid limit")
	// ErrExceedsBurst is returned when a single request asks for more tokens
	// than the bucket can ever hold, so it could never succeed.
	ErrExceedsBurst = errors.New("ratekeeper: n exceeds burst capacity")
	// ErrEmptyKey is returned when the key is empty.
	ErrEmptyKey = errors.New("ratekeeper: key must not be empty")
)

// Limiter enforces a Limit per key using a Store.
type Limiter struct {
	store   Store
	limit   Limit
	policy  FailurePolicy
	onError func(key string, err error)
	now     func() time.Time
	prefix  string
}

// Option configures a Limiter.
type Option func(*Limiter)

// WithFailurePolicy sets the behaviour when the store returns an error.
func WithFailurePolicy(p FailurePolicy) Option {
	return func(l *Limiter) { l.policy = p }
}

// WithErrorHandler registers a callback invoked whenever the store fails,
// regardless of failure policy. Useful for logging and metrics.
func WithErrorHandler(fn func(key string, err error)) Option {
	return func(l *Limiter) { l.onError = fn }
}

// WithKeyPrefix namespaces all keys, so several limiters can share a store.
func WithKeyPrefix(prefix string) Option {
	return func(l *Limiter) { l.prefix = prefix }
}

// WithClock overrides the time source. Intended for tests.
func WithClock(now func() time.Time) Option {
	return func(l *Limiter) { l.now = now }
}

// New creates a Limiter that enforces limit using store.
func New(store Store, limit Limit, opts ...Option) (*Limiter, error) {
	if store == nil {
		return nil, errors.New("ratekeeper: store must not be nil")
	}
	if err := limit.validate(); err != nil {
		return nil, err
	}
	l := &Limiter{store: store, limit: limit, policy: FailClosed, now: time.Now}
	for _, opt := range opts {
		opt(l)
	}
	return l, nil
}

// Limit returns the limit enforced by l.
func (l *Limiter) Limit() Limit { return l.limit }

// Allow reports whether one request for key may proceed, consuming a token
// if so.
func (l *Limiter) Allow(ctx context.Context, key string) (Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN reports whether n requests for key may proceed at once, consuming
// n tokens if so. Either all n are allowed or none are.
//
// If the store fails, the returned error is nil and Result.Allowed follows
// the configured FailurePolicy; use WithErrorHandler to observe the failure.
// Errors are returned only for invalid arguments.
func (l *Limiter) AllowN(ctx context.Context, key string, n int) (Result, error) {
	if key == "" {
		return Result{}, ErrEmptyKey
	}
	if n <= 0 {
		return Result{}, fmt.Errorf("ratekeeper: n must be > 0, got %d", n)
	}
	if n > l.limit.Capacity() {
		return Result{}, fmt.Errorf("%w: n=%d, capacity=%d", ErrExceedsBurst, n, l.limit.Capacity())
	}

	res, err := l.store.Take(ctx, l.prefix+key, l.limit, n, l.now())
	if err != nil {
		if l.onError != nil {
			l.onError(key, err)
		}
		return Result{
			Allowed:   l.policy == FailOpen,
			Limit:     l.limit.Capacity(),
			Remaining: 0,
		}, nil
	}
	return res, nil
}
