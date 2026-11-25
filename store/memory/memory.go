// Package memory provides an in-process token bucket Store.
//
// It is suitable for single-instance services, tests, and as a local
// fallback when a shared store is unavailable. State is not shared between
// processes.
package memory

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/ChhaviRathore01/ratekeeper"
)

type bucket struct {
	tokens float64
	last   time.Time
	// full is when the bucket will be back at capacity; used for eviction.
	full time.Time
}

// Store is a token bucket store backed by an in-memory map.
type Store struct {
	mu      sync.Mutex
	buckets map[string]*bucket

	stop      chan struct{}
	closeOnce sync.Once
}

// Option configures a Store.
type Option func(*config)

type config struct {
	cleanupInterval time.Duration
}

// WithCleanupInterval sets how often idle, full buckets are evicted to bound
// memory use. Defaults to one minute. Zero or negative disables cleanup.
func WithCleanupInterval(d time.Duration) Option {
	return func(c *config) { c.cleanupInterval = d }
}

// New creates an in-memory Store. Call Close to stop the background cleanup.
func New(opts ...Option) *Store {
	cfg := config{cleanupInterval: time.Minute}
	for _, opt := range opts {
		opt(&cfg)
	}
	s := &Store{
		buckets: make(map[string]*bucket),
		stop:    make(chan struct{}),
	}
	if cfg.cleanupInterval > 0 {
		go s.janitor(cfg.cleanupInterval)
	}
	return s
}

var _ ratekeeper.Store = (*Store)(nil)

// Take implements ratekeeper.Store using the token bucket algorithm.
func (s *Store) Take(_ context.Context, key string, limit ratekeeper.Limit, n int, now time.Time) (ratekeeper.Result, error) {
	capacity := float64(limit.Capacity())
	// Nanoseconds per token, kept as float to avoid truncation drift when
	// Period is not evenly divisible by Rate.
	interval := float64(limit.Period) / float64(limit.Rate)

	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.buckets[key]
	if !ok {
		b = &bucket{tokens: capacity, last: now}
		s.buckets[key] = b
	}

	// Refill based on elapsed time. Guard against clocks moving backwards.
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = math.Min(capacity, b.tokens+float64(elapsed)/interval)
		b.last = now
	}

	res := ratekeeper.Result{Limit: limit.Capacity()}
	need := float64(n)
	if b.tokens >= need {
		b.tokens -= need
		res.Allowed = true
	} else {
		res.RetryAfter = durationFor(need-b.tokens, interval)
	}

	res.Remaining = int(math.Floor(b.tokens))
	res.ResetAt = now.Add(durationFor(capacity-b.tokens, interval))
	b.full = res.ResetAt
	return res, nil
}

// durationFor returns the time needed to accumulate tokens, rounded up so
// callers that wait exactly that long are guaranteed to succeed.
func durationFor(tokens, interval float64) time.Duration {
	if tokens <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(tokens * interval))
}

// Len returns the number of tracked keys.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buckets)
}

// Close stops the background cleanup goroutine. It is safe to call more
// than once.
func (s *Store) Close() {
	s.closeOnce.Do(func() { close(s.stop) })
}

func (s *Store) janitor(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-t.C:
			s.evictFull(now)
		}
	}
}

// evictFull removes buckets that have refilled completely. Dropping them is
// lossless: a missing bucket is recreated full on the next request.
func (s *Store) evictFull(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, b := range s.buckets {
		if !now.Before(b.full) {
			delete(s.buckets, k)
		}
	}
}
