// Package redisstore provides a Redis-backed ratekeeper.Store, so a limit is
// shared by every instance of a service.
//
// It implements GCRA (Generic Cell Rate Algorithm) in a Lua script, so each
// check is a single atomic round-trip: concurrent requests from many
// instances can never race each other. Each key stores one timestamp and
// expires as soon as its bucket is full again, so idle keys cost nothing.
//
// It works with any go-redis client that can run scripts, including
// *redis.Client, *redis.ClusterClient and *redis.Ring. Every operation
// touches a single key, so it is safe on Redis Cluster.
package redisstore

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ChhaviRathore01/ratekeeper"
)

//go:embed gcra.lua
var gcraSource string

var gcraScript = redis.NewScript(gcraSource)

// epoch must match EPOCH in gcra.lua. Timestamps are sent relative to it to
// keep them small enough for exact floating point arithmetic in Lua.
var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// DefaultKeyPrefix namespaces all keys written by the store.
const DefaultKeyPrefix = "ratekeeper:"

// Store is a Redis-backed GCRA store.
type Store struct {
	client     redis.Scripter
	prefix     string
	clientTime bool
}

// Option configures a Store.
type Option func(*Store)

// WithKeyPrefix sets the prefix for all Redis keys. Defaults to
// DefaultKeyPrefix.
func WithKeyPrefix(prefix string) Option {
	return func(s *Store) { s.prefix = prefix }
}

// WithClientTime makes the store use the time supplied by the Limiter
// instead of the Redis server clock.
//
// By default the server clock is used, so clock skew between application
// instances cannot affect limits. Client time is useful for deterministic
// tests or for Redis-compatible servers that do not support TIME in scripts.
func WithClientTime() Option {
	return func(s *Store) { s.clientTime = true }
}

// New creates a Store using client.
func New(client redis.Scripter, opts ...Option) *Store {
	s := &Store{client: client, prefix: DefaultKeyPrefix}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

var _ ratekeeper.Store = (*Store)(nil)

// Take implements ratekeeper.Store.
func (s *Store) Take(ctx context.Context, key string, limit ratekeeper.Limit, n int, now time.Time) (ratekeeper.Result, error) {
	intervalMicros := float64(limit.Period) / float64(limit.Rate) / float64(time.Microsecond)

	nowArg := "server"
	if s.clientTime {
		nowArg = strconv.FormatInt(now.Sub(epoch).Microseconds(), 10)
	}

	raw, err := gcraScript.Run(ctx, s.client,
		[]string{s.prefix + key},
		strconv.FormatFloat(intervalMicros, 'f', -1, 64),
		limit.Capacity(),
		n,
		nowArg,
	).Int64Slice()
	if err != nil {
		return ratekeeper.Result{}, fmt.Errorf("redisstore: %w", err)
	}
	if len(raw) != 4 {
		return ratekeeper.Result{}, fmt.Errorf("redisstore: unexpected script result %v", raw)
	}

	return ratekeeper.Result{
		Allowed:    raw[0] == 1,
		Limit:      limit.Capacity(),
		Remaining:  int(raw[1]),
		RetryAfter: time.Duration(raw[2]) * time.Microsecond,
		ResetAt:    now.Add(time.Duration(raw[3]) * time.Microsecond),
	}, nil
}
