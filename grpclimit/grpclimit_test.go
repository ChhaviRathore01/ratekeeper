package grpclimit_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/ChhaviRathore01/ratekeeper"
	"github.com/ChhaviRathore01/ratekeeper/grpclimit"
	"github.com/ChhaviRathore01/ratekeeper/store/memory"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

const checkMethod = "/grpc.health.v1.Health/Check"

func newLimiter(t *testing.T, limit ratekeeper.Limit, now *time.Time) *ratekeeper.Limiter {
	t.Helper()
	s := memory.New(memory.WithCleanupInterval(0))
	t.Cleanup(s.Close)
	l, err := ratekeeper.New(s, limit, ratekeeper.WithClock(func() time.Time { return *now }))
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// startServer runs a real gRPC server (the standard health service) over an
// in-memory connection, with the interceptors installed.
func startServer(t *testing.T, l *ratekeeper.Limiter, opts ...grpclimit.Option) healthpb.HealthClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(grpclimit.UnaryServerInterceptor(l, opts...)),
		grpc.ChainStreamInterceptor(grpclimit.StreamServerInterceptor(l, opts...)),
	)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return healthpb.NewHealthClient(conn)
}

func check(c healthpb.HealthClient, md ...string) (metadata.MD, error) {
	ctx := context.Background()
	if len(md) > 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, md...)
	}
	var header metadata.MD
	_, err := c.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.Header(&header))
	return header, err
}

func TestUnaryAllowsThenDenies(t *testing.T) {
	now := t0
	c := startServer(t, newLimiter(t, ratekeeper.PerMinute(2), &now))

	for i, want := range []string{"1", "0"} {
		h, err := check(c)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if got := h.Get(grpclimit.MDRemaining); len(got) != 1 || got[0] != want {
			t.Errorf("call %d: remaining = %v, want %s", i, got, want)
		}
		if got := h.Get(grpclimit.MDLimit); len(got) != 1 || got[0] != "2" {
			t.Errorf("limit = %v", got)
		}
	}

	h, err := check(c)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("err = %v, want ResourceExhausted", err)
	}
	if got := h.Get(grpclimit.MDRetryAfter); len(got) != 1 || got[0] != "30" {
		t.Errorf("retry-after = %v, want 30", got)
	}
	if got := h.Get(grpclimit.MDReset); len(got) != 1 || got[0] != "1767225660" {
		t.Errorf("reset = %v", got)
	}

	now = now.Add(30 * time.Second)
	if _, err := check(c); err != nil {
		t.Fatalf("after retry-after: %v", err)
	}
}

func TestByMetadata(t *testing.T) {
	now := t0
	c := startServer(t, newLimiter(t, ratekeeper.PerMinute(1), &now),
		grpclimit.WithKeyFunc(grpclimit.ByMetadata("x-api-key")))

	if _, err := check(c); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing key: err = %v, want InvalidArgument", err)
	}
	if _, err := check(c, "x-api-key", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := check(c, "x-api-key", "a"); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("second call for key a: %v", err)
	}
	if _, err := check(c, "x-api-key", "b"); err != nil {
		t.Fatalf("key b should have its own limit: %v", err)
	}
}

func TestStreamCountsOncePerStream(t *testing.T) {
	now := t0
	c := startServer(t, newLimiter(t, ratekeeper.PerMinute(1), &now))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := c.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first stream should be allowed: %v", err)
	}

	stream2, err := c.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream2.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("second stream: err = %v, want ResourceExhausted", err)
	}
	h, _ := stream2.Header()
	if got := h.Get(grpclimit.MDRetryAfter); len(got) != 1 {
		t.Errorf("stream denial should carry retry-after, got %v", h)
	}
}

func TestSkip(t *testing.T) {
	now := t0
	c := startServer(t, newLimiter(t, ratekeeper.PerMinute(1), &now),
		grpclimit.WithSkip(func(_ context.Context, m string) bool { return m == checkMethod }))
	for i := 0; i < 5; i++ {
		if _, err := check(c); err != nil {
			t.Fatalf("skipped method limited: %v", err)
		}
	}
}

func TestCostAboveBurstIsInternal(t *testing.T) {
	now := t0
	c := startServer(t, newLimiter(t, ratekeeper.PerMinute(1), &now),
		grpclimit.WithCost(func(context.Context, string) int { return 2 }))
	if _, err := check(c); status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want Internal", err)
	}
}

func TestPerMethod(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("203.0.113.9"), Port: 4321},
	})
	key, err := grpclimit.PerMethod(grpclimit.ByPeer)(ctx, checkMethod)
	if err != nil || key != checkMethod+"|203.0.113.9" {
		t.Fatalf("key = %q, err = %v", key, err)
	}
	if _, err := grpclimit.ByPeer(context.Background(), checkMethod); err != grpclimit.ErrNoKey {
		t.Fatalf("no peer: err = %v, want ErrNoKey", err)
	}
}

func TestWithoutHeaders(t *testing.T) {
	now := t0
	c := startServer(t, newLimiter(t, ratekeeper.PerMinute(1), &now), grpclimit.WithoutHeaders())
	h, err := check(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Get(grpclimit.MDLimit)) != 0 {
		t.Fatal("rate limit metadata should be disabled")
	}
	h, _ = check(c)
	if len(h.Get(grpclimit.MDRetryAfter)) != 1 {
		t.Fatal("retry-after should still be sent on denial")
	}
}
