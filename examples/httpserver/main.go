// Command httpserver is a runnable demo of ratekeeper's HTTP middleware.
//
//	go run ./examples/httpserver                        # in-memory store
//	go run ./examples/httpserver -redis localhost:6379  # shared via Redis
//
// Then: for i in $(seq 1 7); do curl -si localhost:8080/hello | head -5; done
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ChhaviRathore01/ratekeeper"
	"github.com/ChhaviRathore01/ratekeeper/httplimit"
	"github.com/ChhaviRathore01/ratekeeper/store/memory"
	"github.com/ChhaviRathore01/ratekeeper/store/redisstore"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	redisAddr := flag.String("redis", "", "Redis address; empty uses an in-memory store")
	flag.Parse()

	var store ratekeeper.Store
	if *redisAddr != "" {
		store = redisstore.New(redis.NewClient(&redis.Options{Addr: *redisAddr}))
		log.Printf("using Redis at %s", *redisAddr)
	} else {
		m := memory.New()
		defer m.Close()
		store = m
		log.Print("using in-memory store")
	}

	// 5 requests per 10 seconds per client IP. Fail open: if Redis goes
	// down, keep serving rather than rejecting everyone.
	limiter, err := ratekeeper.New(store, ratekeeper.Limit{Rate: 5, Period: 10 * time.Second},
		ratekeeper.WithKeyPrefix("demo:"),
		ratekeeper.WithFailurePolicy(ratekeeper.FailOpen),
		ratekeeper.WithErrorHandler(func(key string, err error) {
			log.Printf("rate limiter store error for %q: %v", key, err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "hello!")
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})

	handler := httplimit.Middleware(limiter,
		httplimit.WithSkip(func(r *http.Request) bool { return r.URL.Path == "/healthz" }),
	)(mux)

	log.Printf("listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, handler))
}
