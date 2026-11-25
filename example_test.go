package ratekeeper_test

import (
	"context"
	"fmt"

	"github.com/ChhaviRathore01/ratekeeper"
	"github.com/ChhaviRathore01/ratekeeper/store/memory"
)

func Example() {
	store := memory.New()
	defer store.Close()

	limiter, err := ratekeeper.New(store, ratekeeper.PerSecond(2))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		res, _ := limiter.Allow(ctx, "user:42")
		fmt.Printf("request %d allowed=%v remaining=%d\n", i, res.Allowed, res.Remaining)
	}
	// Output:
	// request 1 allowed=true remaining=1
	// request 2 allowed=true remaining=0
	// request 3 allowed=false remaining=0
}
