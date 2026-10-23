package worklease_test

import (
	"fmt"
	"time"

	"github.com/aetomala/worklease"
)

func ExampleNewToken() {
	expires := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	token := worklease.NewToken("work-1", "worker-1", 7, expires)
	fmt.Println(token.WorkID(), token.HolderID(), token.FencingToken(), token.ExpiresAt().Format(time.RFC3339))
	// Output: work-1 worker-1 7 2026-01-01T12:00:00Z
}
