package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lib/pq"
)

// TestFormatInterval verifies that formatInterval renders whole microseconds,
// rounding a positive remainder up and truncating a negative one toward zero.
func TestFormatInterval(t *testing.T) {
	cases := []struct {
		name string
		d    time.Duration
		want string
	}{
		{"zero", 0, "0 microseconds"},
		{"one nanosecond rounds up", time.Nanosecond, "1 microseconds"},
		{"499 nanoseconds rounds up", 499 * time.Nanosecond, "1 microseconds"},
		{"exactly one microsecond", time.Microsecond, "1 microseconds"},
		{"one microsecond plus one nanosecond", time.Microsecond + 1, "2 microseconds"},
		{"one millisecond", time.Millisecond, "1000 microseconds"},
		{"thirty seconds", 30 * time.Second, "30000000 microseconds"},
		{"negative one second", -time.Second, "-1000000 microseconds"},
		{"negative 1500 nanoseconds rounds toward zero", -1500 * time.Nanosecond, "-1 microseconds"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatInterval(tc.d); got != tc.want {
				t.Errorf("formatInterval(%v) = %q, want %q", tc.d, got, tc.want)
			}
		})
	}
}

// TestWithCtxErr verifies that withCtxErr returns the driver error unchanged
// while the context is live, and once it is done wraps both the driver error
// and the context error, adding only the context error's text to the message.
func TestWithCtxErr(t *testing.T) {
	driverErr := &pq.Error{Code: "57014", Message: "canceling statement due to user request"}

	if got := withCtxErr(context.Background(), driverErr); got != error(driverErr) {
		t.Errorf("live ctx: withCtxErr returned %v, want the driver error unchanged", got)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	got := withCtxErr(cancelled, driverErr)
	if !errors.Is(got, context.Canceled) {
		t.Errorf("cancelled ctx: errors.Is(%v, context.Canceled) = false", got)
	}
	var pqErr *pq.Error
	if !errors.As(got, &pqErr) || pqErr.Code != "57014" {
		t.Errorf("cancelled ctx: errors.As found no *pq.Error with code 57014 in %v", got)
	}
	if want := driverErr.Error() + " (context canceled)"; got.Error() != want {
		t.Errorf("cancelled ctx: message = %q, want %q", got.Error(), want)
	}

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	got = withCtxErr(expired, driverErr)
	if !errors.Is(got, context.DeadlineExceeded) {
		t.Errorf("expired ctx: errors.Is(%v, context.DeadlineExceeded) = false", got)
	}
	if !errors.As(got, &pqErr) {
		t.Errorf("expired ctx: errors.As found no *pq.Error in %v", got)
	}
}
