package postgres

import (
	"testing"
	"time"
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
