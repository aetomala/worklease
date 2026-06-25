package worklease

import (
	"testing"
	"time"
)

// TestWithRenewalBackoffClamping verifies the clamping rules of WithRenewalBackoff
// against the unexported renewalConfig. It lives in package worklease (internal)
// because renewalConfig fields are not visible to the external test package.
func TestWithRenewalBackoffClamping(t *testing.T) {
	cases := []struct {
		name        string
		initial     time.Duration
		maxInterval time.Duration
		jitter      float64
		wantInitial time.Duration
		wantMax     time.Duration
		wantJitter  float64
	}{
		{"non-positive initial floors to 1ms", 0, 5 * time.Second, 0.2, time.Millisecond, 5 * time.Second, 0.2},
		{"non-positive max floors to 1ms then initial caps at max", 100 * time.Millisecond, 0, 0.2, time.Millisecond, time.Millisecond, 0.2},
		{"negative jitter clamps to 0", 100 * time.Millisecond, 5 * time.Second, -1, 100 * time.Millisecond, 5 * time.Second, 0},
		{"jitter above 1 clamps to 1", 100 * time.Millisecond, 5 * time.Second, 2, 100 * time.Millisecond, 5 * time.Second, 1},
		{"initial greater than max caps at max", 10 * time.Second, 5 * time.Second, 0.2, 5 * time.Second, 5 * time.Second, 0.2},
		{"valid values pass through unchanged", 200 * time.Millisecond, 2 * time.Second, 0.5, 200 * time.Millisecond, 2 * time.Second, 0.5},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c renewalConfig
			WithRenewalBackoff(tc.initial, tc.maxInterval, tc.jitter)(&c)
			if c.backoffInitial != tc.wantInitial {
				t.Errorf("backoffInitial = %v, want %v", c.backoffInitial, tc.wantInitial)
			}
			if c.backoffMax != tc.wantMax {
				t.Errorf("backoffMax = %v, want %v", c.backoffMax, tc.wantMax)
			}
			if c.backoffJitter != tc.wantJitter {
				t.Errorf("backoffJitter = %v, want %v", c.backoffJitter, tc.wantJitter)
			}
		})
	}
}
