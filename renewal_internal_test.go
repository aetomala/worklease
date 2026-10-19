package worklease

import (
	"testing"
	"time"
)

// TestDefaultRenewalInterval verifies that the default renewal interval is TTL/2
// and never reaches zero — time.NewTicker panics on a non-positive interval,
// which would crash the process from inside the renewal goroutine.
func TestDefaultRenewalInterval(t *testing.T) {
	cases := []struct {
		ttl  time.Duration
		want time.Duration
	}{
		{30 * time.Second, 15 * time.Second},
		{2 * time.Nanosecond, time.Nanosecond},
		{time.Nanosecond, time.Nanosecond},
	}

	for _, tc := range cases {
		if got := defaultRenewalInterval(tc.ttl); got != tc.want {
			t.Errorf("defaultRenewalInterval(%v) = %v, want %v", tc.ttl, got, tc.want)
		}
	}
}

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

// TestBackoffWait verifies that backoffWait draws from [base, base+jitter*base),
// that the draws spread across that range, and that a jitter of zero or less
// returns base exactly.
func TestBackoffWait(t *testing.T) {
	const base = 100 * time.Millisecond
	const jitter = 0.2
	upper := base + time.Duration(jitter*float64(base))
	lo, hi := upper, time.Duration(0)
	for range 10_000 {
		d := backoffWait(base, jitter)
		if d < base || d >= upper {
			t.Fatalf("backoffWait(%v, %v) = %v, want in [%v, %v)", base, jitter, d, base, upper)
		}
		lo, hi = min(lo, d), max(hi, d)
	}
	if spread := hi - lo; spread <= time.Duration(jitter*float64(base))/10 {
		t.Errorf("10,000 draws spread over %v, want more than %v", spread, time.Duration(jitter*float64(base))/10)
	}
	for _, j := range []float64{0, -1} {
		if d := backoffWait(base, j); d != base {
			t.Errorf("backoffWait(%v, %v) = %v, want %v", base, j, d, base)
		}
	}
}

// TestNextBackoff verifies that nextBackoff doubles its base and caps the result
// at maxInterval.
func TestNextBackoff(t *testing.T) {
	cases := []struct {
		base, maxInterval, want time.Duration
	}{
		{100 * time.Millisecond, 5 * time.Second, 200 * time.Millisecond},
		{3 * time.Second, 5 * time.Second, 5 * time.Second},
		{2500 * time.Millisecond, 5 * time.Second, 5 * time.Second},
		{5 * time.Second, 5 * time.Second, 5 * time.Second},
	}
	for _, tc := range cases {
		if got := nextBackoff(tc.base, tc.maxInterval); got != tc.want {
			t.Errorf("nextBackoff(%v, %v) = %v, want %v", tc.base, tc.maxInterval, got, tc.want)
		}
	}
}

// TestWithRenewalIntervalIgnoresNonPositive verifies that WithRenewalInterval
// keeps the prior interval for zero and negative values and sets a positive one.
func TestWithRenewalIntervalIgnoresNonPositive(t *testing.T) {
	const prior = 15 * time.Second
	for _, d := range []time.Duration{0, -time.Second} {
		c := renewalConfig{renewalInterval: prior}
		WithRenewalInterval(d)(&c)
		if c.renewalInterval != prior {
			t.Errorf("WithRenewalInterval(%v): renewalInterval = %v, want %v", d, c.renewalInterval, prior)
		}
	}
	c := renewalConfig{renewalInterval: prior}
	WithRenewalInterval(50 * time.Millisecond)(&c)
	if c.renewalInterval != 50*time.Millisecond {
		t.Errorf("WithRenewalInterval(50ms): renewalInterval = %v, want 50ms", c.renewalInterval)
	}
}
