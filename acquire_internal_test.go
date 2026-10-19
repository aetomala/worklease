package worklease

import (
	"testing"
	"time"
)

// TestWithPollIntervalIgnoresNonPositive verifies that WithPollInterval keeps the
// 2s default for zero and negative values and sets a positive one.
func TestWithPollIntervalIgnoresNonPositive(t *testing.T) {
	const def = 2 * time.Second
	for _, d := range []time.Duration{0, -time.Second} {
		c := acquireConfig{pollInterval: def}
		WithPollInterval(d)(&c)
		if c.pollInterval != def {
			t.Errorf("WithPollInterval(%v): pollInterval = %v, want %v", d, c.pollInterval, def)
		}
	}
	c := acquireConfig{pollInterval: def}
	WithPollInterval(50 * time.Millisecond)(&c)
	if c.pollInterval != 50*time.Millisecond {
		t.Errorf("WithPollInterval(50ms): pollInterval = %v, want 50ms", c.pollInterval)
	}
}
