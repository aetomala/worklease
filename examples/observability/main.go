// Command observability demonstrates a stdlib-only worklease.LeaseObserver that
// produces the signals a production deployment cares about — without pulling in a
// metrics or tracing library.
//
// It implements the five patterns that are non-obvious from the LeaseObserver
// interface alone:
//
//  1. Per-operation call counting (Acquire / Checkpoint / Renew / Release / ReadCheckpoint).
//  2. Per-operation latency, read directly from the v0.4 Duration field on each event
//     (no Lease wrapping required).
//  3. Lease hold duration, via stateful correlation between OnAcquire and OnRelease
//     keyed on token.FencingToken() — the fencing token is the stable identity of a
//     single holding, so it is the correct correlation key.
//  4. A dedicated fencing counter via OnFenced, which fires *in addition to* the
//     operation callback (OnCheckpoint/OnRenew/OnRelease) — so a fencing metric can be
//     incremented in one place without parsing error values out of every operation.
//  5. Renewal retry tracking via RenewEvent.Attempt — Attempt > 1 means the v0.5
//     renewal goroutine retried after a transient non-fencing error (e.g., a Postgres
//     connection drop); a dedicated counter makes this visible without inspecting errors.
//
// The example runs a clean lifecycle and a real fencing scenario (a successor steals
// an expired lease, fencing the original holder) so every callback fires.
//
// # Mapping to a real metrics backend
//
// Each field below corresponds to a standard instrument:
//
//   - opCalls        -> prometheus.CounterVec{labels: "operation"}        / otel Int64Counter
//   - opErrors       -> prometheus.CounterVec{labels: "operation"}        / otel Int64Counter
//   - opLatency      -> prometheus.HistogramVec{labels: "operation"}      / otel Float64Histogram
//   - fencedTotal    -> prometheus.Counter                                / otel Int64Counter
//   - holdDurations  -> prometheus.Histogram (lease_hold_seconds)         / otel Float64Histogram
//   - renewRetries   -> prometheus.Counter (renew_retry_total)            / otel Int64Counter
//
// Replace the in-memory aggregation with .Inc() / .Observe() calls on those instruments;
// the callback bodies and correlation logic stay identical.
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend/memory"
)

// metricsObserver is a stdlib-only LeaseObserver that aggregates counts, latency,
// hold duration, and fencing events in memory. All methods are called synchronously
// by the library, so the mutex is the only concurrency concern.
type metricsObserver struct {
	mu sync.Mutex

	// ===== Per-operation call counts and latency totals =====
	opCalls   map[string]int           // operation -> call count
	opErrors  map[string]int           // operation -> error count
	opLatency map[string]time.Duration // operation -> summed final-backend-call duration

	// ===== Dedicated fencing counter =====
	fencedTotal int

	// ===== Hold-duration correlation: fencing token -> acquire time =====
	heldSince     map[uint64]time.Time
	holdDurations []time.Duration

	// ===== Renewal retry counter =====
	renewRetries int // incremented for each OnRenew with Attempt > 1; non-zero signals transient errors
}

func newMetricsObserver() *metricsObserver {
	return &metricsObserver{
		opCalls:   map[string]int{},
		opErrors:  map[string]int{},
		opLatency: map[string]time.Duration{},
		heldSince: map[uint64]time.Time{},
	}
}

// Compile-time assertion that metricsObserver satisfies the interface.
var _ worklease.LeaseObserver = (*metricsObserver)(nil)

// record is the shared bookkeeping for every operation callback.
func (m *metricsObserver) record(op string, dur time.Duration, err error) {
	m.opCalls[op]++
	m.opLatency[op] += dur
	if err != nil {
		m.opErrors[op]++
	}
}

func (m *metricsObserver) OnAcquire(_ context.Context, e worklease.AcquireEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("acquire", e.Duration, e.Err)
	// Start a hold-duration timer keyed on the fencing token of this holding.
	if e.Err == nil {
		m.heldSince[e.Token.FencingToken()] = time.Now()
	}
}

func (m *metricsObserver) OnCheckpoint(_ context.Context, e worklease.CheckpointEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("checkpoint", e.Duration, e.Err)
}

func (m *metricsObserver) OnRenew(_ context.Context, e worklease.RenewEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("renew", e.Duration, e.Err)
	if e.Attempt > 1 {
		m.renewRetries++ // Attempt > 1 means the renewal goroutine retried after a transient error
	}
}

func (m *metricsObserver) OnRelease(_ context.Context, e worklease.ReleaseEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("release", e.Duration, e.Err)
	// Close the hold-duration timer for this holding. A fenced holder may never
	// reach a clean Release, so an entry without a matching close is expected and
	// simply left open (a real backend would expose it as an in-flight gauge).
	ft := e.Token.FencingToken()
	if start, ok := m.heldSince[ft]; ok {
		m.holdDurations = append(m.holdDurations, time.Since(start))
		delete(m.heldSince, ft)
	}
}

func (m *metricsObserver) OnReadCheckpoint(_ context.Context, e worklease.ReadCheckpointEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("read_checkpoint", e.Duration, e.Err)
}

// OnFenced fires in addition to the operation-specific callback when an operation is
// fenced. Incrementing a single counter here gives an accurate fencing rate without
// inspecting error values in OnCheckpoint/OnRenew/OnRelease. e.Operation identifies
// which operation triggered it.
func (m *metricsObserver) OnFenced(_ context.Context, e worklease.FencedEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fencedTotal++
}

func (m *metricsObserver) report() {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Println("=== lease metrics ===")
	for _, op := range []string{"acquire", "checkpoint", "renew", "release", "read_checkpoint"} {
		n := m.opCalls[op]
		if n == 0 {
			continue
		}
		fmt.Printf("%-16s calls=%d errors=%d avg_latency=%s\n",
			op, n, m.opErrors[op], m.opLatency[op]/time.Duration(n))
	}
	fmt.Printf("%-16s total=%d\n", "fenced", m.fencedTotal)
	fmt.Printf("%-16s total=%d\n", "renew_retries", m.renewRetries)
	for i, d := range m.holdDurations {
		fmt.Printf("%-16s holding[%d]=%s\n", "hold_duration", i, d)
	}
}

func main() {
	ctx := context.Background()
	obs := newMetricsObserver()

	// A fake clock lets the example expire a lease deterministically (no sleeping),
	// so a successor can steal it and fence the original holder.
	clk := &fakeClock{now: time.Now()}
	b := memory.New(memory.WithClock(clk))

	// Two holders share one backend and one observer.
	leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "holder-A", Observer: obs})
	if err != nil {
		panic(err)
	}
	leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "holder-B", Observer: obs})
	if err != nil {
		panic(err)
	}

	const workID = "report-2026-06"

	// ===== Clean lifecycle on holder A =====
	tokenA, err := leaseA.Acquire(ctx, workID)
	if err != nil {
		panic(err)
	}
	_ = leaseA.Checkpoint(ctx, tokenA, []byte("page=1"))
	_ = leaseA.Checkpoint(ctx, tokenA, []byte("page=2"))
	_ = leaseA.Renew(ctx, tokenA)
	_, _, _ = leaseA.ReadCheckpoint(ctx, tokenA)

	// ===== Fencing scenario =====
	// Holder A stops renewing and its lease expires; holder B acquires the same work
	// ID (bumping the fencing token), then holder A's next write is fenced.
	clk.advance(31 * time.Second)
	tokenB, err := leaseB.Acquire(ctx, workID)
	if err != nil {
		panic(err)
	}
	if err := leaseA.Checkpoint(ctx, tokenA, []byte("page=3")); err != nil {
		fmt.Printf("holder-A checkpoint correctly fenced: %v\n", err)
	}

	// Holder B finishes cleanly — completes its hold-duration correlation.
	_ = leaseB.Release(ctx, tokenB)

	obs.report()
}

// fakeClock is a stdlib-only deterministic clock for the example.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
