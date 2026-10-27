package worklease_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/memory"
	"github.com/aetomala/worklease/internal/testutil"
)

type spyObserver struct {
	acquireCalls        []worklease.AcquireEvent
	checkpointCalls     []worklease.CheckpointEvent
	renewCalls          []worklease.RenewEvent
	releaseCalls        []worklease.ReleaseEvent
	readCheckpointCalls []worklease.ReadCheckpointEvent
	forgetCalls         []worklease.ForgetEvent
	fencedCalls         []worklease.FencedEvent
	order               []string
	mu                  sync.Mutex
}

func (s *spyObserver) OnAcquire(_ context.Context, e worklease.AcquireEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquireCalls = append(s.acquireCalls, e)
	s.order = append(s.order, "OnAcquire")
}

func (s *spyObserver) OnCheckpoint(_ context.Context, e worklease.CheckpointEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpointCalls = append(s.checkpointCalls, e)
	s.order = append(s.order, "OnCheckpoint")
}

func (s *spyObserver) OnRenew(_ context.Context, e worklease.RenewEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewCalls = append(s.renewCalls, e)
	s.order = append(s.order, "OnRenew")
}

func (s *spyObserver) OnRelease(_ context.Context, e worklease.ReleaseEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseCalls = append(s.releaseCalls, e)
	s.order = append(s.order, "OnRelease")
}

func (s *spyObserver) OnReadCheckpoint(_ context.Context, e worklease.ReadCheckpointEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readCheckpointCalls = append(s.readCheckpointCalls, e)
	s.order = append(s.order, "OnReadCheckpoint")
}

func (s *spyObserver) OnForget(_ context.Context, e worklease.ForgetEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forgetCalls = append(s.forgetCalls, e)
	s.order = append(s.order, "OnForget")
}

func (s *spyObserver) OnFenced(_ context.Context, e worklease.FencedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fencedCalls = append(s.fencedCalls, e)
	s.order = append(s.order, "OnFenced")
}

// sweepSpy is a VacuumObserver that records every OnSweep event.
type sweepSpy struct {
	mu     sync.Mutex
	events []worklease.SweepEvent
}

func (s *sweepSpy) OnSweep(_ context.Context, e worklease.SweepEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *sweepSpy) calls() []worklease.SweepEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]worklease.SweepEvent(nil), s.events...)
}

// stallRenew wraps a Backend so that every Renew call stalls. With ignoreCtx
// false, Renew returns ctx.Err() once its context ends; with ignoreCtx true, it
// ignores its context and blocks until release is closed, then returns nil.
// Each call records ctx.Deadline() and signals entered.
type stallRenew struct {
	backend.Backend
	ignoreCtx bool
	release   chan struct{} // closed once by unblock
	entered   chan struct{} // buffered; receives once per Renew call
	once      sync.Once
	mu        sync.Mutex
	deadlines []time.Time // ctx.Deadline() of each Renew call
}

// unblock closes release once, so a spec and its DeferCleanup can both call it.
func (s *stallRenew) unblock() {
	s.once.Do(func() { close(s.release) })
}

func (s *stallRenew) Renew(ctx context.Context, _ backend.LeaseRecord, _ time.Duration) error {
	d, _ := ctx.Deadline()
	s.mu.Lock()
	s.deadlines = append(s.deadlines, d)
	s.mu.Unlock()
	select {
	case s.entered <- struct{}{}:
	default:
	}
	if s.ignoreCtx {
		<-s.release
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

// recordRenew wraps a Backend and records the local time at entry of every
// Renew call that succeeds.
type recordRenew struct {
	backend.Backend
	mu      sync.Mutex
	entries []time.Time // entry time of each successful Renew
}

func (r *recordRenew) Renew(ctx context.Context, rec backend.LeaseRecord, ttl time.Duration) error {
	entry := time.Now()
	err := r.Backend.Renew(ctx, rec, ttl)
	if err == nil {
		r.mu.Lock()
		r.entries = append(r.entries, entry)
		r.mu.Unlock()
	}
	return err
}

func (r *recordRenew) lastEntry() (time.Time, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) == 0 {
		return time.Time{}, 0
	}
	return r.entries[len(r.entries)-1], len(r.entries)
}

// failRenew wraps a Backend so that every Renew fails with a non-fencing error.
type failRenew struct {
	backend.Backend
}

func (failRenew) Renew(_ context.Context, _ backend.LeaseRecord, _ time.Duration) error {
	return errors.New("storage unavailable")
}

// countRenew wraps a Backend and counts every Renew call.
type countRenew struct {
	backend.Backend
	calls atomic.Int64
}

func (c *countRenew) Renew(ctx context.Context, rec backend.LeaseRecord, ttl time.Duration) error {
	c.calls.Add(1)
	return c.Backend.Renew(ctx, rec, ttl)
}

var _ = Describe("worklease", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		ctrl   *gomock.Controller
		mockB  *testutil.MockBackend
		cfg    worklease.Config
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		ctrl = gomock.NewController(GinkgoT())
		mockB = testutil.NewMockBackend(ctrl)
		cfg = worklease.Config{
			TTL:      30 * time.Second,
			HolderID: "test-worker",
		}
	})

	AfterEach(func() {
		cancel()
		ctrl.Finish()
	})

	// ===== PHASE 1: Constructor and Initialization =====
	Describe("New", func() {
		It("backend nil → ErrNilBackend, no Lease returned", func() {
			lease, err := worklease.New(nil, cfg)
			Expect(err).NotTo(BeNil())
			Expect(errors.Is(err, worklease.ErrNilBackend)).To(BeTrue())
			Expect(lease).To(BeNil())
		})

		It("TTL zero → non-nil error, no Lease returned", func() {
			badCfg := cfg
			badCfg.TTL = 0
			lease, err := worklease.New(mockB, badCfg)
			Expect(err).NotTo(BeNil())
			Expect(lease).To(BeNil())
		})

		It("TTL negative → non-nil error, no Lease returned", func() {
			badCfg := cfg
			badCfg.TTL = -time.Second
			lease, err := worklease.New(mockB, badCfg)
			Expect(err).NotTo(BeNil())
			Expect(lease).To(BeNil())
		})

		It("TTL below 1ms → non-nil error naming the 1ms minimum, no Lease returned", func() {
			badCfg := cfg
			badCfg.TTL = 999 * time.Microsecond
			lease, err := worklease.New(mockB, badCfg)
			Expect(err).To(MatchError(ContainSubstring("at least 1ms")))
			Expect(lease).To(BeNil())
		})

		It("TTL exactly 1ms → non-nil Lease, nil error", func() {
			okCfg := cfg
			okCfg.TTL = time.Millisecond
			lease, err := worklease.New(mockB, okCfg)
			Expect(err).To(BeNil())
			Expect(lease).NotTo(BeNil())
		})

		It("HolderID empty → non-nil error, no Lease returned", func() {
			badCfg := cfg
			badCfg.HolderID = ""
			lease, err := worklease.New(mockB, badCfg)
			Expect(err).NotTo(BeNil())
			Expect(lease).To(BeNil())
		})

		It("all required fields provided → non-nil Lease, nil error", func() {
			lease, err := worklease.New(mockB, cfg)
			Expect(err).To(BeNil())
			Expect(lease).NotTo(BeNil())
		})

		Context("configuration errors match ErrConfigInvalid (#114)", func() {
			It("nil backend → ErrNilBackend, which also matches ErrConfigInvalid", func() {
				_, err := worklease.New(nil, cfg)
				Expect(err).To(BeIdenticalTo(worklease.ErrNilBackend))
				Expect(errors.Is(err, worklease.ErrConfigInvalid)).To(BeTrue())
			})

			It("nil backend and an empty Config → the nil-backend check runs first, so the error is ErrNilBackend (#120)", func() {
				// The empty Config also fails the TTL and HolderID checks, so only the
				// documented order makes the nil-backend error the one returned.
				lease, err := worklease.New(nil, worklease.Config{})
				Expect(err).To(BeIdenticalTo(worklease.ErrNilBackend))
				Expect(lease).To(BeNil())
			})

			It("zero TTL → matches ErrConfigInvalid with message \"worklease: New: TTL is required\"", func() {
				badCfg := cfg
				badCfg.TTL = 0
				_, err := worklease.New(mockB, badCfg)
				Expect(errors.Is(err, worklease.ErrConfigInvalid)).To(BeTrue())
				Expect(err.Error()).To(Equal("worklease: New: TTL is required"))
			})

			It("empty HolderID → matches ErrConfigInvalid with message \"worklease: New: HolderID is required\"", func() {
				badCfg := cfg
				badCfg.HolderID = ""
				_, err := worklease.New(mockB, badCfg)
				Expect(errors.Is(err, worklease.ErrConfigInvalid)).To(BeTrue())
				Expect(err.Error()).To(Equal("worklease: New: HolderID is required"))
			})

			It("negative TTL → matches ErrConfigInvalid with message \"worklease: New: TTL must be positive, got -1s\"", func() {
				badCfg := cfg
				badCfg.TTL = -time.Second
				_, err := worklease.New(mockB, badCfg)
				Expect(errors.Is(err, worklease.ErrConfigInvalid)).To(BeTrue())
				Expect(err.Error()).To(Equal("worklease: New: TTL must be positive, got -1s"))
			})

			It("TTL below 1ms → matches ErrConfigInvalid with message \"worklease: New: TTL must be at least 1ms, got 999µs\"", func() {
				badCfg := cfg
				badCfg.TTL = 999 * time.Microsecond
				_, err := worklease.New(mockB, badCfg)
				Expect(errors.Is(err, worklease.ErrConfigInvalid)).To(BeTrue())
				Expect(err.Error()).To(Equal("worklease: New: TTL must be at least 1ms, got 999µs"))
			})

			It("zero TTL and empty HolderID → the TTL check runs first, so the message is \"worklease: New: TTL is required\"", func() {
				badCfg := cfg
				badCfg.TTL = 0
				badCfg.HolderID = ""
				_, err := worklease.New(mockB, badCfg)
				Expect(errors.Is(err, worklease.ErrConfigInvalid)).To(BeTrue())
				Expect(err.Error()).To(Equal("worklease: New: TTL is required"))
			})

			It("negative TTL and empty HolderID → required fields are checked before values, so the message is \"worklease: New: HolderID is required\"", func() {
				badCfg := cfg
				badCfg.TTL = -time.Second
				badCfg.HolderID = ""
				_, err := worklease.New(mockB, badCfg)
				Expect(errors.Is(err, worklease.ErrConfigInvalid)).To(BeTrue())
				Expect(err.Error()).To(Equal("worklease: New: HolderID is required"))
			})
		})
	})

	// ===== PHASE 3: Core Operations =====
	Describe("Acquire", func() {
		It("lease not held → Token with matching workID, holderID, fencingToken=1, nil error", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, err := lease.Acquire(ctx, "w1")
			Expect(err).To(BeNil())
			Expect(token.WorkID()).To(Equal("w1"))
			Expect(token.HolderID()).To(Equal("test-worker"))
			Expect(token.FencingToken()).To(Equal(uint64(1)))
		})

		It("lease held + no WithWaitForLease → ErrLeaseHeld immediately, backend called exactly once", func() {
			lease, _ := worklease.New(mockB, cfg)
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld).Times(1)
			_, err := lease.Acquire(ctx, "w1")
			Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeTrue())
		})

		It("lease held + WithWaitForLease → retries until available", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			// First two attempts return ErrLeaseHeld, third succeeds
			gomock.InOrder(
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld),
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld),
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).Return(record, nil),
			)
			token, err := lease.Acquire(ctx, "w1", worklease.WithWaitForLease(), worklease.WithPollInterval(10*time.Millisecond))
			Expect(err).To(BeNil())
			Expect(token.WorkID()).To(Equal("w1"))
		})

		It("lease held + WithWaitForLease → wraps context.DeadlineExceeded on context deadline", func() {
			lease, _ := worklease.New(mockB, cfg)
			shortCtx, shortCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer shortCancel()
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld).AnyTimes()
			_, err := lease.Acquire(shortCtx, "w1", worklease.WithWaitForLease(), worklease.WithPollInterval(10*time.Millisecond))
			Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue())
			Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeFalse())
		})

		It("uses default poll interval 2s when WithPollInterval not set", func() {
			lease, _ := worklease.New(mockB, cfg)
			// Record when each poll reaches the backend. Calls run on the caller's
			// goroutine, so no lock is needed.
			var calls []time.Time
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).DoAndReturn(
				func(context.Context, string, string, time.Duration) (backend.LeaseRecord, error) {
					calls = append(calls, time.Now())
					return backend.LeaseRecord{}, worklease.ErrLeaseHeld
				}).AnyTimes()
			// At the default 2s interval, exactly two polls fit in 3s: at t=0 and t=2s.
			longCtx, longCancel := context.WithTimeout(ctx, 3*time.Second)
			defer longCancel()
			_, err := lease.Acquire(longCtx, "w1", worklease.WithWaitForLease())
			Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue())
			Expect(calls).To(HaveLen(2))
			gap := calls[1].Sub(calls[0])
			Expect(gap).To(BeNumerically(">=", 1900*time.Millisecond))
			Expect(gap).To(BeNumerically("<", 2300*time.Millisecond))
		})

		It("uses configured poll interval when set via WithPollInterval", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			gomock.InOrder(
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld),
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).Return(record, nil),
			)
			token, err := lease.Acquire(ctx, "w1", worklease.WithWaitForLease(), worklease.WithPollInterval(50*time.Millisecond))
			Expect(err).To(BeNil())
			Expect(token.WorkID()).To(Equal("w1"))
		})

		It("empty workID → non-nil error, backend not called", func() {
			lease, _ := worklease.New(mockB, cfg)
			mockB.EXPECT().Acquire(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			_, err := lease.Acquire(ctx, "")
			Expect(err).NotTo(BeNil())
		})

		It("lease held + WithWaitForLease → context cancelled while waiting wraps context.Canceled, not ErrLeaseHeld", func() {
			lease, _ := worklease.New(mockB, cfg)
			cancelCtx, cancelFn := context.WithCancel(ctx)
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld).AnyTimes()
			// Cancel shortly after the first poll begins.
			go func() {
				time.Sleep(20 * time.Millisecond)
				cancelFn()
			}()
			_, err := lease.Acquire(cancelCtx, "w1", worklease.WithWaitForLease(), worklease.WithPollInterval(10*time.Millisecond))
			Expect(errors.Is(err, context.Canceled)).To(BeTrue())
			Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeFalse())
		})
	})

	Describe("Checkpoint", func() {
		It("fencing token current → nil", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("state"), 30*time.Second).Return(nil)
			err := lease.Checkpoint(ctx, token, []byte("state"))
			Expect(err).To(BeNil())
		})

		It("fencing token stale → ErrFenced; error wraps workID and holderID", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("state"), 30*time.Second).Return(worklease.ErrFenced)
			err := lease.Checkpoint(ctx, token, []byte("state"))
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("w1"))
			Expect(err.Error()).To(ContainSubstring("test-worker"))
		})

		Context("when the backend returns ErrLeaseExpired (exit already declared)", func() {
			var (
				spy   *spyObserver
				lease worklease.Lease
				token worklease.Token
			)

			BeforeEach(func() {
				spy = &spyObserver{}
				spyCfg := cfg
				spyCfg.Observer = spy
				lease, _ = worklease.New(mockB, spyCfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				token, _ = lease.Acquire(ctx, "w1")
				mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("late"), 30*time.Second).Return(worklease.ErrLeaseExpired)
			})

			It("calls OnCheckpoint with Err matching ErrLeaseExpired", func() {
				_ = lease.Checkpoint(ctx, token, []byte("late"))
				spy.mu.Lock()
				defer spy.mu.Unlock()
				Expect(spy.checkpointCalls).To(HaveLen(1))
				Expect(errors.Is(spy.checkpointCalls[0].Err, worklease.ErrLeaseExpired)).To(BeTrue())
			})

			It("does not call OnFenced", func() {
				_ = lease.Checkpoint(ctx, token, []byte("late"))
				spy.mu.Lock()
				defer spy.mu.Unlock()
				Expect(spy.fencedCalls).To(BeEmpty())
			})

			It("returns an error matching ErrLeaseExpired with the worklease: Checkpoint: prefix", func() {
				err := lease.Checkpoint(ctx, token, []byte("late"))
				Expect(errors.Is(err, worklease.ErrLeaseExpired)).To(BeTrue())
				Expect(err.Error()).To(HavePrefix("worklease: Checkpoint: "))
			})
		})
	})

	Describe("Renew", func() {
		It("fencing token current → nil", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil)
			err := lease.Renew(ctx, token)
			Expect(err).To(BeNil())
		})

		It("fencing token stale → ErrFenced; error wraps workID and holderID", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(worklease.ErrFenced)
			err := lease.Renew(ctx, token)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("w1"))
			Expect(err.Error()).To(ContainSubstring("test-worker"))
		})
	})

	Describe("ExitMode", func() {
		It("String returns none, finished, abandoned, retired, expired for the five modes", func() {
			Expect(worklease.ExitNone.String()).To(Equal("none"))
			Expect(worklease.ExitFinished.String()).To(Equal("finished"))
			Expect(worklease.ExitAbandoned.String()).To(Equal("abandoned"))
			Expect(worklease.ExitRetired.String()).To(Equal("retired"))
			Expect(worklease.ExitExpired.String()).To(Equal("expired"))
		})

		It("String formats an undefined value as ExitMode(<n>)", func() {
			Expect(worklease.ExitMode(42).String()).To(Equal("ExitMode(42)"))
		})

		It("worklease constants equal the backend constants", func() {
			Expect(worklease.ExitNone).To(Equal(backend.ExitNone))
			Expect(worklease.ExitFinished).To(Equal(backend.ExitFinished))
			Expect(worklease.ExitAbandoned).To(Equal(backend.ExitAbandoned))
			Expect(worklease.ExitRetired).To(Equal(backend.ExitRetired))
			Expect(worklease.ExitExpired).To(Equal(backend.ExitExpired))
		})
	})

	Describe("Release", func() {
		It("fencing token current → nil", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).Return(nil)
			err := lease.Release(ctx, token, worklease.ExitFinished)
			Expect(err).To(BeNil())
		})

		It("fencing token stale → ErrFenced; error wraps workID and holderID", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).Return(worklease.ErrFenced)
			err := lease.Release(ctx, token, worklease.ExitFinished)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("w1"))
			Expect(err.Error()).To(ContainSubstring("test-worker"))
		})

		Context("when mode is ExitNone, ExitExpired, or an undefined value", func() {
			It("returns an error matching ErrInvalidExitMode, does not call Backend.Release, and calls no observer method", func() {
				spy := &spyObserver{}
				spyCfg := cfg
				spyCfg.Observer = spy
				lease, _ := worklease.New(mockB, spyCfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				token, _ := lease.Acquire(ctx, "w1")
				mockB.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				spy.mu.Lock()
				before := len(spy.order)
				spy.mu.Unlock()
				for _, mode := range []worklease.ExitMode{worklease.ExitNone, worklease.ExitExpired, worklease.ExitMode(99)} {
					err := lease.Release(ctx, token, mode)
					Expect(errors.Is(err, worklease.ErrInvalidExitMode)).To(BeTrue())
				}
				spy.mu.Lock()
				after := len(spy.order)
				spy.mu.Unlock()
				Expect(after).To(Equal(before))
			})
		})

		Context("when mode is ExitFinished, ExitAbandoned, or ExitRetired", func() {
			It("passes the mode to Backend.Release and reports it in ReleaseEvent.Mode", func() {
				spy := &spyObserver{}
				spyCfg := cfg
				spyCfg.Observer = spy
				lease, _ := worklease.New(mockB, spyCfg)
				for i, mode := range []worklease.ExitMode{worklease.ExitFinished, worklease.ExitAbandoned, worklease.ExitRetired} {
					record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: uint64(i + 1), ExpiresAt: time.Now().Add(30 * time.Second)}
					mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
					token, _ := lease.Acquire(ctx, "w1")
					mockB.EXPECT().Release(gomock.Any(), record, mode).Return(nil)
					Expect(lease.Release(ctx, token, mode)).To(Succeed())
				}
				spy.mu.Lock()
				calls := append([]worklease.ReleaseEvent(nil), spy.releaseCalls...)
				spy.mu.Unlock()
				Expect(calls).To(HaveLen(3))
				Expect(calls[0].Mode).To(Equal(worklease.ExitFinished))
				Expect(calls[1].Mode).To(Equal(worklease.ExitAbandoned))
				Expect(calls[2].Mode).To(Equal(worklease.ExitRetired))
			})
		})

		Context("when the backend returns ErrLeaseExpired", func() {
			It("calls OnRelease with the error, does not call OnFenced, and wraps with the worklease: Release: prefix", func() {
				spy := &spyObserver{}
				spyCfg := cfg
				spyCfg.Observer = spy
				lease, _ := worklease.New(mockB, spyCfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				token, _ := lease.Acquire(ctx, "w1")
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).Return(worklease.ErrLeaseExpired)

				err := lease.Release(ctx, token, worklease.ExitFinished)
				Expect(errors.Is(err, worklease.ErrLeaseExpired)).To(BeTrue())
				Expect(err.Error()).To(HavePrefix("worklease: Release: "))

				spy.mu.Lock()
				defer spy.mu.Unlock()
				Expect(spy.releaseCalls).To(HaveLen(1))
				Expect(errors.Is(spy.releaseCalls[0].Err, worklease.ErrLeaseExpired)).To(BeTrue())
				Expect(spy.fencedCalls).To(BeEmpty())
			})
		})
	})

	Describe("ReadCheckpoint", func() {
		It("returns ExitNone and nil State for a first acquisition", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(backend.Checkpoint{}, nil)
			cp, err := lease.ReadCheckpoint(ctx, token)
			Expect(err).To(BeNil())
			Expect(cp.State).To(BeNil())
			Expect(cp.PrevExit).To(Equal(worklease.ExitNone))
		})

		It("returns the backend Checkpoint unchanged on success", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			checkpointData := []byte("checkpoint-data")
			want := backend.Checkpoint{State: checkpointData, PrevExit: backend.ExitFinished, PrevHolderID: "prev-holder"}
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(want, nil)
			cp, err := lease.ReadCheckpoint(ctx, token)
			Expect(err).To(BeNil())
			Expect(cp).To(Equal(want))
		})

		It("returns ExitExpired and the last checkpoint bytes after a predecessor expired", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			checkpointData := []byte("checkpoint-data")
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(backend.Checkpoint{State: checkpointData, PrevExit: backend.ExitExpired, PrevHolderID: "crashed"}, nil)
			cp, err := lease.ReadCheckpoint(ctx, token)
			Expect(err).To(BeNil())
			Expect(cp.State).To(Equal(checkpointData))
			Expect(cp.PrevExit).To(Equal(worklease.ExitExpired))
			Expect(cp.PrevHolderID).To(Equal("crashed"))
		})

		It("fencing token stale → ErrFenced; error wraps workID and holderID", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(backend.Checkpoint{State: []byte("x")}, worklease.ErrFenced)
			cp, err := lease.ReadCheckpoint(ctx, token)
			Expect(cp).To(Equal(worklease.Checkpoint{}))
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring(`worklease: ReadCheckpoint: workID="w1" holderID="test-worker"`))
		})

		It("non-fencing backend error → wrapped with the worklease: ReadCheckpoint: prefix", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			backendErr := errors.New("connection reset")
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(backend.Checkpoint{}, backendErr)
			cp, err := lease.ReadCheckpoint(ctx, token)
			Expect(cp).To(Equal(worklease.Checkpoint{}))
			Expect(errors.Is(err, backendErr)).To(BeTrue())
			Expect(err.Error()).To(HavePrefix("worklease: ReadCheckpoint: "))
		})
	})

	Describe("leaseClient.Forget", func() {
		var spy *spyObserver

		BeforeEach(func() {
			spy = &spyObserver{}
			cfg.Observer = spy
		})

		Context("when the backend delete succeeds", func() {
			It("returns nil and calls OnForget once with the token, a Duration, and nil Err", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				token, _ := lease.Acquire(ctx, "w1")

				spy.mu.Lock()
				ordersBefore := len(spy.order)
				spy.mu.Unlock()

				mockB.EXPECT().Forget(gomock.Any(), record).DoAndReturn(
					func(context.Context, backend.LeaseRecord) error {
						time.Sleep(time.Millisecond)
						return nil
					})
				err := lease.Forget(ctx, token)
				Expect(err).To(BeNil())

				spy.mu.Lock()
				defer spy.mu.Unlock()
				Expect(spy.order[ordersBefore:]).To(Equal([]string{"OnForget"}))
				Expect(spy.forgetCalls).To(HaveLen(1))
				Expect(spy.forgetCalls[0].Token).To(Equal(token))
				Expect(spy.forgetCalls[0].Duration).To(BeNumerically(">", 0))
				Expect(spy.forgetCalls[0].Err).To(BeNil())
			})
		})

		Context("when the backend returns ErrFenced", func() {
			It("wraps ErrFenced with workID and holderID context and calls OnForget then OnFenced with OperationForget", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				token, _ := lease.Acquire(ctx, "w1")

				spy.mu.Lock()
				ordersBefore := len(spy.order)
				spy.mu.Unlock()

				mockB.EXPECT().Forget(gomock.Any(), record).Return(worklease.ErrFenced)
				err := lease.Forget(ctx, token)
				Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				Expect(err.Error()).To(ContainSubstring("workID"))
				Expect(err.Error()).To(ContainSubstring("holderID"))

				spy.mu.Lock()
				defer spy.mu.Unlock()
				Expect(spy.order[ordersBefore:]).To(Equal([]string{"OnForget", "OnFenced"}))
				Expect(spy.forgetCalls).To(HaveLen(1))
				Expect(errors.Is(spy.forgetCalls[0].Err, worklease.ErrFenced)).To(BeTrue())
				Expect(spy.fencedCalls).To(HaveLen(1))
				Expect(spy.fencedCalls[0].Operation).To(Equal(worklease.OperationForget))
				Expect(spy.fencedCalls[0].Token).To(Equal(token))
			})
		})

		Context("when the backend returns a non-fenced, non-nil error", func() {
			It("wraps the error with the worklease: Forget: prefix", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				token, _ := lease.Acquire(ctx, "w1")

				mockB.EXPECT().Forget(gomock.Any(), record).Return(errors.New("connection lost"))
				err := lease.Forget(ctx, token)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("worklease: Forget:"))
			})

			It("calls OnForget with the error and does not call OnFenced", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				token, _ := lease.Acquire(ctx, "w1")

				backendErr := errors.New("connection lost")
				mockB.EXPECT().Forget(gomock.Any(), record).Return(backendErr)
				err := lease.Forget(ctx, token)
				Expect(errors.Is(err, backendErr)).To(BeTrue())

				spy.mu.Lock()
				defer spy.mu.Unlock()
				Expect(spy.forgetCalls).To(HaveLen(1))
				Expect(spy.forgetCalls[0].Err).To(Equal(backendErr))
				Expect(spy.fencedCalls).To(BeEmpty())
			})
		})
	})

	// ===== PHASE 5: Concurrency / Goroutine lifecycle =====
	Describe("StartRenewal", func() {
		It("normal stop → non-nil renewCtx, non-nil stopRenewal; renewCtx NOT cancelled on stopRenewal", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(500*time.Millisecond))
			defer stopRenewal()

			Expect(renewCtx).NotTo(BeNil())
			Expect(stopRenewal).NotTo(BeNil())

			// Call stop and verify renewCtx is not cancelled
			stopRenewal()
			Expect(renewCtx.Done()).NotTo(BeClosed())
		})

		It("stopRenewal is idempotent (can be called twice without panic)", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(500*time.Millisecond))

			Expect(func() {
				stopRenewal()
				stopRenewal()
			}).NotTo(Panic())

			Expect(renewCtx.Done()).NotTo(BeClosed())
		})

		It("fencing path → renewCtx cancelled when Renew returns ErrFenced", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(worklease.ErrFenced)

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(50*time.Millisecond))
			defer stopRenewal()

			Eventually(renewCtx.Done()).Should(BeClosed())
			Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrFenced))
		})

		It("non-fencing error → retries with backoff, then cancels renewCtx with cause ErrLeaseWindowExhausted once the lease window closes", func() {
			// Short TTL so the bounded-retry loop exhausts the window quickly.
			shortCfg := cfg
			shortCfg.TTL = 150 * time.Millisecond
			lease, _ := worklease.New(mockB, shortCfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(150 * time.Millisecond),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 150*time.Millisecond).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			// Every renewal attempt fails with a non-fencing error, so the goroutine
			// retries until the lease window is exhausted (v0.5 bounded-retry contract).
			mockB.EXPECT().Renew(gomock.Any(), record, 150*time.Millisecond).Return(errors.New("connection lost")).AnyTimes()

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token,
				worklease.WithRenewalInterval(10*time.Millisecond),
				worklease.WithRenewalBackoff(1*time.Millisecond, 5*time.Millisecond, 0))
			defer stopRenewal()

			Eventually(renewCtx.Done()).Should(BeClosed())
			Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))
		})

		It("ErrLeaseExpired from Renew → cancels renewCtx at once with a cause matching ErrLeaseWindowExhausted and ErrLeaseExpired, without retrying", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			// Times(1): a retry would be a second call. The one-hour backoff and the
			// open 30s window mean only the terminal path can close renewCtx in time.
			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(worklease.ErrLeaseExpired).Times(1)

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token,
				worklease.WithRenewalInterval(10*time.Millisecond),
				worklease.WithRenewalBackoff(time.Hour, time.Hour, 0))
			defer stopRenewal()

			Eventually(renewCtx.Done(), time.Second).Should(BeClosed())
			cause := context.Cause(renewCtx)
			Expect(errors.Is(cause, worklease.ErrLeaseWindowExhausted)).To(BeTrue())
			Expect(errors.Is(cause, worklease.ErrLeaseExpired)).To(BeTrue())
		})

		It("successful renewal → retry window extends past the token's original ExpiresAt", func() {
			shortCfg := cfg
			shortCfg.TTL = 200 * time.Millisecond
			lease, _ := worklease.New(mockB, shortCfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(200 * time.Millisecond),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 200*time.Millisecond).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			// Renewals succeed, except for one transient error injected after the
			// token's original ExpiresAt — when the lease is still valid in storage.
			var injected atomic.Bool
			mockB.EXPECT().Renew(gomock.Any(), record, 200*time.Millisecond).DoAndReturn(
				func(context.Context, backend.LeaseRecord, time.Duration) error {
					if time.Now().After(record.ExpiresAt) && injected.CompareAndSwap(false, true) {
						return errors.New("connection reset")
					}
					return nil
				}).AnyTimes()

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token,
				worklease.WithRenewalInterval(50*time.Millisecond),
				worklease.WithRenewalBackoff(5*time.Millisecond, 10*time.Millisecond, 0))
			defer stopRenewal()

			Eventually(injected.Load).Should(BeTrue())
			Consistently(func() error { return context.Cause(renewCtx) }, 150*time.Millisecond).Should(BeNil())
		})

		It("backend clock ahead of local clock → retry window bounded by local acquire time plus TTL", func() {
			shortCfg := cfg
			shortCfg.TTL = 150 * time.Millisecond
			lease, _ := worklease.New(mockB, shortCfg)
			// ExpiresAt reflects a backend clock running 10s ahead of the local clock.
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(10*time.Second + 150*time.Millisecond),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 150*time.Millisecond).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().Renew(gomock.Any(), record, 150*time.Millisecond).Return(errors.New("connection lost")).AnyTimes()

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token,
				worklease.WithRenewalInterval(10*time.Millisecond),
				worklease.WithRenewalBackoff(1*time.Millisecond, 5*time.Millisecond, 0))
			defer stopRenewal()

			Eventually(renewCtx.Done(), time.Second).Should(BeClosed())
			Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))
		})

		It("normal stop → context.Cause(renewCtx) is nil (goroutine never cancels on stop)", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(500*time.Millisecond))
			stopRenewal()

			Expect(renewCtx.Done()).NotTo(BeClosed())
			Expect(context.Cause(renewCtx)).To(BeNil())
		})

		It("parent context cancelled → renewCtx cancelled with the parent's cause, not set by the goroutine", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil).AnyTimes()

			shortCtx, shortCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer shortCancel()

			renewCtx, stopRenewal := lease.StartRenewal(shortCtx, token, worklease.WithRenewalInterval(50*time.Millisecond))
			defer stopRenewal()

			Eventually(renewCtx.Done()).Should(BeClosed())
			// Path 4: the goroutine sets no cause; renewCtx reflects the parent's cause.
			Expect(context.Cause(renewCtx)).To(MatchError(context.DeadlineExceeded))
		})

		Context("when a Renew call stalls (#88)", func() {
			var (
				spy   *spyObserver
				stall *stallRenew
				lease worklease.Lease
				token worklease.Token
			)

			// setup builds a lease over memory.New() wrapped in stallRenew, with a
			// 200ms TTL and spyObserver, and acquires "w1".
			setup := func(ignoreCtx bool) {
				spy = &spyObserver{}
				stall = &stallRenew{
					Backend:   memory.New(),
					ignoreCtx: ignoreCtx,
					release:   make(chan struct{}),
					entered:   make(chan struct{}, 1),
				}
				DeferCleanup(stall.unblock)
				var err error
				lease, err = worklease.New(stall, worklease.Config{TTL: 200 * time.Millisecond, HolderID: "test-worker", Observer: spy})
				Expect(err).NotTo(HaveOccurred())
				token, err = lease.Acquire(ctx, "w1")
				Expect(err).NotTo(HaveOccurred())
			}

			renewEvents := func() []worklease.RenewEvent {
				spy.mu.Lock()
				defer spy.mu.Unlock()
				return append([]worklease.RenewEvent(nil), spy.renewCalls...)
			}

			interval := worklease.WithRenewalInterval(20 * time.Millisecond)

			It("stopRenewal returns within 100ms while Renew blocks until its context ends", func() {
				setup(false)
				_, stopRenewal := lease.StartRenewal(ctx, token, interval)
				Eventually(stall.entered).Should(Receive())

				begin := time.Now()
				stopRenewal()
				Expect(time.Since(begin)).To(BeNumerically("<", 100*time.Millisecond))
			})

			It("stopRenewal returns within 100ms while Renew ignores its context", func() {
				setup(true)
				_, stopRenewal := lease.StartRenewal(ctx, token, interval)
				Eventually(stall.entered).Should(Receive())

				begin := time.Now()
				stopRenewal()
				Expect(time.Since(begin)).To(BeNumerically("<", 100*time.Millisecond))
			})

			It("does not cancel renewCtx when stopRenewal abandons an in-flight Renew", func() {
				setup(true)
				renewCtx, stopRenewal := lease.StartRenewal(ctx, token, interval)
				Eventually(stall.entered).Should(Receive())

				stopRenewal()
				Expect(renewCtx.Done()).NotTo(BeClosed())
				Expect(context.Cause(renewCtx)).To(BeNil())
			})

			It("cancels renewCtx with cause ErrLeaseWindowExhausted when the window closes during a Renew that blocks until its context ends", func() {
				setup(false)
				renewCtx, stopRenewal := lease.StartRenewal(ctx, token, interval)
				defer stopRenewal()

				Eventually(renewCtx.Done(), time.Second).Should(BeClosed())
				Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))
			})

			It("cancels renewCtx with cause ErrLeaseWindowExhausted when the window closes during a Renew that ignores its context", func() {
				setup(true)
				renewCtx, stopRenewal := lease.StartRenewal(ctx, token, interval)
				defer stopRenewal()

				Eventually(renewCtx.Done(), time.Second).Should(BeClosed())
				Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))
			})

			It("passes Renew a context whose deadline is the lease window", func() {
				setup(false)
				renewCtx, stopRenewal := lease.StartRenewal(ctx, token, interval)
				defer stopRenewal()
				window, ok := worklease.LeaseWindow(renewCtx)
				Expect(ok).To(BeTrue())

				Eventually(stall.entered).Should(Receive())
				stall.mu.Lock()
				defer stall.mu.Unlock()
				Expect(stall.deadlines).NotTo(BeEmpty())
				Expect(stall.deadlines[0]).To(BeTemporally("==", window))
			})

			It("reports an attempt abandoned by stopRenewal to OnRenew once, with context.Canceled", func() {
				setup(true)
				_, stopRenewal := lease.StartRenewal(ctx, token, interval)
				Eventually(stall.entered).Should(Receive())

				stopRenewal()
				events := renewEvents()
				Expect(events).To(HaveLen(1))
				Expect(events[0].Err).To(MatchError(context.Canceled))
				Expect(events[0].Duration).To(BeNumerically(">", 0))
				Consistently(renewEvents, 60*time.Millisecond).Should(HaveLen(1))
			})

			It("reports an attempt cut off by the window to OnRenew once, with context.DeadlineExceeded", func() {
				setup(true)
				renewCtx, stopRenewal := lease.StartRenewal(ctx, token, interval)
				Eventually(renewCtx.Done(), time.Second).Should(BeClosed())

				stopRenewal()
				events := renewEvents()
				Expect(events).To(HaveLen(1))
				Expect(events[0].Err).To(MatchError(context.DeadlineExceeded))
				spy.mu.Lock()
				defer spy.mu.Unlock()
				Expect(spy.fencedCalls).To(BeEmpty())
			})

			It("reports an attempt abandoned by cancellation of the parent context to OnRenew once, with the parent's error, and sets no cause", func() {
				// Renew ignores its context, so only the parent-cancellation case of
				// the attempt wait can end it before the window closes.
				setup(true)
				errParentStopped := errors.New("parent stopped")
				parentCtx, parentCancel := context.WithCancelCause(ctx)
				defer parentCancel(nil)
				renewCtx, stopRenewal := lease.StartRenewal(parentCtx, token, interval)
				defer stopRenewal()
				Eventually(stall.entered).Should(Receive())

				parentCancel(errParentStopped)
				// Observed before stopRenewal, which would report the attempt itself.
				Eventually(renewEvents, 200*time.Millisecond).Should(HaveLen(1))
				events := renewEvents()
				Expect(events[0].Err).To(MatchError(context.Canceled))

				stopRenewal()
				Expect(context.Cause(renewCtx)).To(MatchError(errParentStopped))
			})

			It("lets the goroutine of an abandoned Renew call exit once the call returns", func() {
				setup(true)
				before := runtime.NumGoroutine()
				_, stopRenewal := lease.StartRenewal(ctx, token, interval)
				Eventually(stall.entered).Should(Receive())

				stopRenewal()
				stall.unblock()
				Eventually(runtime.NumGoroutine, time.Second).Should(BeNumerically("<=", before))
			})
		})

		Context("when the window closes during a backoff wait (#88)", func() {
			It("cancels renewCtx with cause ErrLeaseWindowExhausted at the window, without waiting out the backoff", func() {
				lease, err := worklease.New(failRenew{Backend: memory.New()}, worklease.Config{TTL: 300 * time.Millisecond, HolderID: "test-worker"})
				Expect(err).NotTo(HaveOccurred())
				acquired := time.Now()
				token, err := lease.Acquire(ctx, "w1")
				Expect(err).NotTo(HaveOccurred())

				renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalBackoff(5*time.Second, 5*time.Second, 0))
				defer stopRenewal()

				Eventually(renewCtx.Done(), time.Second).Should(BeClosed())
				Expect(time.Since(acquired)).To(BeNumerically("<", 450*time.Millisecond))
				cause := context.Cause(renewCtx)
				Expect(cause).To(MatchError(worklease.ErrLeaseWindowExhausted))
				Expect(errors.Is(cause, worklease.ErrLeaseExpired)).To(BeFalse())
			})
		})

		It("with WithWaitForLease, the first window starts at the attempt that succeeded", func() {
			shortCfg := cfg
			shortCfg.TTL = 300 * time.Millisecond
			lease, _ := worklease.New(mockB, shortCfg)
			record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(time.Hour)}
			// The first attempt finds the lease held; the second, one poll later, succeeds.
			var firstCall time.Time
			gomock.InOrder(
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 300*time.Millisecond).DoAndReturn(
					func(context.Context, string, string, time.Duration) (backend.LeaseRecord, error) {
						firstCall = time.Now()
						return backend.LeaseRecord{}, worklease.ErrLeaseHeld
					}),
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 300*time.Millisecond).Return(record, nil),
			)
			mockB.EXPECT().Renew(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			token, err := lease.Acquire(ctx, "w1", worklease.WithWaitForLease(), worklease.WithPollInterval(400*time.Millisecond))
			Expect(err).NotTo(HaveOccurred())

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(time.Second))
			defer stopRenewal()

			// The window starts at the successful attempt, at least one poll after the
			// first call, so it is still open although the first call is over a TTL ago.
			time.Sleep(50 * time.Millisecond)
			Expect(renewCtx.Err()).To(BeNil())
			w, ok := worklease.LeaseWindow(renewCtx)
			Expect(ok).To(BeTrue())
			Expect(w).To(BeTemporally(">=", firstCall.Add(400*time.Millisecond+300*time.Millisecond)))
		})

		Context("when the window closes between renewal ticks (#88)", func() {
			It("cancels renewCtx with cause ErrLeaseWindowExhausted before the next tick, without calling Renew", func() {
				shortCfg := cfg
				shortCfg.TTL = 100 * time.Millisecond
				lease, _ := worklease.New(mockB, shortCfg)
				// ExpiresAt is an hour away, so only the local window can close renewCtx.
				record := backend.LeaseRecord{
					WorkID:       "w1",
					HolderID:     "test-worker",
					FencingToken: 1,
					ExpiresAt:    time.Now().Add(time.Hour),
				}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 100*time.Millisecond).Return(record, nil)
				token, _ := lease.Acquire(ctx, "w1")
				mockB.EXPECT().Renew(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(time.Second))
				defer stopRenewal()

				Eventually(renewCtx.Done(), 500*time.Millisecond).Should(BeClosed())
				Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))
			})

			It("cancels renewCtx at once when StartRenewal is called after the window has closed", func() {
				shortCfg := cfg
				shortCfg.TTL = 50 * time.Millisecond
				lease, _ := worklease.New(mockB, shortCfg)
				// ExpiresAt is an hour away; the window closes because the TTL elapses
				// locally before StartRenewal is called.
				record := backend.LeaseRecord{
					WorkID:       "w1",
					HolderID:     "test-worker",
					FencingToken: 1,
					ExpiresAt:    time.Now().Add(time.Hour),
				}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 50*time.Millisecond).Return(record, nil)
				token, _ := lease.Acquire(ctx, "w1")
				mockB.EXPECT().Renew(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				time.Sleep(60 * time.Millisecond)

				renewCtx, stopRenewal := lease.StartRenewal(ctx, token)
				defer stopRenewal()

				Eventually(renewCtx.Done(), 50*time.Millisecond).Should(BeClosed())
				Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))
			})

			It("cancels renewCtx without calling Renew or emitting OnRenew when the tick fires on a closed window (#122)", func() {
				// With the window closed and a 1ns tick, the tick wait finds the window
				// timer and the ticker both ready, and select picks one at random. Only
				// the ticker branch reaches the closed-window check, so each trial
				// exercises it roughly half the time (40-50% measured): 100 trials miss
				// a missing check with probability below 10^-20. On correct code no
				// outcome calls Renew.
				const trials = 100
				counter := &countRenew{Backend: memory.New()}
				spy := &spyObserver{}
				lease, err := worklease.New(counter, worklease.Config{TTL: time.Millisecond, HolderID: "test-worker", Observer: spy})
				Expect(err).NotTo(HaveOccurred())
				token, err := lease.Acquire(ctx, "w1")
				Expect(err).NotTo(HaveOccurred())
				time.Sleep(5 * time.Millisecond) // the 1ms window is now closed

				for range trials {
					renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(time.Nanosecond))
					Eventually(renewCtx.Done(), time.Second, time.Millisecond).Should(BeClosed())
					stopRenewal()
					Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))
				}

				Expect(counter.calls.Load()).To(BeZero())
				spy.mu.Lock()
				defer spy.mu.Unlock()
				Expect(spy.renewCalls).To(BeEmpty())
			})
		})

		Context("first window ignores the backend's expiry timestamp (#117)", func() {
			// startWith acquires through the mock with a 300ms TTL and the given
			// ExpiresAt, and starts renewal with a tick far beyond the window.
			startWith := func(expiresAt time.Time) (context.Context, func()) {
				shortCfg := cfg
				shortCfg.TTL = 300 * time.Millisecond
				lease, _ := worklease.New(mockB, shortCfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: expiresAt}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 300*time.Millisecond).Return(record, nil)
				mockB.EXPECT().Renew(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				token, err := lease.Acquire(ctx, "w1")
				Expect(err).NotTo(HaveOccurred())
				return lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(time.Second))
			}

			It("a past ExpiresAt does not cancel renewCtx before the local window closes", func() {
				renewCtx, stopRenewal := startWith(time.Now().Add(-time.Hour))
				defer stopRenewal()
				time.Sleep(200 * time.Millisecond)
				Expect(renewCtx.Err()).To(BeNil())
				Eventually(renewCtx.Done(), 300*time.Millisecond).Should(BeClosed())
				Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))
			})

			It("a zero ExpiresAt does not cancel renewCtx before the local window closes", func() {
				renewCtx, stopRenewal := startWith(time.Time{})
				defer stopRenewal()
				time.Sleep(200 * time.Millisecond)
				Expect(renewCtx.Err()).To(BeNil())
				Eventually(renewCtx.Done(), 300*time.Millisecond).Should(BeClosed())
				Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))
			})

			It("a future ExpiresAt does not extend the window past the local start plus TTL", func() {
				renewCtx, stopRenewal := startWith(time.Now().Add(time.Hour))
				defer stopRenewal()
				Eventually(renewCtx.Done(), 500*time.Millisecond).Should(BeClosed())
				Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))
			})
		})
	})

	Describe("LeaseWindow", func() {
		// memLease builds a lease over b with the given TTL.
		memLease := func(b backend.Backend, ttl time.Duration) worklease.Lease {
			lease, err := worklease.New(b, worklease.Config{TTL: ttl, HolderID: "test-worker"})
			Expect(err).NotTo(HaveOccurred())
			return lease
		}

		windowOf := func(c context.Context) time.Time {
			w, _ := worklease.LeaseWindow(c)
			return w
		}

		It("returns ok false for a context not derived from StartRenewal", func() {
			w, ok := worklease.LeaseWindow(ctx)
			Expect(ok).To(BeFalse())
			Expect(w.IsZero()).To(BeTrue())
		})

		It("returns the initial window, between acquire start plus TTL and acquire end plus TTL", func() {
			lease := memLease(memory.New(), 30*time.Second)
			before := time.Now()
			token, err := lease.Acquire(ctx, "w1")
			Expect(err).NotTo(HaveOccurred())
			after := time.Now()

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token)
			defer stopRenewal()
			w, ok := worklease.LeaseWindow(renewCtx)
			Expect(ok).To(BeTrue())

			Expect(w).To(BeTemporally(">=", before.Add(30*time.Second)))
			Expect(w).To(BeTemporally("<=", after.Add(30*time.Second)))
		})

		It("advances after a successful renewal", func() {
			lease := memLease(memory.New(), 200*time.Millisecond)
			token, err := lease.Acquire(ctx, "w1")
			Expect(err).NotTo(HaveOccurred())

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(20*time.Millisecond))
			defer stopRenewal()
			first, ok := worklease.LeaseWindow(renewCtx)
			Expect(ok).To(BeTrue())

			Eventually(func() time.Time { return windowOf(renewCtx) }).Should(BeTemporally(">", first))
		})

		It("never exceeds the expiry the backend recorded at each renewal", func() {
			rec := &recordRenew{Backend: memory.New()}
			lease := memLease(rec, 200*time.Millisecond)
			token, err := lease.Acquire(ctx, "w1")
			Expect(err).NotTo(HaveOccurred())

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(20*time.Millisecond))
			defer stopRenewal()

			// Read the window before the entry time: the renewal that published the
			// window recorded its entry first, so the bound holds for every sample.
			Eventually(func() int { _, n := rec.lastEntry(); return n }).Should(BeNumerically(">=", 3))
			for range 10 {
				w := windowOf(renewCtx)
				last, _ := rec.lastEntry()
				Expect(w).To(BeTemporally("<=", last.Add(200*time.Millisecond)))
				time.Sleep(10 * time.Millisecond)
			}
		})

		It("is readable from a context derived from renewCtx", func() {
			lease := memLease(memory.New(), 30*time.Second)
			token, err := lease.Acquire(ctx, "w1")
			Expect(err).NotTo(HaveOccurred())

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token)
			defer stopRenewal()
			child, cancelChild := context.WithTimeout(renewCtx, time.Minute)
			defer cancelChild()

			want, ok := worklease.LeaseWindow(renewCtx)
			Expect(ok).To(BeTrue())
			got, ok := worklease.LeaseWindow(child)
			Expect(ok).To(BeTrue())
			Expect(got).To(BeTemporally("==", want))
		})

		It("returns ok false after renewCtx is cancelled by fencing", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")
			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(worklease.ErrFenced)

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(10*time.Millisecond))
			defer stopRenewal()

			Eventually(renewCtx.Done()).Should(BeClosed())
			Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrFenced))
			_, ok := worklease.LeaseWindow(renewCtx)
			Expect(ok).To(BeFalse())
		})

		It("returns ok false after the parent context is cancelled", func() {
			lease := memLease(memory.New(), 30*time.Second)
			token, err := lease.Acquire(ctx, "w1")
			Expect(err).NotTo(HaveOccurred())
			parentCtx, parentCancel := context.WithCancel(ctx)

			renewCtx, stopRenewal := lease.StartRenewal(parentCtx, token)
			defer stopRenewal()
			_, ok := worklease.LeaseWindow(renewCtx)
			Expect(ok).To(BeTrue())

			parentCancel()
			Eventually(func() bool { _, ok := worklease.LeaseWindow(renewCtx); return ok }).Should(BeFalse())
		})

		It("stays valid and stops advancing after stopRenewal", func() {
			lease := memLease(memory.New(), 200*time.Millisecond)
			token, err := lease.Acquire(ctx, "w1")
			Expect(err).NotTo(HaveOccurred())

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(20*time.Millisecond))
			first := windowOf(renewCtx)
			Eventually(func() time.Time { return windowOf(renewCtx) }).Should(BeTemporally(">", first))

			stopRenewal()
			frozen, ok := worklease.LeaseWindow(renewCtx)
			Expect(ok).To(BeTrue())
			Consistently(func() time.Time { return windowOf(renewCtx) }, 60*time.Millisecond).Should(BeTemporally("==", frozen))
			_, ok = worklease.LeaseWindow(renewCtx)
			Expect(ok).To(BeTrue())
		})
	})

	// ===== Token =====
	Describe("Token", func() {
		It("String() produces exact format", func() {
			lease, _ := worklease.New(mockB, cfg)
			expiresAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    expiresAt,
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			expected := "worklease.Token{workID=\"w1\" holderID=\"test-worker\" fencingToken=1 expiresAt=2026-01-01T12:00:00Z}"
			Expect(token.String()).To(Equal(expected))
		})

		It("WorkID() returns correct value", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			Expect(token.WorkID()).To(Equal("w1"))
		})

		It("HolderID() returns correct value", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			Expect(token.HolderID()).To(Equal("test-worker"))
		})

		It("FencingToken() returns correct value", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 5,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			Expect(token.FencingToken()).To(Equal(uint64(5)))
		})

		It("ExpiresAt() returns correct value", func() {
			lease, _ := worklease.New(mockB, cfg)
			expiresAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    expiresAt,
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			Expect(token.ExpiresAt()).To(Equal(expiresAt))
		})
	})

	// ===== PHASE 7: Observer =====
	Describe("Observer", func() {
		var spy *spyObserver

		BeforeEach(func() {
			spy = &spyObserver{}
			cfg.Observer = spy
		})

		Context("when Config.Observer is nil", func() {
			It("does not panic on any operation", func() {
				nilCfg := worklease.Config{TTL: 30 * time.Second, HolderID: "test-worker"}
				lease, err := worklease.New(mockB, nilCfg)
				Expect(err).NotTo(HaveOccurred())

				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("s"), 30*time.Second).Return(nil)
				mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil)
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).Return(nil)

				Expect(func() {
					token, _ := lease.Acquire(ctx, "w1")
					_ = lease.Checkpoint(ctx, token, []byte("s"))
					_ = lease.Renew(ctx, token)
					_ = lease.Release(ctx, token, worklease.ExitFinished)
				}).NotTo(Panic())
			})
		})

		Context("when Config.Observer is set", func() {
			It("calls OnAcquire with the token and nil error on successful Acquire", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)

				token, err := lease.Acquire(ctx, "w1")
				Expect(err).NotTo(HaveOccurred())

				spy.mu.Lock()
				calls := spy.acquireCalls
				spy.mu.Unlock()

				Expect(calls).To(HaveLen(1))
				Expect(calls[0].WorkID).To(Equal("w1"))
				Expect(calls[0].Err).To(BeNil())
				Expect(calls[0].Token.WorkID()).To(Equal(token.WorkID()))
			})

			It("calls OnAcquire with zero Token and non-nil error on failed Acquire", func() {
				lease, _ := worklease.New(mockB, cfg)
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld)

				_, err := lease.Acquire(ctx, "w1")
				Expect(err).To(HaveOccurred())

				spy.mu.Lock()
				calls := spy.acquireCalls
				spy.mu.Unlock()

				Expect(calls).To(HaveLen(1))
				Expect(calls[0].Err).To(MatchError(worklease.ErrLeaseHeld))
				Expect(calls[0].Token).To(Equal(worklease.Token{}))
			})

			It("calls OnCheckpoint with the token, state size, and nil error on successful Checkpoint", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("payload"), 30*time.Second).Return(nil)

				token, _ := lease.Acquire(ctx, "w1")
				err := lease.Checkpoint(ctx, token, []byte("payload"))
				Expect(err).NotTo(HaveOccurred())

				spy.mu.Lock()
				calls := spy.checkpointCalls
				spy.mu.Unlock()

				Expect(calls).To(HaveLen(1))
				Expect(calls[0].Size).To(Equal(7))
				Expect(calls[0].Err).To(BeNil())
			})

			It("calls OnCheckpoint then OnFenced when Checkpoint returns ErrFenced", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().Checkpoint(gomock.Any(), record, gomock.Any(), 30*time.Second).Return(worklease.ErrFenced)

				token, _ := lease.Acquire(ctx, "w1")
				_ = lease.Checkpoint(ctx, token, []byte("s"))

				spy.mu.Lock()
				cCalls := spy.checkpointCalls
				fCalls := spy.fencedCalls
				spy.mu.Unlock()

				Expect(cCalls).To(HaveLen(1))
				Expect(errors.Is(cCalls[0].Err, worklease.ErrFenced)).To(BeTrue())
				Expect(fCalls).To(HaveLen(1))
			})

			It("calls OnRenew with the token and nil error on successful Renew", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil)

				token, _ := lease.Acquire(ctx, "w1")
				err := lease.Renew(ctx, token)
				Expect(err).NotTo(HaveOccurred())

				spy.mu.Lock()
				calls := spy.renewCalls
				spy.mu.Unlock()

				Expect(calls).To(HaveLen(1))
				Expect(calls[0].Err).To(BeNil())
			})

			It("calls OnRenew then OnFenced when Renew returns ErrFenced", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(worklease.ErrFenced)

				token, _ := lease.Acquire(ctx, "w1")
				_ = lease.Renew(ctx, token)

				spy.mu.Lock()
				rCalls := spy.renewCalls
				fCalls := spy.fencedCalls
				spy.mu.Unlock()

				Expect(rCalls).To(HaveLen(1))
				Expect(errors.Is(rCalls[0].Err, worklease.ErrFenced)).To(BeTrue())
				Expect(fCalls).To(HaveLen(1))
			})

			It("calls OnRelease with the token and nil error on successful Release", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).Return(nil)

				token, _ := lease.Acquire(ctx, "w1")
				err := lease.Release(ctx, token, worklease.ExitFinished)
				Expect(err).NotTo(HaveOccurred())

				spy.mu.Lock()
				calls := spy.releaseCalls
				spy.mu.Unlock()

				Expect(calls).To(HaveLen(1))
				Expect(calls[0].Err).To(BeNil())
			})

			It("calls OnAcquire with ErrLeaseHeld when lease is held and WithWaitForLease is not passed", func() {
				lease, _ := worklease.New(mockB, cfg)
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld)

				_, _ = lease.Acquire(ctx, "w1")

				spy.mu.Lock()
				calls := spy.acquireCalls
				spy.mu.Unlock()

				Expect(calls).To(HaveLen(1))
				Expect(errors.Is(calls[0].Err, worklease.ErrLeaseHeld)).To(BeTrue())
			})
		})

		Context("StartRenewal goroutine", func() {
			It("calls OnRenew after each successful renewal tick", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil).AnyTimes()

				token, _ := lease.Acquire(ctx, "w1")
				_, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(30*time.Millisecond))
				defer stopRenewal()

				Eventually(func() int {
					spy.mu.Lock()
					defer spy.mu.Unlock()
					return len(spy.renewCalls)
				}).Should(BeNumerically(">=", 1))

				spy.mu.Lock()
				firstCall := spy.renewCalls[0]
				spy.mu.Unlock()
				Expect(firstCall.Err).To(BeNil())
			})

			It("calls OnRenew then OnFenced when renewal returns ErrFenced", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(worklease.ErrFenced)

				token, _ := lease.Acquire(ctx, "w1")
				renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(30*time.Millisecond))
				defer stopRenewal()

				Eventually(renewCtx.Done()).Should(BeClosed())

				spy.mu.Lock()
				rCalls := spy.renewCalls
				fCalls := spy.fencedCalls
				spy.mu.Unlock()

				Expect(rCalls).To(HaveLen(1))
				Expect(errors.Is(rCalls[0].Err, worklease.ErrFenced)).To(BeTrue())
				Expect(rCalls[0].Attempt).To(Equal(1))
				Expect(fCalls).To(HaveLen(1))
			})

			It("calls OnRenew once with Attempt 1 and never OnFenced when renewal returns ErrLeaseExpired", func() {
				lease, _ := worklease.New(mockB, cfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(worklease.ErrLeaseExpired).Times(1)

				token, _ := lease.Acquire(ctx, "w1")
				renewCtx, stopRenewal := lease.StartRenewal(ctx, token,
					worklease.WithRenewalInterval(10*time.Millisecond),
					worklease.WithRenewalBackoff(time.Hour, time.Hour, 0))
				defer stopRenewal()

				Eventually(renewCtx.Done(), time.Second).Should(BeClosed())

				spy.mu.Lock()
				rCalls := append([]worklease.RenewEvent(nil), spy.renewCalls...)
				fCalls := len(spy.fencedCalls)
				spy.mu.Unlock()

				Expect(rCalls).To(HaveLen(1))
				Expect(rCalls[0].Attempt).To(Equal(1))
				Expect(errors.Is(rCalls[0].Err, worklease.ErrLeaseExpired)).To(BeTrue())
				Expect(fCalls).To(BeZero())
			})

			It("increments RenewEvent.Attempt on each retry until the lease window is exhausted", func() {
				// Short TTL so the bounded-retry loop produces several attempts then exhausts.
				shortCfg := cfg
				shortCfg.TTL = 120 * time.Millisecond
				lease, _ := worklease.New(mockB, shortCfg)
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(120 * time.Millisecond)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 120*time.Millisecond).Return(record, nil)
				mockB.EXPECT().Renew(gomock.Any(), record, 120*time.Millisecond).Return(errors.New("connection lost")).AnyTimes()

				token, _ := lease.Acquire(ctx, "w1")
				renewCtx, stopRenewal := lease.StartRenewal(ctx, token,
					worklease.WithRenewalInterval(10*time.Millisecond),
					worklease.WithRenewalBackoff(1*time.Millisecond, 5*time.Millisecond, 0))
				defer stopRenewal()

				Eventually(renewCtx.Done()).Should(BeClosed())
				Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))

				spy.mu.Lock()
				rCalls := append([]worklease.RenewEvent(nil), spy.renewCalls...)
				spy.mu.Unlock()

				Expect(len(rCalls)).To(BeNumerically(">=", 2))
				Expect(rCalls[0].Attempt).To(Equal(1))
				Expect(rCalls[1].Attempt).To(Equal(2))
				// Attempt is strictly increasing across the retry sequence.
				for i := 1; i < len(rCalls); i++ {
					Expect(rCalls[i].Attempt).To(Equal(rCalls[i-1].Attempt + 1))
				}
			})
		})
	})

	Describe("leaseClient observer", func() {
		var (
			spy    *spyObserver
			lease  worklease.Lease
			record backend.LeaseRecord
		)

		BeforeEach(func() {
			spy = &spyObserver{}
			cfg.Observer = spy
			lease, _ = worklease.New(mockB, cfg)
			record = backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(30 * time.Second)}
		})

		acquire := func() worklease.Token {
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, err := lease.Acquire(ctx, "w1")
			Expect(err).NotTo(HaveOccurred())
			return token
		}

		Context("Checkpoint", func() {
			It("calls OnCheckpoint before OnFenced when the backend returns ErrFenced", func() {
				token := acquire()
				mockB.EXPECT().Checkpoint(gomock.Any(), record, gomock.Any(), 30*time.Second).Return(worklease.ErrFenced)
				_ = lease.Checkpoint(ctx, token, []byte("s"))
				Expect(spy.order).To(Equal([]string{"OnAcquire", "OnCheckpoint", "OnFenced"}))
				Expect(spy.fencedCalls[0].Operation).To(Equal(worklease.OperationCheckpoint))
			})
			It("calls OnCheckpoint with correct Duration measured from the backend call", func() {
				token := acquire()
				mockB.EXPECT().Checkpoint(gomock.Any(), record, gomock.Any(), 30*time.Second).DoAndReturn(
					func(context.Context, backend.LeaseRecord, []byte, time.Duration) error {
						time.Sleep(5 * time.Millisecond)
						return nil
					})
				_ = lease.Checkpoint(ctx, token, []byte("s"))
				Expect(spy.checkpointCalls[0].Duration).To(BeNumerically(">=", 5*time.Millisecond))
			})
			It("calls OnCheckpoint with Size equal to len(state)", func() {
				token := acquire()
				mockB.EXPECT().Checkpoint(gomock.Any(), record, gomock.Any(), 30*time.Second).Return(nil)
				_ = lease.Checkpoint(ctx, token, []byte("payload"))
				Expect(spy.checkpointCalls[0].Size).To(Equal(7))
			})
			It("does not call OnFenced when the backend returns a non-fencing error", func() {
				token := acquire()
				mockB.EXPECT().Checkpoint(gomock.Any(), record, gomock.Any(), 30*time.Second).Return(errors.New("boom"))
				_ = lease.Checkpoint(ctx, token, []byte("s"))
				Expect(spy.fencedCalls).To(BeEmpty())
			})
			It("does not call OnFenced when the backend returns nil", func() {
				token := acquire()
				mockB.EXPECT().Checkpoint(gomock.Any(), record, gomock.Any(), 30*time.Second).Return(nil)
				_ = lease.Checkpoint(ctx, token, []byte("s"))
				Expect(spy.fencedCalls).To(BeEmpty())
			})
		})

		Context("Renew", func() {
			It("calls OnRenew before OnFenced when the backend returns ErrFenced", func() {
				token := acquire()
				mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(worklease.ErrFenced)
				_ = lease.Renew(ctx, token)
				Expect(spy.order).To(Equal([]string{"OnAcquire", "OnRenew", "OnFenced"}))
				Expect(spy.fencedCalls[0].Operation).To(Equal(worklease.OperationRenew))
			})
			It("calls OnRenew with correct Duration measured from the backend call", func() {
				token := acquire()
				mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).DoAndReturn(
					func(context.Context, backend.LeaseRecord, time.Duration) error {
						time.Sleep(5 * time.Millisecond)
						return nil
					})
				_ = lease.Renew(ctx, token)
				Expect(spy.renewCalls[0].Duration).To(BeNumerically(">=", 5*time.Millisecond))
			})
			It("does not call OnFenced when the backend returns nil", func() {
				token := acquire()
				mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil)
				_ = lease.Renew(ctx, token)
				Expect(spy.fencedCalls).To(BeEmpty())
			})
		})

		Context("Release", func() {
			It("calls OnRelease before OnFenced when the backend returns ErrFenced", func() {
				token := acquire()
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).Return(worklease.ErrFenced)
				_ = lease.Release(ctx, token, worklease.ExitFinished)
				Expect(spy.order).To(Equal([]string{"OnAcquire", "OnRelease", "OnFenced"}))
				Expect(spy.fencedCalls[0].Operation).To(Equal(worklease.OperationRelease))
			})
			It("calls OnRelease with correct Duration measured from the backend call", func() {
				token := acquire()
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).DoAndReturn(
					func(context.Context, backend.LeaseRecord, backend.ExitMode) error {
						time.Sleep(5 * time.Millisecond)
						return nil
					})
				_ = lease.Release(ctx, token, worklease.ExitFinished)
				Expect(spy.releaseCalls[0].Duration).To(BeNumerically(">=", 5*time.Millisecond))
			})
			It("does not call OnFenced when the backend returns nil", func() {
				token := acquire()
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).Return(nil)
				_ = lease.Release(ctx, token, worklease.ExitFinished)
				Expect(spy.fencedCalls).To(BeEmpty())
			})
		})

		Context("ReadCheckpoint", func() {
			It("calls OnReadCheckpoint with PrevExit, PrevHolderID, and Size from the backend result", func() {
				token := acquire()
				mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(backend.Checkpoint{State: []byte("prior"), PrevExit: backend.ExitAbandoned, PrevHolderID: "h-prev"}, nil)
				_, _ = lease.ReadCheckpoint(ctx, token)
				Expect(spy.readCheckpointCalls).To(HaveLen(1))
				Expect(spy.readCheckpointCalls[0].PrevExit).To(Equal(worklease.ExitAbandoned))
				Expect(spy.readCheckpointCalls[0].PrevHolderID).To(Equal("h-prev"))
				Expect(spy.readCheckpointCalls[0].Size).To(Equal(5))
			})
			It("calls OnReadCheckpoint with ExitNone, empty PrevHolderID, and Size 0 when Err is non-nil", func() {
				token := acquire()
				mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(backend.Checkpoint{State: []byte("leak"), PrevExit: backend.ExitFinished, PrevHolderID: "x"}, errors.New("boom"))
				_, _ = lease.ReadCheckpoint(ctx, token)
				Expect(spy.readCheckpointCalls).To(HaveLen(1))
				Expect(spy.readCheckpointCalls[0].PrevExit).To(Equal(worklease.ExitNone))
				Expect(spy.readCheckpointCalls[0].PrevHolderID).To(BeEmpty())
				Expect(spy.readCheckpointCalls[0].Size).To(BeZero())
			})
			It("calls OnReadCheckpoint with Size equal to len of the returned state", func() {
				token := acquire()
				mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(backend.Checkpoint{State: []byte("prior")}, nil)
				_, _ = lease.ReadCheckpoint(ctx, token)
				Expect(spy.readCheckpointCalls[0].Size).To(Equal(5))
			})
			It("calls OnReadCheckpoint with the error when the backend returns ErrFenced", func() {
				token := acquire()
				mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(backend.Checkpoint{}, worklease.ErrFenced)
				_, _ = lease.ReadCheckpoint(ctx, token)
				Expect(errors.Is(spy.readCheckpointCalls[0].Err, worklease.ErrFenced)).To(BeTrue())
			})
			It("does not call OnFenced when ReadCheckpoint returns ErrFenced", func() {
				token := acquire()
				mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(backend.Checkpoint{}, worklease.ErrFenced)
				_, _ = lease.ReadCheckpoint(ctx, token)
				Expect(spy.fencedCalls).To(BeEmpty())
			})
		})

		Context("Acquire", func() {
			It("calls OnAcquire with the zero Token when the backend returns an error", func() {
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld)
				_, _ = lease.Acquire(ctx, "w1")
				Expect(spy.acquireCalls[0].Token).To(Equal(worklease.Token{}))
				Expect(errors.Is(spy.acquireCalls[0].Err, worklease.ErrLeaseHeld)).To(BeTrue())
			})
			It("calls OnAcquire with a populated Token on success", func() {
				_ = acquire()
				Expect(spy.acquireCalls[0].Token.WorkID()).To(Equal("w1"))
				Expect(spy.acquireCalls[0].Err).To(BeNil())
			})
			It("calls OnAcquire with Duration measured from the backend call only — not the wait loop", func() {
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).DoAndReturn(
					func(context.Context, string, string, time.Duration) (backend.LeaseRecord, error) {
						time.Sleep(5 * time.Millisecond)
						return record, nil
					})
				_, err := lease.Acquire(ctx, "w1")
				Expect(err).NotTo(HaveOccurred())
				Expect(spy.acquireCalls[0].Duration).To(BeNumerically(">=", 5*time.Millisecond))
			})

			It("calls OnAcquire once per poll when WithWaitForLease is set", func() {
				gomock.InOrder(
					mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld),
					mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld),
					mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil),
				)
				_, err := lease.Acquire(ctx, "w1", worklease.WithWaitForLease(), worklease.WithPollInterval(time.Millisecond))
				Expect(err).NotTo(HaveOccurred())
				Expect(spy.acquireCalls).To(HaveLen(3))
				Expect(spy.acquireCalls[0].Err).To(MatchError(worklease.ErrLeaseHeld))
				Expect(spy.acquireCalls[1].Err).To(MatchError(worklease.ErrLeaseHeld))
				Expect(spy.acquireCalls[2].Err).NotTo(HaveOccurred())
			})
		})
	})

	Describe("HasWaitForLease", func() {
		Context("with no options", func() {
			It("returns false", func() {
				Expect(worklease.HasWaitForLease(nil)).To(BeFalse())
			})
		})
		Context("with WithWaitForLease option", func() {
			It("returns true", func() {
				Expect(worklease.HasWaitForLease([]worklease.AcquireOption{worklease.WithWaitForLease()})).To(BeTrue())
			})
		})
		Context("with WithPollInterval only", func() {
			It("returns false", func() {
				Expect(worklease.HasWaitForLease([]worklease.AcquireOption{worklease.WithPollInterval(time.Second)})).To(BeFalse())
			})
		})
		Context("with nil options slice", func() {
			It("returns false without panicking", func() {
				Expect(func() { worklease.HasWaitForLease(nil) }).NotTo(Panic())
				Expect(worklease.HasWaitForLease(nil)).To(BeFalse())
			})
		})
		Context("with WithWaitForLease and WithPollInterval together", func() {
			It("returns true", func() {
				opts := []worklease.AcquireOption{
					worklease.WithWaitForLease(),
					worklease.WithPollInterval(500 * time.Millisecond),
				}
				Expect(worklease.HasWaitForLease(opts)).To(BeTrue())
			})
		})
	})

	Describe("NewVacuum", func() {
		It("returns nil and ErrNilBackend for a nil backend", func() {
			v, err := worklease.NewVacuum(nil, worklease.VacuumConfig{})
			Expect(v).To(BeNil())
			Expect(errors.Is(err, worklease.ErrNilBackend)).To(BeTrue())
		})

		It("returns a Vacuum and nil error for a non-nil backend and a zero VacuumConfig", func() {
			v, err := worklease.NewVacuum(mockB, worklease.VacuumConfig{})
			Expect(err).NotTo(HaveOccurred())
			Expect(v).NotTo(BeNil())
		})
	})

	Describe("Vacuum.Sweep", func() {
		Context("when opts.Retention is zero", func() {
			It("returns 0 and ErrRetentionRequired without calling the backend", func() {
				v, err := worklease.NewVacuum(mockB, worklease.VacuumConfig{})
				Expect(err).NotTo(HaveOccurred())
				n, err := v.Sweep(ctx, worklease.SweepOptions{Retention: 0})
				Expect(n).To(BeZero())
				Expect(errors.Is(err, worklease.ErrRetentionRequired)).To(BeTrue())
			})
		})

		Context("when opts.Retention is negative", func() {
			It("returns 0 and ErrRetentionRequired without calling the backend", func() {
				v, err := worklease.NewVacuum(mockB, worklease.VacuumConfig{})
				Expect(err).NotTo(HaveOccurred())
				n, err := v.Sweep(ctx, worklease.SweepOptions{Retention: -time.Second})
				Expect(n).To(BeZero())
				Expect(errors.Is(err, worklease.ErrRetentionRequired)).To(BeTrue())
			})
		})

		Context("when opts.Retention is not positive", func() {
			It("returns an error that matches ErrConfigInvalid and keeps its message", func() {
				v, err := worklease.NewVacuum(mockB, worklease.VacuumConfig{})
				Expect(err).NotTo(HaveOccurred())
				_, err = v.Sweep(ctx, worklease.SweepOptions{Retention: 0})
				Expect(errors.Is(err, worklease.ErrRetentionRequired)).To(BeTrue())
				Expect(errors.Is(err, worklease.ErrConfigInvalid)).To(BeTrue())
				Expect(err.Error()).To(Equal("worklease: retention must be greater than zero"))
			})
		})

		Context("when opts.Retention is positive", func() {
			It("delegates to Backend.Sweep and returns its result unchanged, success case", func() {
				v, err := worklease.NewVacuum(mockB, worklease.VacuumConfig{})
				Expect(err).NotTo(HaveOccurred())
				opts := worklease.SweepOptions{Retention: time.Hour, IncludeExpired: true}
				mockB.EXPECT().Sweep(gomock.Any(), opts).Return(int64(3), nil)
				n, err := v.Sweep(ctx, opts)
				Expect(err).To(BeNil())
				Expect(n).To(Equal(int64(3)))
			})

			It("delegates to Backend.Sweep and returns its error unchanged, failure case", func() {
				v, err := worklease.NewVacuum(mockB, worklease.VacuumConfig{})
				Expect(err).NotTo(HaveOccurred())
				opts := worklease.SweepOptions{Retention: time.Hour}
				backendErr := errors.New("sweep failed")
				mockB.EXPECT().Sweep(gomock.Any(), opts).Return(int64(0), backendErr)
				n, err := v.Sweep(ctx, opts)
				Expect(n).To(BeZero())
				Expect(err).To(Equal(backendErr))
			})
		})

		Context("on a zero-value Vacuum", func() {
			It("zero-value Vacuum → returns 0 and ErrNilBackend without panicking", func() {
				var (
					n   int64
					err error
				)
				Expect(func() {
					n, err = (&worklease.Vacuum{}).Sweep(ctx, worklease.SweepOptions{Retention: time.Hour})
				}).NotTo(Panic())
				Expect(n).To(BeZero())
				Expect(errors.Is(err, worklease.ErrNilBackend)).To(BeTrue())
			})
		})

		Context("on a nil *Vacuum", func() {
			It("nil *Vacuum → returns 0 and ErrNilBackend without panicking", func() {
				var (
					v   *worklease.Vacuum
					n   int64
					err error
				)
				Expect(func() {
					n, err = v.Sweep(ctx, worklease.SweepOptions{Retention: time.Hour})
				}).NotTo(Panic())
				Expect(n).To(BeZero())
				Expect(errors.Is(err, worklease.ErrNilBackend)).To(BeTrue())
			})
		})

		Context("with an observer", func() {
			var spy *sweepSpy

			BeforeEach(func() {
				spy = &sweepSpy{}
			})

			It("calls OnSweep once with the options, the deleted count, a Duration, and nil Err on success", func() {
				v, err := worklease.NewVacuum(mockB, worklease.VacuumConfig{Observer: spy})
				Expect(err).NotTo(HaveOccurred())
				opts := worklease.SweepOptions{Retention: time.Hour, IncludeExpired: true}
				mockB.EXPECT().Sweep(gomock.Any(), opts).DoAndReturn(
					func(context.Context, backend.SweepOptions) (int64, error) {
						time.Sleep(time.Millisecond)
						return 4, nil
					})
				n, err := v.Sweep(ctx, opts)
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(4)))

				events := spy.calls()
				Expect(events).To(HaveLen(1))
				Expect(events[0].Options).To(Equal(opts))
				Expect(events[0].Deleted).To(Equal(int64(4)))
				Expect(events[0].Duration).To(BeNumerically(">", 0))
				Expect(events[0].Err).To(BeNil())
			})

			It("calls OnSweep once with the backend error on failure", func() {
				v, err := worklease.NewVacuum(mockB, worklease.VacuumConfig{Observer: spy})
				Expect(err).NotTo(HaveOccurred())
				opts := worklease.SweepOptions{Retention: time.Hour}
				backendErr := errors.New("sweep failed")
				mockB.EXPECT().Sweep(gomock.Any(), opts).Return(int64(0), backendErr)
				_, err = v.Sweep(ctx, opts)
				Expect(err).To(Equal(backendErr))

				events := spy.calls()
				Expect(events).To(HaveLen(1))
				Expect(events[0].Err).To(Equal(backendErr))
				Expect(events[0].Deleted).To(BeZero())
			})

			It("does not call OnSweep when Retention is not positive", func() {
				v, err := worklease.NewVacuum(mockB, worklease.VacuumConfig{Observer: spy})
				Expect(err).NotTo(HaveOccurred())
				mockB.EXPECT().Sweep(gomock.Any(), gomock.Any()).Times(0)
				_, err = v.Sweep(ctx, worklease.SweepOptions{Retention: 0})
				Expect(errors.Is(err, worklease.ErrRetentionRequired)).To(BeTrue())
				Expect(spy.calls()).To(BeEmpty())
			})
		})
	})

	// ===== PHASE 8: v0.8 API audit =====
	Describe("Sentinel re-exports (v0.8)", func() {
		Context("the sentinels are the identical values of the backend sentinels", func() {
			It("worklease.ErrFenced is backend.ErrFenced", func() {
				Expect(worklease.ErrFenced).To(BeIdenticalTo(backend.ErrFenced))
				Expect(errors.Is(backend.ErrFenced, worklease.ErrFenced)).To(BeTrue())
			})

			It("worklease.ErrLeaseHeld is backend.ErrLeaseHeld", func() {
				Expect(worklease.ErrLeaseHeld).To(BeIdenticalTo(backend.ErrLeaseHeld))
				Expect(errors.Is(backend.ErrLeaseHeld, worklease.ErrLeaseHeld)).To(BeTrue())
			})

			It("worklease.ErrLeaseExpired is backend.ErrLeaseExpired", func() {
				Expect(worklease.ErrLeaseExpired).To(BeIdenticalTo(backend.ErrLeaseExpired))
				Expect(errors.Is(backend.ErrLeaseExpired, worklease.ErrLeaseExpired)).To(BeTrue())
			})

			It("worklease.ErrInvalidExitMode is backend.ErrInvalidExitMode", func() {
				Expect(worklease.ErrInvalidExitMode).To(BeIdenticalTo(backend.ErrInvalidExitMode))
				Expect(errors.Is(backend.ErrInvalidExitMode, worklease.ErrInvalidExitMode)).To(BeTrue())
			})

			It("each of the four sentinels has its documented message", func() {
				Expect(worklease.ErrFenced.Error()).To(Equal("worklease: fenced — the lease is no longer held by this token"))
				Expect(worklease.ErrLeaseHeld.Error()).To(Equal("worklease: lease is currently held"))
				Expect(worklease.ErrLeaseExpired.Error()).To(Equal("worklease: lease has expired"))
				Expect(worklease.ErrInvalidExitMode.Error()).To(Equal("worklease: invalid exit mode"))
			})
		})
	})

	Describe("Operation (v0.8)", func() {
		named := []worklease.Operation{worklease.OperationCheckpoint, worklease.OperationRenew, worklease.OperationRelease, worklease.OperationForget}

		It("the zero value is not any named Operation", func() {
			var zero worklease.Operation
			for _, op := range named {
				Expect(zero).NotTo(Equal(op))
			}
		})

		It("the named values are Checkpoint=1, Renew=2, Release=3, Forget=4", func() {
			Expect(uint8(worklease.OperationCheckpoint)).To(Equal(uint8(1)))
			Expect(uint8(worklease.OperationRenew)).To(Equal(uint8(2)))
			Expect(uint8(worklease.OperationRelease)).To(Equal(uint8(3)))
			Expect(uint8(worklease.OperationForget)).To(Equal(uint8(4)))
		})

		It("String returns the text form of each named value", func() {
			Expect(worklease.OperationCheckpoint.String()).To(Equal("Checkpoint"))
			Expect(worklease.OperationRenew.String()).To(Equal("Renew"))
			Expect(worklease.OperationRelease.String()).To(Equal("Release"))
			Expect(worklease.OperationForget.String()).To(Equal("Forget"))
		})

		It("String formats an unknown value as Operation(<n>)", func() {
			Expect(worklease.Operation(0).String()).To(Equal("Operation(0)"))
			Expect(worklease.Operation(9).String()).To(Equal("Operation(9)"))
		})
	})

	Describe("NewToken (v0.8)", func() {
		It("the accessors report the work ID, holder ID, fencing token, and expiry that were passed", func() {
			expires := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			token := worklease.NewToken("work-1", "holder-1", 7, expires)
			Expect(token.WorkID()).To(Equal("work-1"))
			Expect(token.HolderID()).To(Equal("holder-1"))
			Expect(token.FencingToken()).To(Equal(uint64(7)))
			Expect(token.ExpiresAt()).To(Equal(expires))
		})

		It("NewToken with zero arguments equals the zero Token", func() {
			Expect(worklease.NewToken("", "", 0, time.Time{})).To(Equal(worklease.Token{}))
		})

		It("a Token from NewToken has a closed lease window: StartRenewal cancels the renewal context", func() {
			mb := testutil.NewMockBackend(gomock.NewController(GinkgoT()))
			mb.EXPECT().Renew(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			l, err := worklease.New(mb, worklease.Config{TTL: 30 * time.Second, HolderID: "holder-1"})
			Expect(err).NotTo(HaveOccurred())
			token := worklease.NewToken("work-1", "holder-1", 1, time.Now().Add(time.Hour))
			renewCtx, stopRenewal := l.StartRenewal(context.Background(), token)
			DeferCleanup(stopRenewal)
			Eventually(renewCtx.Done(), time.Second).Should(BeClosed())
			Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))
		})
	})

	Describe("NoopObserver (v0.8)", func() {
		It("a struct embedding NoopObserver and overriding OnAcquire counts OnAcquire and tolerates every other event", func() {
			spy := &acquireSpy{}
			var obs worklease.LeaseObserver = spy
			Expect(func() {
				obs.OnAcquire(ctx, worklease.AcquireEvent{})
				obs.OnCheckpoint(ctx, worklease.CheckpointEvent{})
				obs.OnRenew(ctx, worklease.RenewEvent{})
				obs.OnRelease(ctx, worklease.ReleaseEvent{})
				obs.OnReadCheckpoint(ctx, worklease.ReadCheckpointEvent{})
				obs.OnForget(ctx, worklease.ForgetEvent{})
				obs.OnFenced(ctx, worklease.FencedEvent{})
			}).NotTo(Panic())
			Expect(spy.n).To(Equal(1))
		})

		It("a struct embedding NoopVacuumObserver and overriding OnSweep counts the sweep event", func() {
			spy := &noopSweepSpy{}
			var obs worklease.VacuumObserver = spy
			obs.OnSweep(ctx, worklease.SweepEvent{})
			Expect(spy.n).To(Equal(1))
		})
	})
})

// The acquireSpy type embeds NoopObserver and counts OnAcquire calls.
type acquireSpy struct {
	worklease.NoopObserver
	n int
}

func (s *acquireSpy) OnAcquire(_ context.Context, _ worklease.AcquireEvent) { s.n++ }

// The noopSweepSpy type embeds NoopVacuumObserver and counts OnSweep calls.
type noopSweepSpy struct {
	worklease.NoopVacuumObserver
	n int
}

func (s *noopSweepSpy) OnSweep(_ context.Context, _ worklease.SweepEvent) { s.n++ }
