package worklease_test

import (
	"context"
	"errors"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/testutil"
)

type spyObserver struct {
	acquireCalls        []worklease.AcquireEvent
	checkpointCalls     []worklease.CheckpointEvent
	renewCalls          []worklease.RenewEvent
	releaseCalls        []worklease.ReleaseEvent
	readCheckpointCalls []worklease.ReadCheckpointEvent
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

func (s *spyObserver) OnFenced(_ context.Context, e worklease.FencedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fencedCalls = append(s.fencedCalls, e)
	s.order = append(s.order, "OnFenced")
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
		It("backend nil → non-nil error, no Lease returned", func() {
			lease, err := worklease.New(nil, cfg)
			Expect(err).NotTo(BeNil())
			Expect(lease).To(BeNil())
		})

		It("TTL zero → non-nil error, no Lease returned", func() {
			badCfg := cfg
			badCfg.TTL = 0
			lease, err := worklease.New(mockB, badCfg)
			Expect(err).NotTo(BeNil())
			Expect(lease).To(BeNil())
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
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld).AnyTimes()
			// With default 2s poll interval, this should timeout before succeeding (100ms timeout < 2s poll interval)
			shortCtx, shortCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer shortCancel()
			_, err := lease.Acquire(shortCtx, "w1", worklease.WithWaitForLease())
			Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue())
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

			mockB.EXPECT().Release(gomock.Any(), record).Return(nil)
			err := lease.Release(ctx, token)
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

			mockB.EXPECT().Release(gomock.Any(), record).Return(worklease.ErrFenced)
			err := lease.Release(ctx, token)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("w1"))
			Expect(err.Error()).To(ContainSubstring("test-worker"))
		})
	})

	Describe("ReadCheckpoint", func() {
		It("fresh acquisition (nil state from backend) → nil state, cleanHandoff=false, nil error", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(nil, false, nil)
			state, cleanHandoff, err := lease.ReadCheckpoint(ctx, token)
			Expect(err).To(BeNil())
			Expect(state).To(BeNil())
			Expect(cleanHandoff).To(BeFalse())
		})

		It("prior Release → cleanHandoff=true, last checkpoint bytes returned", func() {
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
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(checkpointData, true, nil)
			state, cleanHandoff, err := lease.ReadCheckpoint(ctx, token)
			Expect(err).To(BeNil())
			Expect(state).To(Equal(checkpointData))
			Expect(cleanHandoff).To(BeTrue())
		})

		It("expired without Release → cleanHandoff=false, last checkpoint bytes returned", func() {
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
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(checkpointData, false, nil)
			state, cleanHandoff, err := lease.ReadCheckpoint(ctx, token)
			Expect(err).To(BeNil())
			Expect(state).To(Equal(checkpointData))
			Expect(cleanHandoff).To(BeFalse())
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
			lease, _ := worklease.New(mockB, cfg)
			// Short lease window so the bounded-retry loop exhausts it quickly.
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(150 * time.Millisecond),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			// Every renewal attempt fails with a non-fencing error, so the goroutine
			// retries until the lease window is exhausted (v0.5 bounded-retry contract).
			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(errors.New("connection lost")).AnyTimes()

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token,
				worklease.WithRenewalInterval(10*time.Millisecond),
				worklease.WithRenewalBackoff(1*time.Millisecond, 5*time.Millisecond, 0))
			defer stopRenewal()

			Eventually(renewCtx.Done()).Should(BeClosed())
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
				mockB.EXPECT().Release(gomock.Any(), record).Return(nil)

				Expect(func() {
					token, _ := lease.Acquire(ctx, "w1")
					_ = lease.Checkpoint(ctx, token, []byte("s"))
					_ = lease.Renew(ctx, token)
					_ = lease.Release(ctx, token)
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
				mockB.EXPECT().Release(gomock.Any(), record).Return(nil)

				token, _ := lease.Acquire(ctx, "w1")
				err := lease.Release(ctx, token)
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

			It("increments RenewEvent.Attempt on each retry until the lease window is exhausted", func() {
				lease, _ := worklease.New(mockB, cfg)
				// Short window so the bounded-retry loop produces several attempts then exhausts.
				record := backend.LeaseRecord{WorkID: "w1", HolderID: "test-worker", FencingToken: 1, ExpiresAt: time.Now().Add(120 * time.Millisecond)}
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(errors.New("connection lost")).AnyTimes()

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
				mockB.EXPECT().Release(gomock.Any(), record).Return(worklease.ErrFenced)
				_ = lease.Release(ctx, token)
				Expect(spy.order).To(Equal([]string{"OnAcquire", "OnRelease", "OnFenced"}))
				Expect(spy.fencedCalls[0].Operation).To(Equal(worklease.OperationRelease))
			})
			It("calls OnRelease with correct Duration measured from the backend call", func() {
				token := acquire()
				mockB.EXPECT().Release(gomock.Any(), record).DoAndReturn(
					func(context.Context, backend.LeaseRecord) error {
						time.Sleep(5 * time.Millisecond)
						return nil
					})
				_ = lease.Release(ctx, token)
				Expect(spy.releaseCalls[0].Duration).To(BeNumerically(">=", 5*time.Millisecond))
			})
			It("does not call OnFenced when the backend returns nil", func() {
				token := acquire()
				mockB.EXPECT().Release(gomock.Any(), record).Return(nil)
				_ = lease.Release(ctx, token)
				Expect(spy.fencedCalls).To(BeEmpty())
			})
		})

		Context("ReadCheckpoint", func() {
			It("calls OnReadCheckpoint with CleanHandoff true when the backend returns true", func() {
				token := acquire()
				mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return([]byte("prior"), true, nil)
				_, _, _ = lease.ReadCheckpoint(ctx, token)
				Expect(spy.readCheckpointCalls).To(HaveLen(1))
				Expect(spy.readCheckpointCalls[0].CleanHandoff).To(BeTrue())
			})
			It("calls OnReadCheckpoint with Size equal to len of the returned state", func() {
				token := acquire()
				mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return([]byte("prior"), false, nil)
				_, _, _ = lease.ReadCheckpoint(ctx, token)
				Expect(spy.readCheckpointCalls[0].Size).To(Equal(5))
			})
			It("calls OnReadCheckpoint with the error when the backend returns ErrFenced", func() {
				token := acquire()
				mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(nil, false, worklease.ErrFenced)
				_, _, _ = lease.ReadCheckpoint(ctx, token)
				Expect(errors.Is(spy.readCheckpointCalls[0].Err, worklease.ErrFenced)).To(BeTrue())
			})
			It("does not call OnFenced when ReadCheckpoint returns ErrFenced", func() {
				token := acquire()
				mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(nil, false, worklease.ErrFenced)
				_, _, _ = lease.ReadCheckpoint(ctx, token)
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
})
