package worklease_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/testutil"
)

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

		It("lease held + WithWaitForLease → ErrLeaseHeld on context deadline", func() {
			lease, _ := worklease.New(mockB, cfg)
			shortCtx, shortCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer shortCancel()
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld).AnyTimes()
			_, err := lease.Acquire(shortCtx, "w1", worklease.WithWaitForLease(), worklease.WithPollInterval(10*time.Millisecond))
			Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeTrue())
		})

		It("uses default poll interval 2s when WithPollInterval not set", func() {
			lease, _ := worklease.New(mockB, cfg)
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", gomock.Any()).Return(backend.LeaseRecord{}, worklease.ErrLeaseHeld).AnyTimes()
			// With default 2s poll interval, this should timeout before succeeding (100ms timeout < 2s poll interval)
			shortCtx, shortCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer shortCancel()
			_, err := lease.Acquire(shortCtx, "w1", worklease.WithWaitForLease())
			Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeTrue())
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
		})

		It("non-fencing error → renewCtx cancelled when Renew returns non-fencing error", func() {
			lease, _ := worklease.New(mockB, cfg)
			record := backend.LeaseRecord{
				WorkID:       "w1",
				HolderID:     "test-worker",
				FencingToken: 1,
				ExpiresAt:    time.Now().Add(30 * time.Second),
			}
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			token, _ := lease.Acquire(ctx, "w1")

			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(errors.New("connection lost"))

			renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(50*time.Millisecond))
			defer stopRenewal()

			Eventually(renewCtx.Done()).Should(BeClosed())
		})

		It("parent context cancelled → goroutine exits; renewCtx cancelled as consequence", func() {
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
})
