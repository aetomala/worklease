package worker_test

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
	"github.com/aetomala/worklease/worker"
)

var _ = Describe("Runner", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		ctrl   *gomock.Controller
		mockB  *testutil.MockBackend
		lease  worklease.Lease
		cfg    worklease.Config
		record backend.LeaseRecord
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		ctrl = gomock.NewController(GinkgoT())
		mockB = testutil.NewMockBackend(ctrl)
		cfg = worklease.Config{
			TTL:      30 * time.Second,
			HolderID: "test-worker",
		}
		var err error
		lease, err = worklease.New(mockB, cfg)
		Expect(err).NotTo(HaveOccurred())
		record = backend.LeaseRecord{
			WorkID:       "w1",
			HolderID:     "test-worker",
			FencingToken: 1,
		}
	})

	AfterEach(func() {
		cancel()
		ctrl.Finish()
	})

	// ===== PHASE 1: Constructor and Initialization =====
	Describe("Phase 1: Constructor and Initialization", func() {
		It("nil Lease → returns ErrLeaseRequired", func() {
			_, err := worker.NewRunner(worker.RunnerConfig{
				WorkFn: func(_ context.Context, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					return nil, nil
				},
			})
			Expect(errors.Is(err, worker.ErrLeaseRequired)).To(BeTrue())
		})

		It("nil WorkFn → returns ErrWorkFnRequired", func() {
			_, err := worker.NewRunner(worker.RunnerConfig{Lease: lease})
			Expect(errors.Is(err, worker.ErrWorkFnRequired)).To(BeTrue())
		})

		It("valid config → returns non-nil Runner", func() {
			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					return nil, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(r).NotTo(BeNil())
		})
	})

	// ===== PHASE 2: Run — Happy Path =====
	Describe("Phase 2: Run — Happy Path", func() {
		It("WorkFn returns (finalState, nil) → Checkpoint called, Release called, Run returns nil", func() {
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(nil, false, nil)
			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil).AnyTimes()
			mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("done"), 30*time.Second).Return(nil)
			mockB.EXPECT().Release(gomock.Any(), record).Return(nil)

			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					return []byte("done"), nil
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = r.Run(ctx, "w1")
			Expect(err).NotTo(HaveOccurred())
		})

		It("WorkFn returns (nil, nil) → no Checkpoint, Release called, Run returns nil", func() {
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(nil, false, nil)
			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil).AnyTimes()
			mockB.EXPECT().Release(gomock.Any(), record).Return(nil)

			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					return nil, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = r.Run(ctx, "w1")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	// ===== PHASE 3: Run — Crash and Resume =====
	Describe("Phase 3: Run — Crash and Resume", func() {
		It("ReadCheckpoint returns (priorState, cleanHandoff=false) → fn receives prior bytes and cleanHandoff=false", func() {
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return([]byte("cursor-at-42"), false, nil)
			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil).AnyTimes()
			mockB.EXPECT().Release(gomock.Any(), record).Return(nil)

			var capturedPrior []byte
			var capturedHandoff bool
			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
					capturedPrior = prior
					capturedHandoff = cleanHandoff
					return nil, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = r.Run(ctx, "w1")
			Expect(err).NotTo(HaveOccurred())
			Expect(capturedPrior).To(Equal([]byte("cursor-at-42")))
			Expect(capturedHandoff).To(BeFalse())
		})

		It("ReadCheckpoint returns (priorState, cleanHandoff=true) → fn receives cleanHandoff=true", func() {
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return([]byte("cursor-at-99"), true, nil)
			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil).AnyTimes()
			mockB.EXPECT().Release(gomock.Any(), record).Return(nil)

			var capturedHandoff bool
			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, _ []byte, cleanHandoff bool) ([]byte, error) {
					capturedHandoff = cleanHandoff
					return nil, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = r.Run(ctx, "w1")
			Expect(err).NotTo(HaveOccurred())
			Expect(capturedHandoff).To(BeTrue())
		})
	})

	// ===== PHASE 4: Run — ErrFenced Propagation =====
	Describe("Phase 4: Run — ErrFenced Propagation", func() {
		It("WorkFn returns (nil, ErrFenced) → no Checkpoint, no Release, Run returns ErrFenced", func() {
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(nil, false, nil)
			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil).AnyTimes()

			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					return nil, worklease.ErrFenced
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = r.Run(ctx, "w1")
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})

		It("WorkFn returns (partialState, ErrFenced) → partial state ignored, no Release, Run returns ErrFenced", func() {
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(nil, false, nil)
			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil).AnyTimes()

			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					return []byte("partial"), worklease.ErrFenced
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = r.Run(ctx, "w1")
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})

		It("ReadCheckpoint returns ErrFenced → no StartRenewal, no WorkFn call, no Release, Run returns ErrFenced", func() {
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(nil, false, worklease.ErrFenced)

			fnCalled := false
			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					fnCalled = true
					return nil, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = r.Run(ctx, "w1")
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
			Expect(fnCalled).To(BeFalse())
		})
	})

	// ===== PHASE 5: Run — Non-Fencing Errors =====
	Describe("Phase 5: Run — Non-Fencing Errors", func() {
		It("WorkFn returns (partialState, someErr) → Checkpoint called, Release called, Run returns someErr", func() {
			someErr := errors.New("work failed")
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(nil, false, nil)
			mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil).AnyTimes()
			mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("partial"), 30*time.Second).Return(nil)
			mockB.EXPECT().Release(gomock.Any(), record).Return(nil)

			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					return []byte("partial"), someErr
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = r.Run(ctx, "w1")
			Expect(errors.Is(err, someErr)).To(BeTrue())
		})

		It("ReadCheckpoint returns non-fencing error → Release called, Run returns wrapped error", func() {
			readErr := errors.New("storage unavailable")
			mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
			mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(nil, false, readErr)
			mockB.EXPECT().Release(gomock.Any(), record).Return(nil)

			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					return nil, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = r.Run(ctx, "w1")
			Expect(errors.Is(err, readErr)).To(BeTrue())
		})
	})
})
