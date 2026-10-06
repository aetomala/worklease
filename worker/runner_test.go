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
		ctx       context.Context
		cancel    context.CancelFunc
		ctrl      *gomock.Controller
		mockB     *testutil.MockBackend
		mockLease *testutil.MockLease
		lease     worklease.Lease
		cfg       worklease.Config
		record    backend.LeaseRecord
		token     worklease.Token
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		ctrl = gomock.NewController(GinkgoT())
		mockB = testutil.NewMockBackend(ctrl)
		mockLease = testutil.NewMockLease(ctrl)
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
		token = worklease.Token{}
	})

	AfterEach(func() {
		cancel()
		ctrl.Finish()
	})

	// expectMockRun wires mockLease through Acquire, ReadCheckpoint, and
	// StartRenewal. A non-nil cause cancels the renewal context before WorkFn
	// runs, as the renewal goroutine does on fencing or window exhaustion.
	expectMockRun := func(cause error) {
		mockLease.EXPECT().Acquire(gomock.Any(), "w1").Return(token, nil)
		mockLease.EXPECT().ReadCheckpoint(gomock.Any(), token).Return(worklease.Checkpoint{}, nil)
		renewCtx, cancelRenew := context.WithCancelCause(ctx)
		if cause != nil {
			cancelRenew(cause)
		}
		mockLease.EXPECT().StartRenewal(gomock.Any(), token).Return(renewCtx, func() { cancelRenew(context.Canceled) })
	}

	// expectBackendRun wires mockB through Acquire, ReadCheckpoint, and Renew for
	// a Runner built on the real lease client.
	expectBackendRun := func(cp backend.Checkpoint) {
		mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
		mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(cp, nil)
		mockB.EXPECT().Renew(gomock.Any(), record, 30*time.Second).Return(nil).AnyTimes()
	}

	newRunner := func(l worklease.Lease, fn worker.WorkFn) *worker.Runner {
		r, err := worker.NewRunner(worker.RunnerConfig{Lease: l, WorkFn: fn})
		Expect(err).NotTo(HaveOccurred())
		return r
	}

	// ===== PHASE 1: Constructor and Initialization =====
	Describe("Phase 1: Constructor and Initialization", func() {
		It("nil Lease → returns ErrLeaseRequired", func() {
			_, err := worker.NewRunner(worker.RunnerConfig{
				WorkFn: func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return nil, nil
				},
			})
			Expect(errors.Is(err, worker.ErrLeaseRequired)).To(BeTrue())
		})

		It("nil WorkFn → returns ErrWorkFnRequired", func() {
			_, err := worker.NewRunner(worker.RunnerConfig{Lease: lease})
			Expect(errors.Is(err, worker.ErrWorkFnRequired)).To(BeTrue())
		})

		It("sentinel messages carry the worker: package prefix", func() {
			Expect(worker.ErrLeaseRequired.Error()).To(HavePrefix("worker: "))
			Expect(worker.ErrWorkFnRequired.Error()).To(HavePrefix("worker: "))
		})

		It("valid config → returns non-nil Runner", func() {
			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return nil, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(r).NotTo(BeNil())
		})
	})

	// ===== PHASE 2: Run — Successful Exits =====
	Describe("Phase 2: Run — Successful Exits", func() {
		Context("WorkFn returns (state, nil)", func() {
			It("checkpoints state, releases with ExitFinished, returns nil", func() {
				expectBackendRun(backend.Checkpoint{})
				mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("done"), 30*time.Second).Return(nil)
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).Return(nil)

				r := newRunner(lease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return []byte("done"), nil
				})
				Expect(r.Run(ctx, "w1")).To(Succeed())
			})
		})

		Context("WorkFn returns (nil, nil)", func() {
			It("skips Checkpoint, releases with ExitFinished, returns nil", func() {
				expectBackendRun(backend.Checkpoint{})
				mockB.EXPECT().Checkpoint(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).Return(nil)

				r := newRunner(lease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return nil, nil
				})
				Expect(r.Run(ctx, "w1")).To(Succeed())
			})
		})

		Context("WorkFn returns an error wrapping ErrRetire", func() {
			It("checkpoints state, releases with ExitRetired, returns nil", func() {
				expectBackendRun(backend.Checkpoint{})
				mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("last"), 30*time.Second).Return(nil)
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitRetired).Return(nil)

				r := newRunner(lease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return []byte("last"), errors.Join(errors.New("tenant deleted"), worklease.ErrRetire)
				})
				Expect(r.Run(ctx, "w1")).To(Succeed())
			})
		})

		It("passes the backend Checkpoint to WorkFn unchanged", func() {
			expectBackendRun(backend.Checkpoint{State: []byte("cursor-at-42"), PrevExit: backend.ExitAbandoned, PrevHolderID: "w0"})
			mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).Return(nil)

			var got worklease.Checkpoint
			r := newRunner(lease, func(_ context.Context, _ worklease.Token, prior worklease.Checkpoint) ([]byte, error) {
				got = prior
				return nil, nil
			})
			Expect(r.Run(ctx, "w1")).To(Succeed())
			Expect(got).To(Equal(worklease.Checkpoint{State: []byte("cursor-at-42"), PrevExit: worklease.ExitAbandoned, PrevHolderID: "w0"}))
		})
	})

	// ===== PHASE 3: Run — Fencing =====
	Describe("Phase 3: Run — Fencing", func() {
		Context("WorkFn returns ErrFenced, or renewCtx cause is ErrFenced", func() {
			It("does not checkpoint, does not release, returns an error matching ErrFenced (WorkFn returns ErrFenced)", func() {
				expectBackendRun(backend.Checkpoint{})
				mockB.EXPECT().Checkpoint(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockB.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				r := newRunner(lease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return []byte("partial"), worklease.ErrFenced
				})
				Expect(r.Run(ctx, "w1")).To(MatchError(worklease.ErrFenced))
			})

			It("does not checkpoint, does not release, returns an error matching ErrFenced (renewCtx cause is ErrFenced)", func() {
				expectMockRun(worklease.ErrFenced)
				mockLease.EXPECT().Checkpoint(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockLease.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				r := newRunner(mockLease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return []byte("partial"), nil
				})
				Expect(r.Run(ctx, "w1")).To(MatchError(worklease.ErrFenced))
			})
		})

		Context("Release returns ErrFenced", func() {
			It("returns an error matching ErrFenced", func() {
				expectMockRun(nil)
				mockLease.EXPECT().Release(gomock.Any(), token, worklease.ExitFinished).Return(worklease.ErrFenced)

				r := newRunner(mockLease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return nil, nil
				})
				Expect(r.Run(ctx, "w1")).To(MatchError(worklease.ErrFenced))
			})
		})
	})

	// ===== PHASE 4: Run — Failed and Interrupted Exits =====
	Describe("Phase 4: Run — Failed and Interrupted Exits", func() {
		Context("WorkFn returns another error", func() {
			It("checkpoints state, releases with ExitAbandoned, returns the WorkFn error", func() {
				someErr := errors.New("work failed")
				expectBackendRun(backend.Checkpoint{})
				mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("partial"), 30*time.Second).Return(nil)
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitAbandoned).Return(nil)

				r := newRunner(lease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return []byte("partial"), someErr
				})
				Expect(r.Run(ctx, "w1")).To(MatchError(someErr))
			})
		})

		Context("renewCtx cause matches ErrLeaseWindowExhausted", func() {
			It("does not checkpoint or release; returns the WorkFn error when non-nil", func() {
				someErr := errors.New("work interrupted")
				expectMockRun(errors.Join(worklease.ErrLeaseWindowExhausted, worklease.ErrLeaseExpired))
				mockLease.EXPECT().Checkpoint(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockLease.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				r := newRunner(mockLease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return []byte("partial"), someErr
				})
				Expect(r.Run(ctx, "w1")).To(MatchError(someErr))
			})

			It("returns an error matching ErrLeaseWindowExhausted when WorkFn returned nil", func() {
				expectMockRun(worklease.ErrLeaseWindowExhausted)
				mockLease.EXPECT().Checkpoint(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockLease.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				r := newRunner(mockLease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return []byte("partial"), nil
				})
				err := r.Run(ctx, "w1")
				Expect(err).To(MatchError(worklease.ErrLeaseWindowExhausted))
				Expect(err.Error()).To(HavePrefix("worker: "))
			})
		})

		Context("ReadCheckpoint fails", func() {
			It("returns ErrFenced without release when the error is ErrFenced", func() {
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(backend.Checkpoint{}, worklease.ErrFenced)
				mockB.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				fnCalled := false
				r := newRunner(lease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					fnCalled = true
					return nil, nil
				})
				Expect(r.Run(ctx, "w1")).To(MatchError(worklease.ErrFenced))
				Expect(fnCalled).To(BeFalse())
			})

			It("does not release when the error is not ErrFenced, and returns the wrapped error", func() {
				readErr := errors.New("storage unavailable")
				mockB.EXPECT().Acquire(gomock.Any(), "w1", "test-worker", 30*time.Second).Return(record, nil)
				mockB.EXPECT().ReadCheckpoint(gomock.Any(), record).Return(backend.Checkpoint{}, readErr)
				mockB.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				fnCalled := false
				r := newRunner(lease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					fnCalled = true
					return nil, nil
				})
				err := r.Run(ctx, "w1")
				Expect(err).To(MatchError(readErr))
				Expect(err.Error()).To(HavePrefix("worker: read checkpoint: "))
				Expect(fnCalled).To(BeFalse())
			})
		})

		Context("the final Checkpoint fails with a non-fencing error", func() {
			It("releases with ExitAbandoned and returns an error wrapping the checkpoint error with the worker: checkpoint: prefix", func() {
				cpErr := errors.New("disk full")
				expectMockRun(nil)
				mockLease.EXPECT().Checkpoint(gomock.Any(), token, []byte("done")).Return(cpErr)
				mockLease.EXPECT().Release(gomock.Any(), token, worklease.ExitAbandoned).Return(nil)

				r := newRunner(mockLease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return []byte("done"), nil
				})
				err := r.Run(ctx, "w1")
				Expect(err).To(MatchError(cpErr))
				Expect(err.Error()).To(HavePrefix("worker: checkpoint: "))
			})
		})

		Context("Release returns ErrLeaseExpired", func() {
			It("returns an error with the worker: release: prefix matching ErrLeaseExpired when WorkFn returned nil", func() {
				expectMockRun(nil)
				mockLease.EXPECT().Release(gomock.Any(), token, worklease.ExitFinished).Return(worklease.ErrLeaseExpired)

				r := newRunner(mockLease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return nil, nil
				})
				err := r.Run(ctx, "w1")
				Expect(err).To(MatchError(worklease.ErrLeaseExpired))
				Expect(err.Error()).To(HavePrefix("worker: release: "))
			})

			It("returns an error with the worker: release: prefix matching ErrLeaseExpired when WorkFn returned ErrRetire", func() {
				expectMockRun(nil)
				mockLease.EXPECT().Release(gomock.Any(), token, worklease.ExitRetired).Return(worklease.ErrLeaseExpired)

				r := newRunner(mockLease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return nil, worklease.ErrRetire
				})
				err := r.Run(ctx, "w1")
				Expect(err).To(MatchError(worklease.ErrLeaseExpired))
				Expect(err.Error()).To(HavePrefix("worker: release: "))
			})

			It("returns the WorkFn error when WorkFn returned another error", func() {
				someErr := errors.New("work failed")
				expectMockRun(nil)
				mockLease.EXPECT().Release(gomock.Any(), token, worklease.ExitAbandoned).Return(worklease.ErrLeaseExpired)

				r := newRunner(mockLease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					return nil, someErr
				})
				err := r.Run(ctx, "w1")
				Expect(err).To(Equal(someErr))
			})
		})
	})

	// ===== PHASE 5: Run — Cancellation and Cleanup =====
	Describe("Phase 5: Run — Cancellation and Cleanup", func() {
		Context("the caller's context is cancelled while WorkFn runs", func() {
			It("runs the final Checkpoint and Release on a context that is not done", func() {
				parent, cancelParent := context.WithCancel(ctx)
				defer cancelParent()
				expectBackendRun(backend.Checkpoint{})
				mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("final"), 30*time.Second).
					DoAndReturn(func(c context.Context, _ backend.LeaseRecord, _ []byte, _ time.Duration) error {
						return c.Err()
					})
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).
					DoAndReturn(func(c context.Context, _ backend.LeaseRecord, _ backend.ExitMode) error {
						return c.Err()
					})

				r := newRunner(lease, func(wctx context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					cancelParent()
					<-wctx.Done()
					return []byte("final"), nil
				})
				Expect(r.Run(parent, "w1")).To(Succeed())
			})

			It("releases with ExitAbandoned when WorkFn returns the context error", func() {
				parent, cancelParent := context.WithCancel(ctx)
				defer cancelParent()
				expectBackendRun(backend.Checkpoint{})
				mockB.EXPECT().Checkpoint(gomock.Any(), record, []byte("final"), 30*time.Second).Return(nil)
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitAbandoned).
					DoAndReturn(func(c context.Context, _ backend.LeaseRecord, _ backend.ExitMode) error {
						return c.Err()
					})

				r := newRunner(lease, func(wctx context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					cancelParent()
					<-wctx.Done()
					return []byte("final"), wctx.Err()
				})
				Expect(r.Run(parent, "w1")).To(MatchError(context.Canceled))
			})
		})

		Context("CleanupTimeout", func() {
			var deadline time.Time

			captureDeadline := func(c context.Context, _ worklease.Token, _ worklease.ExitMode) error {
				deadline, _ = c.Deadline()
				return nil
			}

			It("uses 5s when zero or negative", func() {
				for _, timeout := range []time.Duration{0, -time.Second} {
					expectMockRun(nil)
					mockLease.EXPECT().Release(gomock.Any(), token, worklease.ExitFinished).DoAndReturn(captureDeadline)

					r, err := worker.NewRunner(worker.RunnerConfig{
						Lease:          mockLease,
						CleanupTimeout: timeout,
						WorkFn: func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
							return nil, nil
						},
					})
					Expect(err).NotTo(HaveOccurred())
					// The parent ctx has a 5s deadline, so derive from a parent without one.
					Expect(r.Run(context.WithoutCancel(ctx), "w1")).To(Succeed())
					Expect(time.Until(deadline)).To(BeNumerically("~", 5*time.Second, 500*time.Millisecond))
				}
			})

			It("bounds the cleanup context deadline by the configured value", func() {
				expectMockRun(nil)
				mockLease.EXPECT().Release(gomock.Any(), token, worklease.ExitFinished).DoAndReturn(captureDeadline)

				r, err := worker.NewRunner(worker.RunnerConfig{
					Lease:          mockLease,
					CleanupTimeout: 2 * time.Second,
					WorkFn: func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
						return nil, nil
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(r.Run(context.WithoutCancel(ctx), "w1")).To(Succeed())
				Expect(time.Until(deadline)).To(BeNumerically("~", 2*time.Second, 500*time.Millisecond))
			})

			It("gives up after CleanupTimeout when the backend blocks", func() {
				expectBackendRun(backend.Checkpoint{})
				mockB.EXPECT().Release(gomock.Any(), record, backend.ExitFinished).
					DoAndReturn(func(c context.Context, _ backend.LeaseRecord, _ backend.ExitMode) error {
						<-c.Done()
						return c.Err()
					})

				r, err := worker.NewRunner(worker.RunnerConfig{
					Lease:          lease,
					CleanupTimeout: 50 * time.Millisecond,
					WorkFn: func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
						return nil, nil
					},
				})
				Expect(err).NotTo(HaveOccurred())

				start := time.Now()
				err = r.Run(ctx, "w1")
				Expect(time.Since(start)).To(BeNumerically("<", time.Second))
				Expect(err).To(MatchError(context.DeadlineExceeded))
				Expect(err.Error()).To(HavePrefix("worker: release: "))
			})
		})
	})

	// ===== PHASE 6: Run — WorkFn Panic =====
	Describe("Phase 6: Run — WorkFn Panic", func() {
		Context("WorkFn panics", func() {
			It("stops renewal, does not release, propagates the panic", func() {
				stopped := false
				mockLease.EXPECT().Acquire(gomock.Any(), "w1").Return(token, nil)
				mockLease.EXPECT().ReadCheckpoint(gomock.Any(), token).Return(worklease.Checkpoint{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), token).Return(ctx, func() { stopped = true })
				mockLease.EXPECT().Checkpoint(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockLease.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				r := newRunner(mockLease, func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					panic("work function failure")
				})
				Expect(func() { _ = r.Run(ctx, "w1") }).To(PanicWith("work function failure"))
				Expect(stopped).To(BeTrue())
			})
		})
	})
})
