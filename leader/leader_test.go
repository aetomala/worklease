package leader_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/leader"
	"github.com/aetomala/worklease/testutil"
)

var (
	ctx       context.Context
	cancel    context.CancelFunc
	ctrl      *gomock.Controller
	mockLease *testutil.MockLease
)

var _ = Describe("leader", func() {
	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		ctrl = gomock.NewController(GinkgoT())
		mockLease = testutil.NewMockLease(ctrl)
	})

	AfterEach(func() {
		cancel()
		ctrl.Finish()
	})

	Describe("Elect", func() {
		Context("when lease is nil", func() {
			It("returns ErrLeaseRequired without calling Acquire", func() {
				err := leader.Elect(ctx, nil, "work-1", leader.Config{}, func(ctx context.Context) error {
					return nil
				})
				Expect(errors.Is(err, leader.ErrLeaseRequired)).To(BeTrue())
			})
		})

		Context("when Acquire returns ErrLeaseHeld", func() {
			It("returns ErrLeaseHeld without calling fn", func() {
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, worklease.ErrLeaseHeld)
				fnCalled := false
				err := leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(ctx context.Context) error {
					fnCalled = true
					return nil
				})
				Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeTrue())
				Expect(fnCalled).To(BeFalse())
			})
		})

		Context("when Acquire succeeds and fn returns nil", func() {
			It("calls fn, calls stopRenewal, calls Release, and returns nil", func() {
				stopCalled := false
				stopFn := func() { stopCalled = true }
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(nil)

				fnCtx := context.Background()
				err := leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(c context.Context) error {
					fnCtx = c
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(stopCalled).To(BeTrue())
				Expect(fnCtx).To(Equal(renewCtx))
			})
		})

		Context("when fn returns worklease.ErrFenced", func() {
			It("returns worklease.ErrFenced and does not call Release", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				// Release must NOT be called — no EXPECT().Release(...)

				err := leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(ctx context.Context) error {
					return worklease.ErrFenced
				})
				Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
			})
		})

		Context("when fn returns a non-fencing error", func() {
			It("releases with ExitAbandoned, does not call OnRelinquished, and returns fn's error", func() {
				fnErr := errors.New("work failed")
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitAbandoned).Return(nil)

				relinquished := false
				cfg := leader.Config{OnRelinquished: func(_ context.Context, _ worklease.Token) { relinquished = true }}
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(ctx context.Context) error {
					return fnErr
				})
				Expect(errors.Is(err, fnErr)).To(BeTrue())
				Expect(relinquished).To(BeFalse())
			})
		})

		Context("when Release returns ErrFenced", func() {
			It("returns worklease.ErrFenced", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(worklease.ErrFenced)

				err := leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(ctx context.Context) error {
					return nil
				})
				Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
			})
		})

		Context("when cfg.AcquireOptions includes WithWaitForLease", func() {
			It("passes the option through to Acquire", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1", gomock.Any()).Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(nil)

				cfg := leader.Config{
					AcquireOptions: []worklease.AcquireOption{worklease.WithWaitForLease()},
				}
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(ctx context.Context) error {
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when fn returns context.Canceled", func() {
			It("releases with ExitAbandoned and returns the fn error", func() {
				innerCtx, innerCancel := context.WithCancel(context.Background())
				stopFn := func() { innerCancel() }

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(innerCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitAbandoned).Return(nil)

				fnErr := leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(renewCtx context.Context) error {
					return context.Canceled
				})
				Expect(errors.Is(fnErr, context.Canceled)).To(BeTrue())
			})
		})

		Context("when BackoffInterval is positive", func() {
			It("sleeps for BackoffInterval before returning on a clean fn exit", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(nil)

				cfg := leader.Config{BackoffInterval: 50 * time.Millisecond}
				start := time.Now()
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error {
					return nil
				})
				elapsed := time.Since(start)

				Expect(err).NotTo(HaveOccurred())
				Expect(elapsed).To(BeNumerically(">=", 40*time.Millisecond))
			})

			It("sleeps BackoffInterval after ExitAbandoned", func() {
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, func() {})
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitAbandoned).Return(nil)

				cfg := leader.Config{BackoffInterval: 50 * time.Millisecond}
				start := time.Now()
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error {
					return errors.New("work failed")
				})
				Expect(err).To(HaveOccurred())
				Expect(time.Since(start)).To(BeNumerically(">=", 40*time.Millisecond))
			})

			It("bypasses the sleep when fn returns ErrFenced", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)

				cfg := leader.Config{BackoffInterval: 5 * time.Second}
				start := time.Now()
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error {
					return worklease.ErrFenced
				})
				elapsed := time.Since(start)

				Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				Expect(elapsed).To(BeNumerically("<", time.Second))
			})

			It("bypasses the sleep when Release returns ErrFenced", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(worklease.ErrFenced)

				cfg := leader.Config{BackoffInterval: 5 * time.Second}
				start := time.Now()
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error {
					return nil
				})
				elapsed := time.Since(start)

				Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				Expect(elapsed).To(BeNumerically("<", time.Second))
			})
		})

		Context("when fn returns an error wrapping ErrRetire", func() {
			It("releases with ExitRetired, calls OnRelinquished, and returns nil", func() {
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, func() {})
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitRetired).Return(nil)

				relinquished := false
				cfg := leader.Config{OnRelinquished: func(_ context.Context, _ worklease.Token) { relinquished = true }}
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error {
					return errors.Join(errors.New("job decommissioned"), worklease.ErrRetire)
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(relinquished).To(BeTrue())
			})
		})

		Context("when the renewal context is cancelled with cause ErrFenced", func() {
			It("does not release and returns ErrFenced, without sleeping BackoffInterval", func() {
				renewCtx, renewCancel := context.WithCancelCause(ctx)
				renewCancel(worklease.ErrFenced)
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, func() {})
				mockLease.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				cfg := leader.Config{BackoffInterval: 5 * time.Second}
				start := time.Now()
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error { return nil })
				Expect(err).To(MatchError(worklease.ErrFenced))
				Expect(time.Since(start)).To(BeNumerically("<", time.Second))
			})
		})

		Context("when the lease window is exhausted", func() {
			It("does not release and returns fn's error", func() {
				fnErr := errors.New("work interrupted")
				renewCtx, renewCancel := context.WithCancelCause(ctx)
				renewCancel(errors.Join(worklease.ErrLeaseWindowExhausted, worklease.ErrLeaseExpired))
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, func() {})
				mockLease.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				err := leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(context.Context) error { return fnErr })
				Expect(err).To(Equal(fnErr))
			})

			It("does not release and returns the cause when fn returned nil", func() {
				renewCtx, renewCancel := context.WithCancelCause(ctx)
				renewCancel(worklease.ErrLeaseWindowExhausted)
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, func() {})
				mockLease.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				err := leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(context.Context) error { return nil })
				Expect(err).To(MatchError(worklease.ErrLeaseWindowExhausted))
				Expect(err.Error()).To(HavePrefix("leader: "))
			})
		})

		Context("when Release fails with a non-fencing error after fn returned nil", func() {
			It("returns an error with the leader: release: prefix", func() {
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, func() {})
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(worklease.ErrLeaseExpired)

				err := leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(context.Context) error { return nil })
				Expect(err).To(MatchError(worklease.ErrLeaseExpired))
				Expect(err.Error()).To(HavePrefix("leader: release: "))
			})
		})

		Context("cleanup context", func() {
			It("releases on a context that is not done when the parent context is cancelled", func() {
				parentCtx, parentCancel := context.WithCancel(ctx)
				defer parentCancel()
				renewCtx, renewCancel := context.WithCancel(parentCtx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, func() {})
				var releaseCtxErr error
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitAbandoned).
					DoAndReturn(func(c context.Context, _ worklease.Token, _ worklease.ExitMode) error {
						releaseCtxErr = c.Err()
						return releaseCtxErr
					})

				err := leader.Elect(parentCtx, mockLease, "work-1", leader.Config{}, func(c context.Context) error {
					parentCancel()
					<-c.Done()
					return c.Err()
				})
				Expect(err).To(MatchError(context.Canceled))
				Expect(releaseCtxErr).NotTo(HaveOccurred())
			})

			It("uses a 5s cleanup bound when CleanupTimeout is zero or negative", func() {
				for _, timeout := range []time.Duration{0, -time.Second} {
					renewCtx, renewCancel := context.WithCancel(ctx)
					var deadline time.Time
					mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
					mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, func() {})
					mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).
						DoAndReturn(func(c context.Context, _ worklease.Token, _ worklease.ExitMode) error {
							deadline, _ = c.Deadline()
							return nil
						})

					// The parent ctx has a 5s deadline, so elect under a parent without one.
					cfg := leader.Config{CleanupTimeout: timeout}
					err := leader.Elect(context.WithoutCancel(ctx), mockLease, "work-1", cfg, func(context.Context) error { return nil })
					renewCancel()
					Expect(err).NotTo(HaveOccurred())
					Expect(time.Until(deadline)).To(BeNumerically("~", 5*time.Second, 500*time.Millisecond))
				}
			})

			It("gives up after CleanupTimeout when Release blocks", func() {
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, func() {})
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).
					DoAndReturn(func(c context.Context, _ worklease.Token, _ worklease.ExitMode) error {
						<-c.Done()
						return c.Err()
					})

				cfg := leader.Config{CleanupTimeout: 50 * time.Millisecond}
				start := time.Now()
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error { return nil })
				Expect(time.Since(start)).To(BeNumerically("<", time.Second))
				Expect(err).To(MatchError(context.DeadlineExceeded))
				Expect(err.Error()).To(HavePrefix("leader: release: "))
			})
		})

		Context("OnElected callback", func() {
			It("calls OnElected after Acquire succeeds and before fn is invoked", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(nil)

				var order []string
				cfg := leader.Config{OnElected: func(_ context.Context, _ worklease.Token) { order = append(order, "elected") }}
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error {
					order = append(order, "fn")
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(order).To(Equal([]string{"elected", "fn"}))
			})
			It("does not call OnElected when Acquire returns an error", func() {
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, worklease.ErrLeaseHeld)
				called := false
				cfg := leader.Config{OnElected: func(_ context.Context, _ worklease.Token) { called = true }}
				_ = leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error { return nil })
				Expect(called).To(BeFalse())
			})
			It("does not panic when OnElected is nil", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(nil)
				Expect(func() {
					_ = leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(context.Context) error { return nil })
				}).NotTo(Panic())
			})
		})

		Context("OnLost callback", func() {
			It("calls OnLost when renewCtx is cancelled before fn returns", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(nil)

				lost := false
				cfg := leader.Config{OnLost: func(_ context.Context, _ worklease.Token) { lost = true }}
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error {
					renewCancel() // simulate fencing / renewal failure during fn
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(lost).To(BeTrue())
			})
			It("does not call OnLost when the parent context is cancelled", func() {
				parentCtx, parentCancel := context.WithCancel(ctx)
				renewCtx, renewCancel := context.WithCancel(parentCtx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, func() {})
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitAbandoned).Return(nil)

				lost := false
				cfg := leader.Config{OnLost: func(_ context.Context, _ worklease.Token) { lost = true }}
				_ = leader.Elect(parentCtx, mockLease, "work-1", cfg, func(c context.Context) error {
					parentCancel() // caller shutdown — the lease was not lost
					<-c.Done()
					return c.Err()
				})
				Expect(lost).To(BeFalse())
			})
			It("does not call OnLost on a clean fn return", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(nil)

				lost := false
				cfg := leader.Config{OnLost: func(_ context.Context, _ worklease.Token) { lost = true }}
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error { return nil })
				Expect(err).NotTo(HaveOccurred())
				Expect(lost).To(BeFalse())
			})
			It("does not panic when OnLost is nil", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(nil)
				Expect(func() {
					_ = leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(context.Context) error {
						renewCancel()
						return nil
					})
				}).NotTo(Panic())
			})
		})

		Context("OnRelinquished callback", func() {
			It("calls OnRelinquished after Release succeeds on a clean exit", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(nil)

				relinquished := false
				cfg := leader.Config{OnRelinquished: func(_ context.Context, _ worklease.Token) { relinquished = true }}
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error { return nil })
				Expect(err).NotTo(HaveOccurred())
				Expect(relinquished).To(BeTrue())
			})
			It("does not call OnRelinquished when Release returns ErrFenced", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(worklease.ErrFenced)

				relinquished := false
				cfg := leader.Config{OnRelinquished: func(_ context.Context, _ worklease.Token) { relinquished = true }}
				_ = leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error { return nil })
				Expect(relinquished).To(BeFalse())
			})
			It("does not call OnRelinquished when fn returns ErrFenced", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				// Release must NOT be called.

				relinquished := false
				cfg := leader.Config{OnRelinquished: func(_ context.Context, _ worklease.Token) { relinquished = true }}
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error { return worklease.ErrFenced })
				Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				Expect(relinquished).To(BeFalse())
			})
			It("does not panic when OnRelinquished is nil", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}, worklease.ExitFinished).Return(nil)
				Expect(func() {
					_ = leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(context.Context) error { return nil })
				}).NotTo(Panic())
			})
		})
	})
})
