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
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)

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
			It("calls Release and returns the fn error", func() {
				fnErr := errors.New("work failed")
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)

				err := leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(ctx context.Context) error {
					return fnErr
				})
				Expect(errors.Is(err, fnErr)).To(BeTrue())
			})
		})

		Context("when Release returns ErrFenced", func() {
			It("returns worklease.ErrFenced", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(worklease.ErrFenced)

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
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)

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
			It("calls Release and returns the fn error", func() {
				innerCtx, innerCancel := context.WithCancel(context.Background())
				stopFn := func() { innerCancel() }

				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(innerCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)

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
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)

				cfg := leader.Config{BackoffInterval: 50 * time.Millisecond}
				start := time.Now()
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error {
					return nil
				})
				elapsed := time.Since(start)

				Expect(err).NotTo(HaveOccurred())
				Expect(elapsed).To(BeNumerically(">=", 40*time.Millisecond))
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
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(worklease.ErrFenced)

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

		Context("OnElected callback", func() {
			It("calls OnElected after Acquire succeeds and before fn is invoked", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)

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
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)
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
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)

				lost := false
				cfg := leader.Config{OnLost: func(_ context.Context, _ worklease.Token) { lost = true }}
				err := leader.Elect(ctx, mockLease, "work-1", cfg, func(context.Context) error {
					renewCancel() // simulate fencing / renewal failure during fn
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(lost).To(BeTrue())
			})
			It("does not call OnLost on a clean fn return", func() {
				stopFn := func() {}
				renewCtx, renewCancel := context.WithCancel(ctx)
				defer renewCancel()
				mockLease.EXPECT().Acquire(gomock.Any(), "work-1").Return(worklease.Token{}, nil)
				mockLease.EXPECT().StartRenewal(gomock.Any(), worklease.Token{}).Return(renewCtx, stopFn)
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)

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
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)
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
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)

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
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(worklease.ErrFenced)

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
				mockLease.EXPECT().Release(gomock.Any(), worklease.Token{}).Return(nil)
				Expect(func() {
					_ = leader.Elect(ctx, mockLease, "work-1", leader.Config{}, func(context.Context) error { return nil })
				}).NotTo(Panic())
			})
		})
	})
})
