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
					// Simulate fn detecting context cancellation
					return context.Canceled
				})
				Expect(errors.Is(fnErr, context.Canceled)).To(BeTrue())
			})
		})
	})
})
