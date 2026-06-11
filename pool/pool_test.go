package pool_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/pool"
	"github.com/aetomala/worklease/testutil"
)

type testPermError struct{ msg string }

func (e testPermError) Error() string   { return e.msg }
func (e testPermError) Permanent() bool { return true }

var (
	ctx       context.Context
	cancel    context.CancelFunc
	ctrl      *gomock.Controller
	mockLease *testutil.MockLease
)

var _ = Describe("pool", func() {
	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		ctrl = gomock.NewController(GinkgoT())
		mockLease = testutil.NewMockLease(ctrl)
	})

	AfterEach(func() {
		cancel()
		ctrl.Finish()
	})

	Describe("New", func() {
		Context("when lease is nil", func() {
			It("returns ErrConfigInvalid", func() {
				p, err := pool.New(nil, pool.Config{WorkIDs: []string{"w1"}}, func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) { return nil, nil })
				Expect(errors.Is(err, pool.ErrConfigInvalid)).To(BeTrue())
				Expect(p).To(BeNil())
			})
		})
		Context("when cfg.WorkIDs is empty", func() {
			It("returns ErrConfigInvalid", func() {
				p, err := pool.New(mockLease, pool.Config{WorkIDs: []string{}}, func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) { return nil, nil })
				Expect(errors.Is(err, pool.ErrConfigInvalid)).To(BeTrue())
				Expect(p).To(BeNil())
			})
		})
		Context("when cfg.AcquireOptions includes WithWaitForLease", func() {
			It("returns ErrConfigInvalid", func() {
				p, err := pool.New(mockLease, pool.Config{
					WorkIDs:        []string{"w1"},
					AcquireOptions: []worklease.AcquireOption{worklease.WithWaitForLease()},
				}, func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) { return nil, nil })
				Expect(errors.Is(err, pool.ErrConfigInvalid)).To(BeTrue())
				Expect(p).To(BeNil())
			})
		})
		Context("with valid config", func() {
			It("returns a non-nil Pool", func() {
				p, err := pool.New(mockLease, pool.Config{WorkIDs: []string{"w1"}}, func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) { return nil, nil })
				Expect(err).NotTo(HaveOccurred())
				Expect(p).NotTo(BeNil())
			})
			It("does not start any goroutines", func() {
				// Construction completes immediately with no mock calls
				p, err := pool.New(mockLease, pool.Config{WorkIDs: []string{"w1"}}, func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) { return nil, nil })
				Expect(err).NotTo(HaveOccurred())
				Expect(p).NotTo(BeNil())
			})
		})
	})

	Describe("Run", func() {
		Context("when ctx is cancelled before any slot acquires", func() {
			It("returns nil after all goroutines exit", func() {
				mockLease.EXPECT().Acquire(gomock.Any(), "w1").DoAndReturn(
					func(ctx context.Context, _ string, _ ...worklease.AcquireOption) (worklease.Token, error) {
						<-ctx.Done()
						return worklease.Token{}, ctx.Err()
					},
				).AnyTimes()

				lctx, lcancel := context.WithCancel(context.Background())
				fn := func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) { return nil, nil }
				p, _ := pool.New(mockLease, pool.Config{WorkIDs: []string{"w1"}}, fn)

				done := make(chan error, 1)
				go func() { done <- p.Run(lctx) }()
				lcancel()
				Eventually(done).Should(Receive(BeNil()))
			})
		})

		Context("when a slot acquires and WorkFn returns nil", func() {
			It("reacquires the slot at the next loop iteration", func() {
				lctx, lcancel := context.WithCancel(context.Background())
				defer lcancel()

				callCount := 0
				fnCh := make(chan struct{}, 2)
				renewCtx, renewCancel := context.WithCancel(context.Background())
				defer renewCancel()
				stopFn := func() {}

				mockLease.EXPECT().Acquire(gomock.Any(), "w1").Return(worklease.Token{}, nil).Times(2)
				mockLease.EXPECT().ReadCheckpoint(gomock.Any(), gomock.Any()).Return(nil, false, nil).Times(2)
				mockLease.EXPECT().StartRenewal(gomock.Any(), gomock.Any()).Return(renewCtx, stopFn).Times(2)
				mockLease.EXPECT().Release(gomock.Any(), gomock.Any()).Return(nil).Times(2)
				mockLease.EXPECT().Acquire(gomock.Any(), "w1").DoAndReturn(
					func(ctx context.Context, _ string, _ ...worklease.AcquireOption) (worklease.Token, error) {
						<-ctx.Done()
						return worklease.Token{}, ctx.Err()
					},
				).AnyTimes()

				fn := func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					callCount++
					fnCh <- struct{}{}
					return nil, nil
				}
				p, _ := pool.New(mockLease, pool.Config{WorkIDs: []string{"w1"}}, fn)

				done := make(chan error, 1)
				go func() { done <- p.Run(lctx) }()

				Eventually(fnCh).Should(Receive())
				Eventually(fnCh).Should(Receive())
				lcancel()
				Eventually(done).Should(Receive(BeNil()))
				Expect(callCount).To(BeNumerically(">=", 2))
			})
		})

		Context("when runner.Run returns ErrFenced", func() {
			It("reacquires the slot with no backoff", func() {
				lctx, lcancel := context.WithCancel(context.Background())
				defer lcancel()

				renewCtx, renewCancel := context.WithCancel(context.Background())
				defer renewCancel()
				stopFn := func() {}
				iteration := 0
				itCh := make(chan int, 2)

				// Iteration 1: fn returns ErrFenced → r.Run returns ErrFenced (no Release call)
				gomock.InOrder(
					mockLease.EXPECT().Acquire(gomock.Any(), "w1").Return(worklease.Token{}, nil),
					mockLease.EXPECT().ReadCheckpoint(gomock.Any(), gomock.Any()).Return(nil, false, nil),
					mockLease.EXPECT().StartRenewal(gomock.Any(), gomock.Any()).Return(renewCtx, stopFn),
				)
				// Iteration 2: fn returns PermanentError → goroutine exits
				gomock.InOrder(
					mockLease.EXPECT().Acquire(gomock.Any(), "w1").Return(worklease.Token{}, nil),
					mockLease.EXPECT().ReadCheckpoint(gomock.Any(), gomock.Any()).Return(nil, false, nil),
					mockLease.EXPECT().StartRenewal(gomock.Any(), gomock.Any()).Return(renewCtx, stopFn),
					mockLease.EXPECT().Release(gomock.Any(), gomock.Any()).Return(nil),
				)

				fn := func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					iteration++
					itCh <- iteration
					if iteration == 1 {
						return nil, worklease.ErrFenced
					}
					return nil, testPermError{"stop"}
				}
				p, _ := pool.New(mockLease, pool.Config{WorkIDs: []string{"w1"}}, fn)

				done := make(chan error, 1)
				go func() { done <- p.Run(lctx) }()

				Eventually(itCh, "2s").Should(Receive())
				Eventually(itCh, "2s").Should(Receive())
				Eventually(done, "2s").Should(Receive(BeNil()))
			})
		})

		Context("when WorkFn returns a PermanentError", func() {
			It("drops the slot goroutine without reacquiring", func() {
				renewCtx, renewCancel := context.WithCancel(context.Background())
				defer renewCancel()
				stopFn := func() {}

				gomock.InOrder(
					mockLease.EXPECT().Acquire(gomock.Any(), "w1").Return(worklease.Token{}, nil),
					mockLease.EXPECT().ReadCheckpoint(gomock.Any(), gomock.Any()).Return(nil, false, nil),
					mockLease.EXPECT().StartRenewal(gomock.Any(), gomock.Any()).Return(renewCtx, stopFn),
					mockLease.EXPECT().Release(gomock.Any(), gomock.Any()).Return(nil),
				)

				fn := func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					return nil, testPermError{"permanent failure"}
				}
				lctx, lcancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer lcancel()

				p, _ := pool.New(mockLease, pool.Config{WorkIDs: []string{"w1"}}, fn)
				err := p.Run(lctx)
				Expect(err).To(BeNil())
			})
		})

		Context("when WorkFn returns a non-permanent error", func() {
			It("waits BackoffInterval before reacquiring", func() {
				const backoff = 50 * time.Millisecond
				lctx, lcancel := context.WithCancel(context.Background())
				defer lcancel()

				renewCtx, renewCancel := context.WithCancel(context.Background())
				defer renewCancel()
				stopFn := func() {}
				iteration := 0
				times := make([]time.Time, 0, 2)

				gomock.InOrder(
					mockLease.EXPECT().Acquire(gomock.Any(), "w1").Return(worklease.Token{}, nil),
					mockLease.EXPECT().ReadCheckpoint(gomock.Any(), gomock.Any()).Return(nil, false, nil),
					mockLease.EXPECT().StartRenewal(gomock.Any(), gomock.Any()).Return(renewCtx, stopFn),
					mockLease.EXPECT().Release(gomock.Any(), gomock.Any()).Return(nil),
				)
				gomock.InOrder(
					mockLease.EXPECT().Acquire(gomock.Any(), "w1").DoAndReturn(
						func(ctx context.Context, _ string, _ ...worklease.AcquireOption) (worklease.Token, error) {
							times = append(times, time.Now())
							return worklease.Token{}, nil
						},
					),
					mockLease.EXPECT().ReadCheckpoint(gomock.Any(), gomock.Any()).Return(nil, false, nil),
					mockLease.EXPECT().StartRenewal(gomock.Any(), gomock.Any()).Return(renewCtx, stopFn),
					mockLease.EXPECT().Release(gomock.Any(), gomock.Any()).Return(nil),
				)

				workErr := errors.New("transient failure")
				fn := func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					iteration++
					if iteration == 1 {
						times = append(times, time.Now())
						return nil, workErr
					}
					return nil, testPermError{"stop"}
				}
				p, _ := pool.New(mockLease, pool.Config{WorkIDs: []string{"w1"}, BackoffInterval: backoff}, fn)
				done := make(chan error, 1)
				go func() { done <- p.Run(lctx) }()
				Eventually(done, "2s").Should(Receive(BeNil()))

				Expect(times).To(HaveLen(2))
				Expect(times[1].Sub(times[0])).To(BeNumerically(">=", backoff))
			})
		})
	})

	Describe("ActiveSlots", func() {
		Context("returns only slots for which runner.Run is currently executing", func() {
			It("slot appears during fn and disappears after", func() {
				blocked := make(chan struct{})
				release := make(chan struct{})
				renewCtx, renewCancel := context.WithCancel(context.Background())
				defer renewCancel()
				stopFn := func() {}

				mockLease.EXPECT().Acquire(gomock.Any(), "w1").Return(worklease.Token{}, nil).AnyTimes()
				mockLease.EXPECT().ReadCheckpoint(gomock.Any(), gomock.Any()).Return(nil, false, nil).AnyTimes()
				mockLease.EXPECT().StartRenewal(gomock.Any(), gomock.Any()).Return(renewCtx, stopFn).AnyTimes()
				mockLease.EXPECT().Release(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				lctx, lcancel := context.WithCancel(context.Background())
				fn := func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					close(blocked)
					<-release
					return nil, testPermError{"stop"}
				}
				p, _ := pool.New(mockLease, pool.Config{WorkIDs: []string{"w1"}}, fn)
				go p.Run(lctx)

				<-blocked
				Expect(p.ActiveSlots()).To(ConsistOf("w1"))

				close(release)
				lcancel()
			})
		})

		Context("does not include slots in backoff", func() {
			It("slot is absent from ActiveSlots during backoff period", func() {
				blocked := make(chan struct{})
				entered := make(chan struct{})
				const backoff = 200 * time.Millisecond
				renewCtx, renewCancel := context.WithCancel(context.Background())
				defer renewCancel()
				stopFn := func() {}

				iteration := 0
				mockLease.EXPECT().Acquire(gomock.Any(), "w1").DoAndReturn(
					func(ctx context.Context, _ string, _ ...worklease.AcquireOption) (worklease.Token, error) {
						if iteration > 0 {
							close(entered)
							<-ctx.Done()
							return worklease.Token{}, ctx.Err()
						}
						return worklease.Token{}, nil
					},
				).AnyTimes()
				mockLease.EXPECT().ReadCheckpoint(gomock.Any(), gomock.Any()).Return(nil, false, nil).AnyTimes()
				mockLease.EXPECT().StartRenewal(gomock.Any(), gomock.Any()).Return(renewCtx, stopFn).AnyTimes()
				mockLease.EXPECT().Release(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				lctx, lcancel := context.WithCancel(context.Background())
				defer lcancel()

				fn := func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					iteration++
					close(blocked)
					return nil, errors.New("transient")
				}
				p, _ := pool.New(mockLease, pool.Config{WorkIDs: []string{"w1"}, BackoffInterval: backoff}, fn)
				go p.Run(lctx)

				<-blocked
				Eventually(func() []string { return p.ActiveSlots() }, "1s").Should(BeEmpty())
			})
		})

		Context("is safe for concurrent use", func() {
			It("does not trigger the race detector", func() {
				lctx, lcancel := context.WithCancel(context.Background())
				renewCtx, renewCancel := context.WithCancel(context.Background())
				defer renewCancel()
				stopFn := func() {}

				mockLease.EXPECT().Acquire(gomock.Any(), "w1").Return(worklease.Token{}, nil).AnyTimes()
				mockLease.EXPECT().ReadCheckpoint(gomock.Any(), gomock.Any()).Return(nil, false, nil).AnyTimes()
				mockLease.EXPECT().StartRenewal(gomock.Any(), gomock.Any()).Return(renewCtx, stopFn).AnyTimes()
				mockLease.EXPECT().Release(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				p, _ := pool.New(mockLease, pool.Config{WorkIDs: []string{"w1"}}, func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					return nil, nil
				})
				go p.Run(lctx)

				const readers = 10
				results := make(chan []string, readers)
				for i := 0; i < readers; i++ {
					go func() { results <- p.ActiveSlots() }()
				}
				for i := 0; i < readers; i++ {
					<-results
				}
				lcancel()
			})
		})
	})
})
