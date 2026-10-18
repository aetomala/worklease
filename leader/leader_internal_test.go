package leader

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("jitterWait", func() {
	It("returns a value in [base, 1.2*base) over 10,000 draws", func() {
		// The default idle wait is the base Elect uses when IdleInterval <= 0.
		Expect(defaultIdleInterval).To(Equal(time.Second))
		const base = defaultIdleInterval
		lo, hi := 2*base, time.Duration(0)
		for i := 0; i < 10_000; i++ {
			d := jitterWait(base)
			Expect(d).To(BeNumerically(">=", base))
			Expect(d).To(BeNumerically("<", base+base/5))
			lo, hi = min(lo, d), max(hi, d)
		}
		// The draws spread across the range: a constant wait is not jitter.
		Expect(hi - lo).To(BeNumerically(">", base/10))
	})

	It("returns 0 for a zero base", func() {
		Expect(jitterWait(0)).To(Equal(time.Duration(0)))
	})
})
