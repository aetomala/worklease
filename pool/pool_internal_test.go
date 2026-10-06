package pool

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("jitterWait", func() {
	It("returns a value in [base, 1.2*base) over 10,000 draws", func() {
		const base = time.Second
		for i := 0; i < 10_000; i++ {
			d := jitterWait(base)
			Expect(d).To(BeNumerically(">=", base))
			Expect(d).To(BeNumerically("<", base+base/5))
		}
	})

	It("returns 0 for a zero base", func() {
		Expect(jitterWait(0)).To(Equal(time.Duration(0)))
	})
})
