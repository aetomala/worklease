package checkpoint_test

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease/checkpoint"
)

// fakeCodec is a test-only Codec that returns configurable errors.
type fakeCodec struct {
	encodeErr error
	decodeErr error
}

func (f fakeCodec) Marshal(_ any) ([]byte, error) {
	if f.encodeErr != nil {
		return nil, f.encodeErr
	}
	return []byte("fake"), nil
}

func (f fakeCodec) Unmarshal(_ []byte, _ any) error {
	return f.decodeErr
}

type progress struct {
	Step  int    `json:"step"`
	Label string `json:"label"`
}

var _ = Describe("Codec", func() {

	// ===== PHASE 1: Constructor =====
	Describe("Phase 1: Constructor", func() {
		It("JSON() returns a value that satisfies the Codec interface", func() {
			var c checkpoint.Codec = checkpoint.JSON()
			Expect(c).NotTo(BeNil())
		})
	})

	// ===== PHASE 2: Encode =====
	Describe("Phase 2: Encode", func() {
		It("Encode[T] with JSONCodec produces valid JSON bytes", func() {
			p := progress{Step: 3, Label: "archive"}
			data, err := checkpoint.Encode(checkpoint.JSON(), p)
			Expect(err).NotTo(HaveOccurred())
			Expect(data).To(ContainSubstring(`"step":3`))
			Expect(data).To(ContainSubstring(`"label":"archive"`))
		})

		It("Encode[T] propagates codec error", func() {
			encErr := errors.New("encode error")
			_, err := checkpoint.Encode(fakeCodec{encodeErr: encErr}, progress{})
			Expect(errors.Is(err, encErr)).To(BeTrue())
		})
	})

	// ===== PHASE 3: Decode =====
	Describe("Phase 3: Decode", func() {
		It("Decode[T] with JSONCodec populates the target value correctly", func() {
			data := []byte(`{"step":7,"label":"done"}`)
			got, err := checkpoint.Decode[progress](checkpoint.JSON(), data)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Step).To(Equal(7))
			Expect(got.Label).To(Equal("done"))
		})

		It("Decode[T] on nil data returns zero value, no error", func() {
			got, err := checkpoint.Decode[progress](checkpoint.JSON(), nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(progress{}))
		})

		It("Decode[T] propagates codec error", func() {
			decErr := errors.New("decode error")
			_, err := checkpoint.Decode[progress](fakeCodec{decodeErr: decErr}, []byte("x"))
			Expect(errors.Is(err, decErr)).To(BeTrue())
		})
	})

	// ===== PHASE 4: Round-trip =====
	Describe("Phase 4: Round-trip", func() {
		It("Encode[T] then Decode[T] with JSONCodec returns the original value", func() {
			original := progress{Step: 12, Label: "billing"}
			data, err := checkpoint.Encode(checkpoint.JSON(), original)
			Expect(err).NotTo(HaveOccurred())

			got, err := checkpoint.Decode[progress](checkpoint.JSON(), data)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(original))
		})
	})
})
