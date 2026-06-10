package checkpoint_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCheckpoint(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Checkpoint Suite")
}
