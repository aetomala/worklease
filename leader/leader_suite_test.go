package leader_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLeader(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Leader Suite")
}
