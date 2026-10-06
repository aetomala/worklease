package postgres_test

import (
	"database/sql"
	_ "embed"
	"os"
	"testing"

	_ "github.com/lib/pq"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var db *sql.DB

// schemaSQL is the canonical schema, applied verbatim so the suite cannot drift
// from backend/postgres/schema.sql (ADR-0017).
//
//go:embed schema.sql
var schemaSQL string

func TestPostgres(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Postgres Backend Suite")
}

var _ = BeforeSuite(func() {
	dsn := os.Getenv("WORKLEASE_TEST_POSTGRES_DSN")
	if dsn == "" {
		// CI sets WORKLEASE_REQUIRE_POSTGRES so a missing DSN fails the run
		// instead of passing with zero specs executed.
		if os.Getenv("WORKLEASE_REQUIRE_POSTGRES") != "" {
			Fail("WORKLEASE_REQUIRE_POSTGRES is set but WORKLEASE_TEST_POSTGRES_DSN is not")
		}
		Skip("WORKLEASE_TEST_POSTGRES_DSN not set — skipping postgres integration tests")
	}
	var err error
	db, err = sql.Open("postgres", dsn)
	Expect(err).NotTo(HaveOccurred())
	Expect(db.Ping()).To(Succeed())

	_, err = db.Exec("DROP TABLE IF EXISTS worklease_leases; DROP SEQUENCE IF EXISTS worklease_fencing_seq;")
	Expect(err).NotTo(HaveOccurred())
	_, err = db.Exec(schemaSQL)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if db != nil {
		db.Close()
	}
})
