package postgres_test

import (
	"database/sql"
	_ "embed"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
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

// envPostgresDriver names the environment variable that selects the
// database/sql driver the PostgreSQL specs open: "postgres" for
// github.com/lib/pq (the default when unset) or "pgx" for
// github.com/jackc/pgx/v5/stdlib.
const envPostgresDriver = "WORKLEASE_TEST_POSTGRES_DRIVER"

// The postgresDriver function returns the driver name from envPostgresDriver,
// "postgres" when it is unset, and fails the suite on any other value.
func postgresDriver() string {
	switch d := os.Getenv(envPostgresDriver); d {
	case "", "postgres":
		return "postgres"
	case "pgx":
		return "pgx"
	default:
		Fail(envPostgresDriver + " must be postgres or pgx, got " + d)
		return ""
	}
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
	db, err = sql.Open(postgresDriver(), dsn)
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
