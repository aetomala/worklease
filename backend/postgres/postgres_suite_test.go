package postgres_test

import (
	"database/sql"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/lib/pq"
)

var db *sql.DB

func TestPostgres(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Postgres Backend Suite")
}

var _ = BeforeSuite(func() {
	dsn := os.Getenv("WORKLEASE_TEST_POSTGRES_DSN")
	if dsn == "" {
		Skip("WORKLEASE_TEST_POSTGRES_DSN not set — skipping postgres integration tests")
	}
	var err error
	db, err = sql.Open("postgres", dsn)
	Expect(err).NotTo(HaveOccurred())
	Expect(db.Ping()).To(Succeed())

	_, err = db.Exec(`
		DROP TABLE IF EXISTS worklease_leases;
		CREATE TABLE worklease_leases (
			work_id         TEXT PRIMARY KEY,
			holder_id       TEXT        NOT NULL,
			fencing_token   BIGINT      NOT NULL DEFAULT 1,
			expires_at      TIMESTAMPTZ NOT NULL,
			checkpoint      BYTEA,
			clean_handoff   BOOLEAN     NOT NULL DEFAULT FALSE,
			acquired_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);`)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if db != nil {
		db.Close()
	}
})
