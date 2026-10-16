package integration_test

import (
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/memory"
	"github.com/aetomala/worklease/backend/postgres"
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

// Integration suite constants: the PostgreSQL schema the suite runs in, the
// canonical DDL it applies, and the environment variables that select and
// require the PostgreSQL run.
const (
	integrationSchema  = "worklease_integration"
	schemaPath         = "../backend/postgres/schema.sql"
	envPostgresDSN     = "WORKLEASE_TEST_POSTGRES_DSN"
	envRequirePostgres = "WORKLEASE_REQUIRE_POSTGRES"
)

// Harness names, used as the top-level Describe titles, and the messages for a
// missing PostgreSQL DSN.
const (
	harnessNameMemory   = "memory backend"
	harnessNamePostgres = "postgres backend"
	msgSkipPostgres     = "WORKLEASE_TEST_POSTGRES_DSN not set — skipping postgres integration specs"
	msgRequirePostgres  = "WORKLEASE_REQUIRE_POSTGRES is set but WORKLEASE_TEST_POSTGRES_DSN is not"
)

// SQL the PostgreSQL harness runs. The search_path of every pooled
// connection is integrationSchema, so none of these touch other schemas.
const (
	queryCreateSchema = "CREATE SCHEMA IF NOT EXISTS " + integrationSchema
	queryResetSchema  = "DROP TABLE IF EXISTS worklease_leases;"
	queryExpireLease  = "UPDATE worklease_leases SET expires_at = NOW() - INTERVAL '1 second' WHERE work_id = $1"
)

// The testEnv type is one spec's backend and a function that expires a held
// lease in it.
type testEnv struct {
	backend backend.Backend
	expire  func(workID string)
}

// The harness type names a backend and builds a fresh testEnv for each spec.
// The optional beforeEach runs before every spec registered for the harness.
type harness struct {
	name       string
	newEnv     func() testEnv
	beforeEach func()
}

// The runIntegration function registers every integration spec against h, in
// a top-level container titled h.name.
func runIntegration(h harness) bool {
	return Describe(h.name, func() {
		if h.beforeEach != nil {
			BeforeEach(h.beforeEach)
		}
		leaderSpecs(h)
		poolSpecs(h)
		workerSpecs(h)
	})
}

var _ = runIntegration(memoryHarness())

var _ = runIntegration(postgresHarness())

// The memoryHarness function returns the harness named "memory backend".
func memoryHarness() harness {
	return harness{
		name: harnessNameMemory,
		newEnv: func() testEnv {
			clk := &offsetClock{}
			return testEnv{
				backend: memory.New(memory.WithClock(clk)),
				expire:  func(string) { clk.advance(time.Hour) },
			}
		},
	}
}

// The postgresHarness function returns the harness named "postgres backend".
// It reads pgDB and schemaSQL when a spec runs, not when it is called.
func postgresHarness() harness {
	return harness{
		name: harnessNamePostgres,
		newEnv: func() testEnv {
			_, err := pgDB.Exec(queryResetSchema)
			Expect(err).NotTo(HaveOccurred())
			_, err = pgDB.Exec(string(schemaSQL))
			Expect(err).NotTo(HaveOccurred())
			b, err := postgres.New(pgDB)
			Expect(err).NotTo(HaveOccurred())
			return testEnv{
				backend: b,
				expire: func(workID string) {
					res, err := pgDB.Exec(queryExpireLease, workID)
					Expect(err).NotTo(HaveOccurred())
					Expect(res.RowsAffected()).To(Equal(int64(1)))
				},
			}
		},
		beforeEach: func() {
			if pgDB == nil {
				Skip(msgSkipPostgres)
			}
		},
	}
}

// Package-level PostgreSQL state, set by BeforeSuite. The pgDB variable is nil
// when no DSN is configured.
var (
	pgDB      *sql.DB
	schemaSQL []byte
)

var _ = BeforeSuite(func() {
	dsn := os.Getenv(envPostgresDSN)
	if dsn == "" {
		if os.Getenv(envRequirePostgres) != "" {
			Fail(msgRequirePostgres)
		}
		return
	}

	admin, err := sql.Open("postgres", dsn)
	Expect(err).NotTo(HaveOccurred())
	Expect(admin.Ping()).To(Succeed())
	_, err = admin.Exec(queryCreateSchema)
	Expect(err).NotTo(HaveOccurred())
	Expect(admin.Close()).To(Succeed())

	dsn2, err := dsnWithSearchPath(dsn)
	Expect(err).NotTo(HaveOccurred())
	pgDB, err = sql.Open("postgres", dsn2)
	Expect(err).NotTo(HaveOccurred())
	Expect(pgDB.Ping()).To(Succeed())

	schemaSQL, err = os.ReadFile(schemaPath)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if pgDB != nil {
		Expect(pgDB.Close()).To(Succeed())
	}
})

// The offsetClock type is a memory.Clock that follows real time plus an
// offset. The advance method moves the offset forward.
type offsetClock struct {
	mu     sync.Mutex
	offset time.Duration
}

func (c *offsetClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.offset)
}

func (c *offsetClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offset += d
}

// The dsnWithSearchPath function returns dsn in key/value form with
// search_path set to integrationSchema.
func dsnWithSearchPath(dsn string) (string, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		kv, err := pq.ParseURL(dsn)
		if err != nil {
			return "", err
		}
		dsn = kv
	}
	return dsn + " search_path=" + integrationSchema, nil
}
