// Package worklease provides lease-based work coordination for distributed systems.
//
// worklease solves a different problem than distributed locking. Distributed locks
// answer: who owns this resource right now? worklease answers: when ownership changes,
// what does the new owner need to continue the work the previous owner started?
//
// The library is built around three primitives:
//
//   - A Lease — a time-limited, named claim on a unit of work.
//   - A fencing token — a monotonically incrementing integer that prevents zombie
//     writes from a worker that was slow, not dead.
//   - A checkpoint — progress state written atomically with lease renewal, so the
//     last known state survives to the next owner.
//
// # Entry point
//
// Construct a Lease with [New], backed by a [Backend]:
//
//	backend, err := postgres.New(db)
//	lease, err := worklease.New(backend, worklease.Config{
//	    TTL:      30 * time.Second,
//	    HolderID: workerID,
//	})
//
// See the [Lease] interface for the full API.
//
// # What worklease is not
//
// worklease is not a distributed lock library, a workflow engine, a leader election
// library, or a general-purpose job queue. For mutual exclusion alone, use distlock
// or pglock. For durable workflow execution, use Temporal. For leader election, use
// client-go/leaderelection.
//
// # Backends
//
// v0.1 ships two backends:
//
//   - [github.com/aetomala/worklease/backend/postgres] — PostgreSQL-backed, suitable
//     for production use. Requires a running PostgreSQL instance and the worklease
//     schema applied.
//   - [github.com/aetomala/worklease/backend/memory] — in-memory, suitable for
//     unit tests within a single process.
package worklease
