// Command observability demonstrates a stdlib-only worklease.LeaseObserver.
//
// It wires a logging observer into a lease backed by the in-memory backend and
// exercises one of each operation — Acquire, Checkpoint, Renew, ReadCheckpoint,
// Release — so every callback fires once. No third-party observability libraries
// are used: the observer emits structured lines via the standard log package.
package main

import (
	"context"
	"log"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend/memory"
)

// logObserver implements worklease.LeaseObserver using only the stdlib log
// package. Each callback emits one structured line. Methods must not block or
// panic — logging satisfies both.
type logObserver struct{ l *log.Logger }

// Compile-time assertion that logObserver satisfies the interface.
var _ worklease.LeaseObserver = logObserver{}

func (o logObserver) OnAcquire(_ context.Context, e worklease.AcquireEvent) {
	o.l.Printf("acquire workID=%q fencingToken=%d dur=%s err=%v",
		e.WorkID, e.Token.FencingToken(), e.Duration, e.Err)
}

func (o logObserver) OnCheckpoint(_ context.Context, e worklease.CheckpointEvent) {
	o.l.Printf("checkpoint workID=%q size=%d dur=%s err=%v",
		e.Token.WorkID(), e.Size, e.Duration, e.Err)
}

func (o logObserver) OnRenew(_ context.Context, e worklease.RenewEvent) {
	o.l.Printf("renew workID=%q dur=%s err=%v", e.Token.WorkID(), e.Duration, e.Err)
}

func (o logObserver) OnRelease(_ context.Context, e worklease.ReleaseEvent) {
	o.l.Printf("release workID=%q dur=%s err=%v", e.Token.WorkID(), e.Duration, e.Err)
}

func (o logObserver) OnReadCheckpoint(_ context.Context, e worklease.ReadCheckpointEvent) {
	o.l.Printf("readCheckpoint workID=%q size=%d cleanHandoff=%t dur=%s err=%v",
		e.Token.WorkID(), e.Size, e.CleanHandoff, e.Duration, e.Err)
}

func (o logObserver) OnFenced(_ context.Context, e worklease.FencedEvent) {
	o.l.Printf("fenced workID=%q operation=%d", e.Token.WorkID(), e.Operation)
}

func main() {
	ctx := context.Background()
	obs := logObserver{l: log.Default()}

	lease, err := worklease.New(memory.New(), worklease.Config{
		TTL:      30 * time.Second,
		HolderID: "observability-example",
		Observer: obs,
	})
	if err != nil {
		log.Fatalf("new lease: %v", err)
	}

	token, err := lease.Acquire(ctx, "report-2026-06")
	if err != nil {
		log.Fatalf("acquire: %v", err)
	}
	if err := lease.Checkpoint(ctx, token, []byte("page=7")); err != nil {
		log.Fatalf("checkpoint: %v", err)
	}
	if err := lease.Renew(ctx, token); err != nil {
		log.Fatalf("renew: %v", err)
	}
	if _, _, err := lease.ReadCheckpoint(ctx, token); err != nil {
		log.Fatalf("read checkpoint: %v", err)
	}
	if err := lease.Release(ctx, token); err != nil {
		log.Fatalf("release: %v", err)
	}
}
