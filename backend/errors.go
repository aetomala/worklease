package backend

import "errors"

// Error message constants for the sentinels in this package. The messages keep
// the worklease: prefix they had in v0.7. ErrFenced's text changed in v0.8
// (ADR-0019, D-18); the other three are unchanged.
const (
	msgFenced          = "worklease: fenced — the lease is no longer held by this token"
	msgLeaseHeld       = "worklease: lease is currently held"
	msgLeaseExpired    = "worklease: lease has expired"
	msgInvalidExitMode = "worklease: invalid exit mode"
)

// Sentinel errors returned by Backend implementations. Package worklease
// re-exports each one as the identical value, so errors.Is matches either name.
var (
	// ErrFenced is returned by Checkpoint, Renew, Release, ReadCheckpoint, and
	// Forget when no row exists for the work ID, and when the stored row no
	// longer matches the record: Checkpoint, Renew, Release, and Forget compare
	// the holder ID and fencing token, and ReadCheckpoint compares the fencing
	// token.
	ErrFenced = errors.New(msgFenced)

	// ErrLeaseHeld is returned by Acquire when the lease for the work ID is held
	// and has not expired.
	ErrLeaseHeld = errors.New(msgLeaseHeld)

	// ErrLeaseExpired is returned by Renew when the lease has already expired,
	// by Release when the lease matches but has already expired, and by
	// Checkpoint only once the holder has declared an exit with Release.
	ErrLeaseExpired = errors.New(msgLeaseExpired)

	// ErrInvalidExitMode is returned by Release when mode is not ExitFinished,
	// ExitAbandoned, or ExitRetired. It is returned before any other check and
	// without side effects.
	ErrInvalidExitMode = errors.New(msgInvalidExitMode)
)
