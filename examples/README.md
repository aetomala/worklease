# worklease — Examples

Runnable examples demonstrating lease-based work coordination with `worklease`.
Each example uses the in-memory backend — no infrastructure required.
Swap in the PostgreSQL backend for production use.

---

## Examples

### [Subscription Cancellation](subscription-cancellation/)

A multi-step SaaS cancellation flow that crashes mid-execution. Best for:
- Understanding what worklease solves that a distributed lock does not
- Seeing crash recovery (`cleanHandoff = false`) and zombie fencing (`ErrFenced`) side by side

**Features**:
- Three sequential scenarios: happy path, crash recovery, zombie fencing
- `StartRenewal` keeping the lease alive across a long-running flow
- Effect-before-checkpoint ordering demonstrated explicitly

**Run**:
```bash
cd subscription-cancellation
go run .
```
