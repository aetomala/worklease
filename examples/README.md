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

---

### [Cross-Tenant Migration](cross-tenant-migration/)

A coordinator that migrates tenants one at a time across a shared database. Best for:
- Understanding checkpoint-as-cursor (progress through a collection, not steps in a flow)
- Seeing how crash recovery skips already-processed items rather than re-running from scratch

**Features**:
- For-loop coordination pattern with `StartRenewal` keeping the lease alive across many iterations
- Checkpoint after each tenant — fine-grained recovery cursor
- Zombie fencing mid-batch with fencing token values in output

**Run**:
```bash
cd cross-tenant-migration
go run .
```
