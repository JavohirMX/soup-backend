# Design note (one page)

**Shape.** One FastAPI process (stateless), N worker processes, one Postgres 16. Postgres is the
only source of truth and the only clock. Every state transition is **one conditional `UPDATE` on
the job row** plus an audit row in `job_events`, in the same transaction. There is no
read-decide-write in application code.

**Why Postgres, not SQLite.** The thing under test is concurrency between independent worker
processes. Postgres gives row-level locks, `FOR UPDATE SKIP LOCKED`, and the EvalPlanQual
re-check, which re-evaluates the `WHERE` of a blocked `UPDATE` against the newly committed row.
SQLite has a single database-wide writer lock. A double claim is impossible there only because all
writes are serialized, and the same property becomes a throughput ceiling and `SQLITE_BUSY` storms
with 8+ writer processes. It also cannot be shared by workers on several hosts.

**Claim.**
```sql
UPDATE jobs SET state='running', epoch=epoch+1, attempts=attempts+1,
       lease_owner=$w, lease_expires_at=now()+ttl
WHERE id = (SELECT id FROM jobs WHERE state='queued' ORDER BY created_at
            FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING id, epoch, payload;
```
Workers never wait on each other because locked rows are skipped. The row lock plus
`state='queued'` means a job is claimed by at most one transaction at a time. `epoch` is a
**fencing token**. Every later write by that worker (heartbeat, finish) is
`WHERE id=$1 AND epoch=$2 AND state='running'`, so a worker that lost its lease writes nothing.

**Dead worker detection.** This uses a lease, not a liveness probe. A heartbeat thread on its own
connection extends `lease_expires_at = now() + 5s` every 1s. Every worker runs a reaper every 1s:
expired `running` rows (`FOR UPDATE SKIP LOCKED`) go back to `queued`, to `cancelled` if a cancel
was requested, or to `failed` once `attempts >= max_attempts` (a crude poison-job stop). "Dead" is
indistinguishable from "frozen for longer than the TTL", and that is fine: the fence handles the
frozen worker when it wakes up. Detection latency is TTL plus the reap interval, about 6s. A worker
frozen *inside* a transaction would hold the row lock, and `SKIP LOCKED` would make it unreapable.
Worker sessions therefore set `idle_in_transaction_session_timeout` to the lease TTL, so the server
kills the session and releases the lock.

**Cancelling a running job.** `POST /jobs/{id}/cancel` sets `cancel_requested=true`. A queued job
becomes `cancelled` immediately. A running job keeps running, and the API returns `202`. The worker
learns about the cancel on its next heartbeat (within 1s) and stops. Correctness does not depend
on the worker noticing, though: the finishing `UPDATE` computes
`state = CASE WHEN cancel_requested THEN 'cancelled' ELSE $outcome END` under the row lock, and
the reaper does the same. Cancel and complete are serialized on the row lock. Either the cancel
commits first, and the job ends `cancelled` with no effect, or the completion commits first, and
the cancel gets `409 already succeeded`. **A `202`/`200` from cancel is a promise that the job ends
`cancelled`.** The harness checks that promise.

**Guarantee: at-least-once execution, exactly-once effect.**
- **Execution is at-least-once.** A worker killed mid-job leaves an expired lease, and the job runs
  again. That is unavoidable without knowing whether the dead worker finished: the two-generals
  problem between the worker and the database.
- **The effect is exactly-once** (the `job_effects` row standing in for "the model artifact was
  registered"). It is inserted **in the same transaction as the fenced `running -> succeeded`
  update**, so it commits if and only if this worker still holds the current epoch and the job was
  not cancelled. A retried or zombie worker cannot commit a second one.
- **This holds only because the effect lives in the same database.** For an external effect (S3
  upload, webhook, GPU-cluster submission) the honest guarantee is at-least-once. The external sink
  then has to deduplicate on `(job_id)` or accept `(job_id, epoch)` as an idempotency/fencing key.
  `EFFECT_MODE=file` demonstrates this: it writes the effect to a file before committing, and the
  harness counts duplicates.
- **I chose not to offer at-most-once.** It means "mark done before doing the work", which loses
  jobs on every crash. For training jobs a duplicate run costs GPU time, but a lost run is silently
  missing work.

**Idempotent create.** `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING`, then a
`SELECT` if nothing was returned. The unique constraint is the actual guarantee: application-level
check-then-insert is racy, and the `check_then_insert` fault shows this. Reusing a key with a
different payload returns `409` instead of silently returning the old job.

**Isolation.** READ COMMITTED. Every transition is a single-row conditional `UPDATE`, and Postgres
re-checks the predicate after acquiring the row lock, which is exactly the check we need.
REPEATABLE READ and SERIALIZABLE do not add safety here. They turn the same races into `40001`
errors that must be retried. `db.with_retry` handles that, and `chaos/silent` measures it.

**Clock.** All lease arithmetic uses the database's `now()`. Worker clocks never enter a
correctness decision, so worker clock skew cannot cause premature reaping. The `worker_clock`
fault shows what happens otherwise.

```
queued --claim(epoch+1)--> running --fenced finish--> succeeded | failed | cancelled
   |                         |  \--lease expired (reaper)--> queued | cancelled | failed
   \--cancel--> cancelled    \--cancel_requested + finish/reap--> cancelled
```
