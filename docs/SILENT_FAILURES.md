# Silent failures: how each one was checked

These are failures that produce no error in any log. For each one I used a targeted, deterministic
scenario (`chaos/silent/`, run with `python -m chaos.silent.run_all`) and, where possible, a
**negative control**: the same scenario with the bug deliberately re-introduced through a fault flag
(`app/faults.py`). A check that has never been seen to fail proves nothing, so every control below
has to fail.

The raw output is in [`results/raw/silent-001.txt`](../results/raw/silent-001.txt) (full suite) and
[`results/raw/silent-002.txt`](../results/raw/silent-002.txt) (zombie re-run, which adds the
"heartbeat disabled" variant) and [`results/raw/silent-003.txt`](../results/raw/silent-003.txt)
(5-minute freeze). Overall: **0 real bugs, 0 of 10 controls missed.**

Every scenario except the zombie test uses a `threading.Barrier`, so 2–16 connections fire at the same
instant, repeated hundreds to 1,000 times. The random chaos run hits these windows only occasionally.
Here they are hit every trial.

## 1. Double claim

**Risk.** Two workers both believe they own a job.

**Check.** 8 connections call `claim()` on a single queued job at the same instant, 1,000 times.
- Real claim (`UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED)`): exactly 1 claim in
  1,000/1,000 trials.
- Control `naive_claim` (`SELECT` the id, then `UPDATE ... WHERE id=$1` without re-checking the
  state): **5–8 claims per trial in 1,000/1,000 trials**.

**Detected in the chaos run too.** It is caught there only by the event-replay invariant ("claimed
while already held"), not by the side-effect count. Fencing still allowed only one of the
double-claimers to commit. This defence in depth hides the bug from any invariant that looks only at
outcomes. That is the reason the harness replays the `job_events` history.

## 2. Zombie worker (lease expired, worker comes back and writes a stale result)

**Check.** This uses real processes (`chaos/silent/zombie.py`):
1. Worker A claims a 4s job and is frozen with `SIGSTOP`.
2. Its 2s lease expires, and worker B reaps the job and re-runs it.
3. A is resumed with `SIGCONT`.

The pause is either shorter than B's run (A wakes while B is still running) or longer (A wakes after
B has finished).

| Variant | Result |
|---|---|
| Fenced, normal heartbeat | 1 effect. A's heartbeat returned `lost`, and A abandoned the job. |
| Fenced, **A's heartbeat disabled** | 1 effect. A finished the job and its `finish()` was rejected (`stale_write_rejected` event). This is the variant where the fence is the only defence. |
| Control `no_fencing` | **2 effects** (`epoch=1 by A`, `epoch=2 by B`), for both pause lengths. |

**"A worker froze for 5 minutes and came back"** was run as `--zombie-pause 300` (`silent-003`):
- Fenced: 1 effect.
- `no_fencing`: 2 effects.

The length of the pause does not matter. Once the lease has expired, the epoch has moved on, and
anything A writes matches zero rows. One detail: in the 300s "heartbeat disabled" variant, A's
heartbeat interval (60s) had also elapsed during the freeze. The heartbeat therefore fired on wake and
A abandoned the job before reaching the fence. The fence-only path is the one shown in `silent-002`
(4s and 12s pauses, `stale_write_rejected(e1,A)`).

**In the chaos runs,** the fence rejected 5 zombie writes (`chaos-006/007`, SIGSTOP variants). With
`kill -9` alone it was used 0 times, because a killed worker never comes back.

## 3. Cancel/complete race

**Check: running job.** The worker's `finish(succeeded)` and the API's cancel fire at the same
instant, 1,000 times.
- Result: 810 trials were cancel-wins (job `cancelled`, 0 effects) and 190 were complete-wins
  (cancel returned `409 too_late`, job `succeeded`, 1 effect).
- 0 cases where a 202 from cancel was followed by `succeeded`.
- Control `no_fencing` (finish ignores `cancel_requested`): **667/1,000 acknowledged cancels ended
  `succeeded` with an effect.**

**Check: queued job.** `claim()` races `cancel()`, and if the claim won, the worker then completes
the job.
- Result: 856 trials were cancel-first and 144 were claim-first. Every claim-first job still ended
  `cancelled` with 0 effects, because the cancel flag set under the row lock is honoured by `finish()`.

**Why it holds.** Both sides are single-row `UPDATE`s, so they are serialized by the row lock.
`finish()` evaluates `CASE WHEN cancel_requested` against the row version that won the lock.

## 4. Duplicate on request retry

**Check.** 16 identical `POST`s (same key, same payload) fire at the same instant, 333 times.

| Implementation | Result |
|---|---|
| `INSERT ... ON CONFLICT DO NOTHING` plus unique constraint | 1 row and 1 id returned to all 16 clients, every trial. |
| Control: check-then-insert without the constraint | **15–16 rows per key, every trial.** |
| Check-then-insert with the constraint kept | Correct, but 4,995 `UniqueViolation` errors (HTTP 500s). Not silent, but a bad API. |
| Same key with a *different* payload | 800/800 were rejected (`409`), never merged. |

**Where it gets caught.** In the chaos harness, client timeouts followed by retries, plus 15% of keys
sent as 4 simultaneous POSTs, never produced a duplicate in the real system. **Finding about the
harness:** the race window is about 1ms, so random chaos detects it only by luck.
- With 2 simultaneous POSTs per key, the check-then-insert control *passed* an 800-job dev run (missed,
  `results/dev/chaos-004-debug-cti.txt`).
- It was caught in the 3,000-job runs (`chaos-014`: 14 duplicates; `chaos-015`, burst 2: 10
  duplicates).

That is why the default burst is now 4, and why this barrier scenario exists. It catches the bug in
333/333 trials.

## 5. Transaction isolation level

**Check.** Scenarios 1, 3 and 4 were re-run under READ COMMITTED, REPEATABLE READ and SERIALIZABLE,
first without and then with transaction retry.
- **READ COMMITTED.** 0 violations and 0 errors. It is correct because every transition is a
  single-row conditional `UPDATE`, and Postgres re-evaluates the `WHERE` against the latest committed
  row version after waiting for the lock (EvalPlanQual).
- **REPEATABLE READ and SERIALIZABLE.** Also 0 silent violations, but the same races now surface as
  `40001 SerializationFailure`. Without retry this meant:
  - 87 of 333 double-claim trials,
  - 333 of 333 complete-vs-cancel trials,
  - 1,479 failures in duplicate-create.

  With `with_retry` it is correct and error-free.

**The silent risk is RC with application-level read-then-write.** The `naive_claim` and
`check_then_insert` controls are exactly that, and that is where RC really bites. The codebase avoids
the pattern entirely. Raising the isolation level is not a fix for that pattern unless every caller
retries `40001`.

## 6. Clock skew

- **Design.** Lease expiry is computed and compared only with Postgres `now()`, a single clock.
- **Check.** One connection claims with a skewed "worker clock" and another immediately runs the
  reaper with a different skew, 200 trials per configuration.
  - DB clock (the real design), with skews of −4s/+4s, −1s/+1s and 0/+30s: **0 premature reaps.**
    Skew is irrelevant by construction.
  - Control `worker_clock` with −4s/+4s (difference larger than the 5s TTL): **200/200 live leases
    reaped immediately.**
  - `worker_clock` with −1s/+1s: 0 premature reaps, but the effective lease silently shrinks by 2s.
    Skew close to the TTL is the real danger.
- **In the chaos harness,** the `worker_clock` control fails the "lease reaped before it expired (by
  DB clock)" invariant.
- **Measured offset** between the containers and Postgres is about 0ms, because they share the
  Docker VM kernel clock. This number says nothing about multi-host deployments. What remains: if
  Postgres fails over to a replica whose clock differs, every lease effectively shifts by that
  difference. Nothing here tests that.

## 7. A worker frozen *inside* a transaction (found while writing these tests)

A worker that freezes between its `UPDATE` and its `COMMIT` holds the job's row lock. Its lease
expires, but the reaper uses `SKIP LOCKED` and **silently skips the row**. The job stays `running`
with an expired lease, and the API's cancel blocks on the lock. Nothing errors.

| Variant | Result |
|---|---|
| Without mitigation | Lease expired, nothing reaped, cancel blocked (hit a 1s `lock_timeout`), job `running`. |
| With `idle_in_transaction_session_timeout` (workers set it to the lease TTL) | The server kills the frozen session and the lock is released. The reaper requeues the job, the cancel succeeds in 2ms, and the worker's late `COMMIT` fails (`IdleInTransactionSessionTimeout`). The worker then reconnects. |

The window is a few milliseconds per job, but it **did happen once in the chaos runs**:
- In `chaos-007-clean-sigstop`, Postgres logged
  `terminating connection due to idle-in-transaction timeout` at 02:44:50 UTC.
- The worker logged `server closed the connection unexpectedly` when it woke up, then reconnected.
- The run passed. Without the timeout, that job would have been stuck for the whole freeze.
