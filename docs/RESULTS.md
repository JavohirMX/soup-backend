# Results and verdict

All raw output is in `results/raw/`, and nothing has been removed. Harness debugging runs from
development (on the host, against older code) are kept separately in `results/dev/`. Two of them
matter below.

Environment: a MacBook running Docker Desktop (10 CPUs, 8 GB VM) with Postgres 16. The API, the
workers and the harness all ran on the same machine, so absolute throughput numbers say little about
a real deployment.

## Chaos harness runs

Spec run: 10,000 jobs, 8 workers, a random worker `kill -9`'d every 2 ± 0.5s and respawned. In
parallel:
- 15% of keys re-POSTed as 4 simultaneous requests;
- 15% re-POSTed 0–30s later;
- 5% of first attempts aborted by a 1ms client timeout and retried;
- cancels of queued jobs (5%), running jobs (4%) and finished jobs (1%);
- 2% of jobs designed to fail.

Each run takes about 220s.

| Run | Config | Result | kill -9 / SIGSTOP | Re-executions | Stale writes fenced off |
|---|---|---|---|---|---|
| `chaos-001..005-clean` | spec run, 5 times | **5 × PASS** | 548 / 0 | 448 | 0 |
| `chaos-006,007-clean-sigstop` | 30% of chaos events are an 8s `SIGSTOP` instead of a kill | **2 × PASS** | 187 / 64 | 210 | 5 |
| `chaos-008-repeatable-read` | REPEATABLE READ instead of READ COMMITTED | **PASS** (4 serialization retries in the whole run) | 108 / 0 | 89 | 0 |
| `chaos-009-file-effect` | effect written to a file *before* the DB commit | **FAIL** (expected): 9 jobs with 2 effects, 236 cancelled jobs with an effect | 122 / 0 | 101 | 0 |
| `chaos-010-file-effect-sigstop` | the same, plus SIGSTOP, 3k jobs | **FAIL** (expected): 7 double effects, 40 effects on cancelled jobs | 31 / 15 | 38 | 1 |
| `chaos-011-ctl-no-fencing-killonly` | control, 3k jobs | **FAIL**: 77 broken cancel promises, **but 0 double effects** | 33 / 0 | 26 | n/a |
| `chaos-012-ctl-no-fencing-sigstop` | control, 3k jobs | **FAIL**: 4 double effects, 79 broken cancel promises, 4 non-holder writes | | | |
| `chaos-013-ctl-naive-claim` | control, 3k jobs | **FAIL**: 672 double claims (effects still 1, because the fence saved it) | | | |
| `chaos-014-ctl-check-then-insert` | control, 3k jobs, burst 4 | **FAIL**: 14 keys with 2 jobs | | | |
| `chaos-015-ctl-check-then-insert-burst2` | control, 3k jobs, burst 2 | **FAIL**: 10 keys with 2 jobs | | | |
| `chaos-016-ctl-worker-clock` | control, 3k jobs | **FAIL**: 528 live leases reaped early | | | |
| `results/dev/chaos-004-debug-cti` | control, 800 jobs, burst 2, host | **PASS, so the bug was missed** | | | |

Totals over the 8 passing runs:
- 80,000 jobs
- 843 `kill -9`s and 64 freezes
- 747 re-executions
- 48,760 duplicate POSTs answered `200` with the original id
- 2,852 cancels of running jobs acknowledged with `202`

**Invariants checked on every run** (see `chaos/harness.py: check()`):
1. Every key has exactly one job, and every POST for that key returned that job's id.
2. Nothing is left `queued` or `running`.
3. Each job has at most one effect, a job is `succeeded` if and only if it has exactly one effect,
   and jobs designed to fail never succeed.
4. Every `200`/`202` from cancel ends `cancelled`, and every `409` matches the final state.
5. Replaying each job's event history shows no claim while the job was already held, epochs
   increasing by +1, transitions only by the current holder, and a history that matches the final row.
6. No lease was reaped before it expired by the database clock.

A real `succeeded` job's single effect row also records the epoch that produced it.

### What these runs show, and what they don't

- **`kill -9` alone never creates a zombie.** A killed worker cannot come back, so in the five spec
  runs the fence was used **0 times** (`stale_writes_rejected=0`). With `no_fencing` and kill-only
  (`chaos-011`) there were still **no** double effects. The harness the brief describes cannot
  detect a missing fence. Only freezing (`SIGSTOP`) produces a worker that wakes up after losing its
  lease (5 fenced writes in `chaos-006/007`, and 4 double effects in the control `chaos-012`).
- **The check-then-insert control was caught in 2 of 3 runs.** It needs simultaneous POSTs on the
  same key, and the window is about 1ms. It was missed at 800 jobs with bursts of 2 (dev run) and
  caught at 3,000 jobs with bursts of 2 or 4. The deterministic barrier test in `chaos/silent` catches
  it in 333/333 trials. Random chaos is a poor detector for narrow races.
- **The naive-claim control did not change any outcome-level invariant.** The fence stopped the
  second claimer from committing. Only the event-history replay caught it. An outcome-only harness
  would have certified a double-claiming queue.
- **One SIGSTOP landed inside an open transaction** (`chaos-007`, at 02:44:50 UTC; the Postgres log
  shows `terminating connection due to idle-in-transaction timeout`). Without the
  `idle_in_transaction_session_timeout` added during this work, that job would have been unreapable
  while the worker was frozen (see `docs/SILENT_FAILURES.md` §7). The worker reconnected and the run
  passed.

### Was 10,000 jobs enough?

**It is not the job count that matters, it is how many times each dangerous interleaving happened.**
Across the passing runs there were:
- **747 re-executions after a lost lease.** No duplicate effect was seen. By the rule of three, the
  95% upper bound on the probability that a re-execution produces a duplicate effect is about
  3/747 ≈ **0.4%**.
- **2,852 acknowledged cancels of running jobs**, with no broken cancel promise. The 95% upper bound
  is about **0.1%**.
- **Only 5 zombie writes.** On its own, that is weak evidence for the fence. This is why the fence,
  the cancel race and the claim race also have deterministic barrier tests with 1,000 trials each,
  plus negative controls showing those tests can fail.

More jobs would tighten these bounds slowly. Targeting the interleavings directly gave far more
evidence per minute than scaling the random run.

## Silent-failure scenarios

See `docs/SILENT_FAILURES.md` and the raw output:
- `silent-001`: the full suite
- `silent-002`: the zombie re-run with the fence as the only defence
- `silent-003`: a 5-minute freeze

Result: **0 real bugs, and 0 of 10 negative controls missed.**

## Load (`chaos/load.py`, open-loop, compose stack: 1 API process, 4 workers)

| Run | Offered load | Result |
|---|---|---|
| `load-001-100rps` | 100 RPS for 120s; 50% create, 30% get, 15% list, 5% cancel; 20–80ms jobs | p99 latency: create 6.6ms, get 5.3ms, list 9.7ms, cancel 5.7ms. 0 errors, 0 5xx, at most 15 requests in flight. Queue depth 0–2, drained 0.5s after the load stopped. |
| `load-002-...-longjobs-4workers` | 100 RPS, 50–300ms jobs | API p99 under 10ms, but the **workers fall behind**: about 44 creates/s against about 23 jobs/s of capacity. The backlog grew to 977 and drained 54s after the load stopped. |
| `load-003-1000rps-10x` | 1,000 RPS from **one** generator | **Invalid.** The generator itself saturated a CPU and stalled, so the run was killed and annotated, not deleted. |
| `load-004..006-250rps-10x-*` | 4 generators × 250 RPS (about 1,000 RPS total) | 0 5xx and 8 transport errors (connection resets) in 60k requests; every other request was answered. Latency p50 was about 6s and p99 about 50s. The API process was at about 76% CPU (single Python process), the generators at about 98% CPU each, and the queue grew by about 190 jobs/s. |

`results/raw/load-consistency-check.txt` covers the load database after all load runs: 41,115 jobs,
0 non-terminal, 0 duplicate effects, 0 effects on non-succeeded jobs, 0 duplicate keys.

**What breaks first at 10x.**
- Nothing broke in terms of correctness.
- Latency broke first. The single API process (one Python process, GIL-bound sync handlers) and the
  load generators were both saturated. That blurs exactly where the knee is, so I would put it
  somewhere between about 300 and 700 RPS for this one-process API.
- Independently, worker capacity is simply `workers / mean job duration`, and **nothing pushes back**.
  There is no admission control, so the API accepts jobs faster than they can run, and the queue and
  latency grow without limit.
- The next limits after scaling API processes and workers would be Postgres connections (2 per
  worker plus the API pool) and, much further out, claim contention. `SKIP LOCKED` scans past locked
  rows, so the cost grows with the number of concurrent claimers.

## Verdict: **SHIP** at 100 RPS

The guarantee I claim held at every step:
- **At-least-once execution with exactly-once effect.** It held across 80k jobs under `kill -9` and
  freezes, plus about 8,000 barrier-synchronized race trials against the real code.
- **Every negative control failed.** So the checks can see the bugs they are meant to find.
- **100 RPS uses a small fraction of one API process.** p99 latency was under 10ms.

**This verdict is for the system as built, where the effect commits in the same Postgres transaction
as the completion.** If the production side effect is external (writing model artifacts, calling a
GPU scheduler) and its sink cannot deduplicate on `job_id`, the verdict becomes **DON'T SHIP**.
`chaos-009` shows what that looks like at 10k jobs: 9 duplicate effects, and **236 jobs whose
cancellation was confirmed but whose effect happened anyway**.

**What to fix first, in order:**
1. **Make the external effect idempotent and fenced.** Write it keyed by `job_id`, using
   `(job_id, epoch)` as a conditional-write token, and document that cancelling a running job is
   best-effort for external effects.
2. **Add admission control.** Return `429` when queue depth exceeds what the workers can drain within
   the SLO, and alert on queue depth, reap rate and the `stale_write_rejected` rate.
3. **Tune the lease for real job lengths.** A 5s TTL means any pause over 5s (GC, VM steal, a
   multi-minute training step blocking the heartbeat thread) causes a re-run, which wastes GPU time.
   Use a TTL of about 30–60s and heartbeat every TTL/5.
4. **Operations:**
   - Add retention for `job_events`, which gains about 4 rows per job.
   - Add a per-job attempts limit for crashes too, not only lease expiry: currently a worker that
     deterministically crashes on a job retries it 5 times, which is acceptable.
   - Test Postgres failover (see below).

## What the harness does NOT catch

- **Postgres crash, restart or failover.** All guarantees assume a single, durable primary. If an
  asynchronous replica is promoted, acknowledged creates or completions can be lost, and the clock
  every lease relies on jumps by the clock difference between the two hosts.
- **Network partitions and slow networks** between workers and the database. Only process death and
  freezing were injected, not packet loss, half-open TCP connections or latency spikes.
- **Non-transactional effects in the real system.** Only the file mode emulates them, and it shows
  they break.
- **API process crashes.** The API was never killed. Its correctness relies on the database, and a
  client retry after an API crash is the same as the timeout-retry case, but that is untested.
- **Long-running behaviour:** table and index bloat, autovacuum, `job_events` growth, connection
  churn over days, memory leaks.
- **Poison jobs that crash the worker** (as opposed to failing cleanly). They are bounded by
  `max_attempts`, but there is no test.
- **Multi-host clock skew.** It is irrelevant by design (one DB clock), but the measured offset here
  is about 0 because all containers share one VM kernel clock.
- **Freezes longer than the lease while holding the row lock.** These are handled by the
  idle-transaction timeout, but the harness hit that path only once by chance. The deterministic
  scenario covers it.
- **Real job durations of minutes or hours, and heartbeat starvation.** A CPU-bound job could starve
  the heartbeat thread through the GIL. These jobs only sleep.
- **Load-test fidelity.** It ran on one laptop, the generators were CPU-bound at 10x, and
  single-region latencies are unrealistically low.
