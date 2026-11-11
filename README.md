# Training-job queue: will it survive production?

This is a job queue with an HTTP API: idempotent create, get, paginated list, and cancel. It runs on
Postgres 16 with 4+ worker processes. Workers claim jobs with `FOR UPDATE SKIP LOCKED`, hold a lease
renewed by heartbeats, and **fence every write with an epoch**. The emulated side effect is a row in
`job_effects`, committed in the same transaction as the job's completion.

**Guarantee: at-least-once execution, exactly-once effect.** The effect is exactly-once only because
it lives in the same database. See [docs/DESIGN.md](docs/DESIGN.md).

**Verdict: SHIP at 100 RPS** for the system as built. It becomes DON'T SHIP if the real side effect
is external and cannot deduplicate on `job_id`. See [docs/RESULTS.md](docs/RESULTS.md).

| Document | Contents |
|---|---|
| [docs/DESIGN.md](docs/DESIGN.md) | One-page design: claims, dead workers, cancellation, the guarantee and why |
| [docs/RESULTS.md](docs/RESULTS.md) | Every chaos and load run, the verdict, what to fix first, and what the harness does NOT catch |
| [docs/SILENT_FAILURES.md](docs/SILENT_FAILURES.md) | How each silent failure was checked, with negative controls |
| [docs/AI_USAGE.md](docs/AI_USAGE.md) | Where AI was used and where it was wrong |
| `results/raw/` | Raw output of every run, including failed and invalid ones |
| `results/dev/` | Harness debugging runs from development, kept because two of them matter |

## Run it

```bash
docker compose up --build          # postgres + api (:8000) + 4 workers
```

```bash
curl -XPOST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"idempotency_key":"train-42","payload":{"duration_ms":500}}'     # 201, or 200 on replay
curl localhost:8000/jobs/<id>
curl 'localhost:8000/jobs?limit=20&state=succeeded'                      # follow next_cursor
curl -XPOST localhost:8000/jobs/<id>/cancel                               # 200 / 202 / 409
```

Status codes:

| Endpoint | Status | Meaning |
|---|---|---|
| `POST /jobs` | `201` | Job created. |
| `POST /jobs` | `200` | Same key and same payload as an earlier request: the original job is returned. |
| `POST /jobs` | `409` | Same key but a different payload. |
| `POST /jobs/{id}/cancel` | `200` | The job is cancelled. |
| `POST /jobs/{id}/cancel` | `202` | The job is running. It is guaranteed to end `cancelled`. |
| `POST /jobs/{id}/cancel` | `409` | The job has already succeeded or failed. |

Pagination is keyset (`created_at`, `id`), so it is stable while new jobs are being inserted.

## Prove it

```bash
# Chaos harness: 10k jobs, 8 workers, kill -9 every ~2s, duplicate POSTs and cancels in parallel.
docker compose --profile chaos run --rm chaos --label clean
docker compose --profile chaos run --rm chaos --label sigstop --stop-frac 0.3      # also freeze workers
docker compose --profile chaos run --rm chaos --label ctl --faults no_fencing --stop-frac 0.3  # must FAIL
./scripts/run_chaos_matrix.sh                                                      # every reported run

# Silent-failure scenarios (barrier races, SIGSTOP zombie, clock skew, isolation levels, frozen transaction)
docker compose --profile chaos run --rm tools python -m chaos.silent.run_all

# Load (needs `docker compose up` running)
docker compose --profile chaos run --rm tools python -m chaos.load --rps 100 --duration 120

# Unit tests on the host (uv), against the compose Postgres on :55432
docker compose up -d postgres && uv sync && uv run pytest
```

Fault flags (`FAULTS=no_fencing,naive_claim,check_then_insert,worker_clock`) re-introduce one bug
each. They exist only to prove that the harness can detect that bug.

## Results summary

- **Real system: 8 out of 8 chaos runs passed** (80,000 jobs).
  - The runs used 843 `kill -9`s and 64 `SIGSTOP` freezes.
  - 747 jobs were re-executed, with 0 duplicate effects and 0 lost jobs.
  - 2,852 cancels of running jobs were acknowledged, and every one was honoured.
- **Every negative control failed, as it should.**
- **There were two blind spots in the harness the brief describes:**
  - `kill -9` never produces a zombie, so it cannot detect a missing fence.
  - Duplicate-on-retry races are about 1ms wide and were missed once.
  - Freezing workers, barrier-synchronized races, and replaying each job's event history close these
    gaps.
- **One real silent failure was found and fixed during the work.** A worker frozen inside a
  transaction makes its job unreapable. It also happened once, by chance, in a chaos run, after the
  fix was in place.
- **100 RPS:** p99 latency under 10ms and no errors. At 10x, correctness held, but latency reached
  tens of seconds. The single API process and the workers saturate, and nothing pushes back.

## Repository layout

```
app/
  queue.py         all SQL: create, claim, heartbeat, finish, reap, cancel, list
  api.py           FastAPI
  worker.py        claim, execute with heartbeat, fenced finish
  schema.sql
  faults.py        negative-control bugs, off by default
chaos/
  harness.py       the 10k / 8 workers / kill -9 harness, with invariant checks
  load.py          open-loop load generator
  silent/          deterministic silent-failure scenarios
tests/             pytest state-machine tests against real Postgres
scripts/run_chaos_matrix.sh
```

## Storage: why Postgres

The question is concurrency between independent worker processes. Postgres provides:
- row locks;
- `SKIP LOCKED`, so workers never wait on each other;
- conditional `UPDATE`s that re-check their `WHERE` clause after taking the lock;
- one authoritative clock;
- access from many hosts.

SQLite serializes all writers behind a single lock. That makes the double-claim question trivial,
and it turns into a throughput ceiling once you have more than a handful of writers. Full reasoning
is in [docs/DESIGN.md](docs/DESIGN.md).

## What surprised me and what still concerns me

The harness the brief describes is not enough to trust this queue. Five clean runs of 10,000 jobs,
with `kill -9` every couple of seconds, all passed, and the fence rejected zero writes in those
runs. A killed process never comes back, so a missing fence looks the same as a working one. I
checked that by turning the fence off (`chaos-011`): still no double effects, only broken cancel
promises. Freezing a worker with `SIGSTOP` is what brings a stale writer back. That is where the
fence shows up, five times in the clean freezes, and as duplicate effects in the control.

A double claim was just as easy to miss. With the fence left on, two workers can both believe they
own a job and only one of them can commit, so the side-effect count stays at one. Replaying each
job's event log is what caught it (`chaos-013`, 672 double claims, still one effect per job). A
green run of the specified harness would have passed both of those broken designs.

The other surprise was a job that is simply stuck, with no error. If a worker stops between the
`UPDATE` and the `COMMIT`, it holds the row lock. The reaper uses `SKIP LOCKED` and walks past it.
The lease is expired, cancel blocks, and nothing is logged. That happened once in `chaos-007`.
Postgres killed the session on `idle_in_transaction_session_timeout`, the worker reconnected, and
the run still passed. Without that timeout the job would have stayed `running` for the whole freeze.

What still concerns me is the effect. Exactly-once is true only while the effect is a row in the
same transaction as the completion. When the effect is a file written before the commit, 9 of
10,000 jobs ran twice, and 236 jobs whose cancel had already been accepted still produced an
effect (`chaos-009`). A real training job writes artifacts outside Postgres. Those sinks have to
dedupe on `job_id`, or this is not safe to ship.

At 100 RPS the API is fine: p99 under 10ms, no errors. At about 1,000 RPS nothing returned 500,
and the queue grew with no limit, because there is no admission control. Postgres failover is also
untested, and every lease depends on that one clock.
