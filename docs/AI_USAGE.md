# AI usage

The brief allows AI tools and asks where they were used, which suggestions were accepted, and what was
verified, changed or redone. This file has two parts:
- a factual log of what the AI agent (Cursor) produced and where it was wrong;
- what I checked before submitting, and what I am not claiming to have re-run myself.

## What the agent generated

Nearly all of the first drafts:
- the code: `app/`, `chaos/`, `tests/`
- the Docker and Compose setup
- the documentation: `docs/`

The design decisions came out of a planning step before any code was written. These were the choices:
- Postgres instead of SQLite
- lease plus fencing epoch
- DB clock only
- effect committed in the same transaction as the completion
- negative-control fault flags

## Where the agent's output was wrong, and how each problem was caught

Listed because the brief asks where a tool was unreliable. Each one was caught by running something,
not by reading the code.

| # | What was wrong | How it was caught | Fix |
|---|---|---|---|
| 1 | `finish()` passed `None` into a `CASE` producing a `jsonb` column, giving the error "expression is of type text". | The first `pytest` run had 5 failures. | Explicit `::jsonb` cast. |
| 2 | The first version of the harness passed with the `check_then_insert` bug present. With 2 simultaneous POSTs per key, the roughly 1ms race window was not hit in an 800-job run. | The negative-control run *passed* when it should have failed. | Added `--dup-burst` (default 4) and a barrier-based scenario. The miss is kept in `results/dev/chaos-004-debug-cti.txt`. A later 3,000-job run with burst 2 did catch it, so detection is luck-dependent. |
| 2b | The agent's first draft of the docs said the burst-2 control "was not detected" in the final matrix. The final run (`chaos-015`) actually detected it. | Comparing the docs with the raw output after the matrix finished. | Docs corrected. |
| 2c | The first 10x load run used one Python generator, which saturated its own CPU and stalled. Read naively, its numbers blame the API. | `docker stats`: generator at 100% CPU, API idle. | The run was killed, kept, and annotated as invalid, then re-run with 4 generators. Even then the generators were near 100% CPU, which `RESULTS.md` states. |
| 3 | The `naive_claim` control was invisible to the side-effect invariant, because fencing still let only one claimer commit. | Control run: double claims happened, yet effects stayed at 1 per job. | The harness now replays `job_events` per job and flags "claimed while already held". |
| 4 | The isolation scenario counted "finish raised `SerializationFailure`, job still running" as a broken cancel promise. That is a loud, recoverable error, not a silent one. | The results contradicted the design note's argument, so they were re-read. | Classified separately as `finish_raised=...`. |
| 5 | The process-level zombie test never exercised the fence. The heartbeat thread noticed the lost lease first, so the worker abandoned the job before writing. | Reading the raw event log of `silent-001`: no `stale_write_rejected` event. | Added a variant with A's heartbeat disabled, where the fence is the only defence. |
| 6 | The worker caught only `OperationalError`, but the server killing an idle-in-transaction session raises `IdleInTransactionSessionTimeout`, which is a different class. The worker would have crashed. | A traceback in the frozen-transaction scenario. | The worker catches `psycopg.Error` and reconnects. |
| 7 | A partly written function was left in `chaos/silent/races.py`. | Review before running. | Deleted. |

## Things the agent got right that are easy to get wrong

- **Use `sys.executable`, not `uv run`, for worker subprocesses in the harness.** `kill -9` on `uv`
  would leave the Python child orphaned and still processing jobs, and the "kill" would be a no-op.
- **Name the heartbeat thread's stop event `_halt`, not `_stop`.** `threading.Thread` has an internal
  `_stop()` method, and shadowing it breaks `join()`.

## What I checked

Cursor wrote the code, the harness, and the first draft of the docs. I accepted the design it
proposed: Postgres, a lease with a fencing epoch, the database clock only, and the effect committed
in the same transaction as completion. I kept the fault flags, because each one is a bug the
harness is supposed to be able to see.

I did not treat the summary as the result. The claims in `docs/RESULTS.md` and
`docs/SILENT_FAILURES.md` match the files in `results/raw/`:

- The passing evidence is `chaos-001` through `chaos-008`. On the five spec runs the fence rejected
  nothing (`stale_writes_rejected=0`). `chaos-011` removes the fence and still has zero double
  effects under `kill -9` only, which is why the `SIGSTOP` runs are in the repo.
- The failures are still there: file effects (`chaos-009`, `chaos-010`), the fault controls
  (`chaos-011` through `chaos-016`), and the 10x load run that saturated the generator itself
  (`load-003`, marked invalid, not deleted). `results/dev/chaos-004-debug-cti.txt` is the run where
  the harness missed `check_then_insert`.
- The 5-minute freeze is `silent-003`: one effect with the fence, two without it.

I have not re-run the 10,000-job matrix on another machine, and I have not tested Postgres
failover. I would not read the 100 RPS numbers as a production capacity. They come from one
laptop, one API process, and jobs that only sleep. The SHIP verdict is for this process, with the
effect inside Postgres. If the effect is external, the result is the file-effect runs, and those
failed.

The "What surprised me" section in the README is my reading of those same runs. The agent drafted
it, and I rewrote it from the logs above rather than leaving the draft marker in place.
