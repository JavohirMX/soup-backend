"""Run every silent-failure scenario, including the negative controls.

    docker compose --profile chaos run --rm tools python -m chaos.silent.run_all
    uv run python -m chaos.silent.run_all --only zombie --zombie-pause 300

Each scenario is labelled with what we expect:
  expect-0   the real design; any violation is a bug        -> OK / BUG
  control    a deliberately injected fault; must be caught  -> DETECTED / MISSED
"""

from __future__ import annotations

import argparse
import json
import time

from app.faults import Faults
from chaos.common import RAW_DIR, Tee, code_fingerprint, next_run_number, utc_stamp
from chaos.silent import clock_skew, frozen_txn, races, zombie


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--only", default="", help="comma list: claim,zombie,cancel,dup,isolation,clock,frozen")
    p.add_argument("--zombie-pause", type=float, default=None, help="run only this pause length for zombie")
    p.add_argument("--trials", type=int, default=1000)
    args = p.parse_args()
    only = set(filter(None, args.only.split(",")))
    want = lambda name: not only or name in only  # noqa: E731

    n = next_run_number("silent")
    out = Tee(RAW_DIR / f"silent-{n:03d}.txt")
    out(f"== silent-{n:03d}  {utc_stamp()}  code={code_fingerprint()}")
    totals = {"bugs": 0, "missed": 0}

    def report(title: str, expect: str, result: dict) -> None:
        v = result.get("violations", 0)
        if expect == "expect-0":
            verdict = "OK" if v == 0 else "BUG"
            totals["bugs"] += v != 0
        else:
            verdict = "DETECTED" if v > 0 else "MISSED"
            totals["missed"] += v == 0
        out(f"[{verdict:8s}] ({expect}) {title}")
        out("           " + json.dumps(result, default=str))

    def timed(fn, *a, **kw):
        t0 = time.monotonic()
        r = fn(*a, **kw)
        r["seconds"] = round(time.monotonic() - t0, 1)
        return r

    T = args.trials
    nf, naive, cti = Faults.parse("no_fencing"), Faults.parse("naive_claim"), Faults.parse("check_then_insert")

    if want("claim"):
        out("\n-- 1. double claim: 8 workers claim the same single queued job at the same instant")
        report("SKIP LOCKED conditional claim", "expect-0", timed(races.double_claim, trials=T))
        report("naive SELECT-then-UPDATE claim", "control", timed(races.double_claim, faults=naive, trials=T))

    if want("zombie"):
        out("\n-- 2. zombie: worker SIGSTOPped past its lease, job re-run by another worker, then SIGCONT")
        pauses = [args.zombie_pause] if args.zombie_pause else [12.0, 4.0]
        for pause in pauses:
            report(f"fenced, pause={pause}s", "expect-0", timed(zombie.zombie, pause_s=pause))
        for pause in pauses:
            report(f"fenced, A's heartbeat disabled (fence is the only defence), pause={pause}s", "expect-0",
                   timed(zombie.zombie, pause_s=pause, heartbeat_s=60))
        for pause in pauses:
            report(f"no_fencing, pause={pause}s", "control", timed(zombie.zombie, pause_s=pause, faults="no_fencing"))

    if want("cancel"):
        out("\n-- 3. cancel/complete race")
        report("running job: complete vs cancel", "expect-0", timed(races.cancel_complete_race, trials=T))
        report("running job: complete vs cancel, no_fencing", "control",
               timed(races.cancel_complete_race, faults=nf, trials=T))
        report("queued job: claim(+complete) vs cancel", "expect-0", timed(races.cancel_vs_claim, trials=T))

    if want("dup"):
        out("\n-- 4. duplicate on request retry: 16 identical POSTs (same key) at the same instant")
        report("INSERT ... ON CONFLICT DO NOTHING + unique constraint", "expect-0",
               timed(races.duplicate_create, trials=T // 3))
        report("check-then-insert, no unique constraint", "control",
               timed(races.duplicate_create, faults=cti, drop_constraint=True, trials=T // 3))
        r = timed(races.duplicate_create, faults=cti, retry=False, trials=T // 3)
        r["note"] = "constraint present: race is not silent, it surfaces as UniqueViolation (HTTP 500)"
        report("check-then-insert, unique constraint kept", "expect-0", r)
        report("same key, different payload (must be 409, never merged)", "expect-0",
               timed(races.payload_mismatch, trials=T // 10))

    if want("isolation"):
        out("\n-- 5. isolation level: same races under each level, first WITHOUT retry, then with retry")
        for level in ("read_committed", "repeatable_read", "serializable"):
            for retry in (False, True):
                tag = f"{level}, retry={retry}"
                report(f"double claim [{tag}]", "expect-0",
                       timed(races.double_claim, isolation=level, retry=retry, trials=T // 3))
                report(f"complete vs cancel [{tag}]", "expect-0",
                       timed(races.cancel_complete_race, isolation=level, retry=retry, trials=T // 3))
                report(f"duplicate create [{tag}]", "expect-0",
                       timed(races.duplicate_create, isolation=level, retry=retry, trials=T // 10))

    if want("clock"):
        out("\n-- 6. clock skew")
        out("           observed offset, this process vs Postgres: " + json.dumps(clock_skew.observed_offset()))
        for c_skew, r_skew in ((-4, 4), (-1, 1), (0, 30)):
            report(f"DB clock, worker skews {c_skew:+}s/{r_skew:+}s", "expect-0",
                   timed(clock_skew.premature_reaps, "db_clock", c_skew, r_skew))
        report("worker clock, skews -4s/+4s (|diff| > TTL)", "control",
               timed(clock_skew.premature_reaps, "worker_clock", -4, 4))
        r = timed(clock_skew.premature_reaps, "worker_clock", -1, 1)
        r["note"] = "skew diff < TTL: no premature reap, but the effective lease silently shrinks by 2s"
        report("worker clock, skews -1s/+1s (|diff| < TTL)", "expect-0", r)

    if want("frozen"):
        out("\n-- 7. worker frozen inside an open transaction (holding the row lock)")
        report("no idle_in_transaction_session_timeout", "control", timed(frozen_txn.frozen_in_transaction, None))
        report("idle_in_transaction_session_timeout=2000ms (workers set it to the lease TTL)", "expect-0",
               timed(frozen_txn.frozen_in_transaction, 2000))

    out(f"\n== real bugs found: {totals['bugs']}   negative controls missed: {totals['missed']}")
    out.close()


if __name__ == "__main__":
    main()
