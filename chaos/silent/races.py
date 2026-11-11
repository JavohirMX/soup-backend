"""Barrier-synchronised races, repeated many times, against a real Postgres.

Each racer has its own connection; all racers block on a barrier and then
fire at the same instant. This maximises the chance of hitting the window
that a random chaos run would only hit occasionally.
"""

from __future__ import annotations

import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from psycopg import errors

from app import db, queue
from app.faults import NONE, Faults
from chaos.common import fresh_database


class Racer:
    def __init__(self, url: str, n: int, isolation: str, retry: bool):
        self.conns = [db.connect(url, isolation) for _ in range(n)]
        self.main = db.connect(url)
        self.pool = ThreadPoolExecutor(n)
        self.retry = retry
        self.raw_errors: Counter = Counter()

    def reset(self) -> None:
        self.main.execute("TRUNCATE jobs, job_events, job_effects")

    def race(self, fns: list[Callable]) -> list:
        barrier = threading.Barrier(len(fns))

        def run(i, fn):
            conn = self.conns[i]
            barrier.wait()
            try:
                if self.retry:
                    return db.with_retry(lambda: fn(conn), attempts=20)
                return fn(conn)
            except (errors.SerializationFailure, errors.DeadlockDetected, errors.UniqueViolation) as e:
                self.raw_errors[type(e).__name__] += 1
                return e
            except Exception as e:  # expected outcomes such as IdempotencyConflict
                return e

        return [f.result() for f in [self.pool.submit(run, i, fn) for i, fn in enumerate(fns)]]

    def effects(self, job_id) -> int:
        return self.main.execute("SELECT count(*) AS n FROM job_effects WHERE job_id = %s",
                                 (job_id,)).fetchone()["n"]

    def close(self) -> None:
        self.pool.shutdown()
        for c in self.conns + [self.main]:
            c.close()


def double_claim(isolation="read_committed", faults: Faults = NONE, trials=1000, claimers=8, retry=True) -> dict:
    url = fresh_database("silent_races")
    r = Racer(url, claimers, isolation, retry)
    winners = Counter()
    for _ in range(trials):
        r.reset()
        queue.create_job(r.main, "k", {})
        res = r.race([lambda c, i=i: queue.claim(c, f"w{i}", 5, faults=faults) for i in range(claimers)])
        winners[sum(1 for x in res if isinstance(x, queue.Claim))] += 1
    r.close()
    violations = sum(n for k, n in winners.items() if k > 1)
    return {"trials": trials, "claims_per_trial": dict(sorted(winners.items())),
            "errors_without_retry": dict(r.raw_errors), "violations": violations}


def _cancel_vs_complete_trial(r: Racer, faults: Faults) -> tuple[str, str, int, str | None]:
    """Job is running; the worker completes at the same instant the API cancels."""
    r.reset()
    queue.create_job(r.main, "k", {})
    c = queue.claim(r.main, "w", 5)
    fin, canc = r.race([
        lambda conn: queue.finish(conn, c.id, c.epoch, "w", "succeeded", faults=faults),
        lambda conn: queue.cancel_job(conn, c.id),
    ])
    cancel_outcome = canc[0] if isinstance(canc, tuple) else type(canc).__name__
    final = queue.get_job(r.main, c.id)["state"]
    n_eff = r.effects(c.id)
    problem = None
    if isinstance(fin, Exception):
        # Loud failure: the worker sees an error and the job is still 'running';
        # its lease will expire and the reaper finishes the story. Not silent.
        cancel_outcome += f" finish_raised={type(fin).__name__}"
        if final != "running" or n_eff:
            problem = f"finish raised but final={final} effects={n_eff}"
    elif cancel_outcome in ("cancelled", "cancel_requested") and (final != "cancelled" or n_eff):
        problem = f"cancel acknowledged ({cancel_outcome}) but final={final} effects={n_eff}"
    elif cancel_outcome == "too_late" and (final != "succeeded" or n_eff != 1):
        problem = f"cancel too_late but final={final} effects={n_eff}"
    elif final == "succeeded" and n_eff != 1 or final != "succeeded" and n_eff != 0:
        problem = f"final={final} effects={n_eff}"
    return cancel_outcome, final, n_eff, problem


def cancel_complete_race(isolation="read_committed", faults: Faults = NONE, trials=1000, retry=True) -> dict:
    url = fresh_database("silent_races")
    r = Racer(url, 2, isolation, retry)
    outcomes, problems = Counter(), []
    for _ in range(trials):
        cancel_outcome, final, _, problem = _cancel_vs_complete_trial(r, faults)
        outcomes[f"cancel={cancel_outcome} final={final}"] += 1
        if problem:
            problems.append(problem)
    r.close()
    return {"trials": trials, "outcomes": dict(outcomes), "errors_without_retry": dict(r.raw_errors),
            "violations": len(problems), "examples": problems[:3]}


def cancel_vs_claim(isolation="read_committed", faults: Faults = NONE, trials=1000, retry=True) -> dict:
    """Job is queued; a worker claims it at the same instant the API cancels it.
    If the claim wins, the worker then tries to complete it."""
    url = fresh_database("silent_races")
    r = Racer(url, 2, isolation, retry)
    outcomes, problems = Counter(), []
    for _ in range(trials):
        r.reset()
        job, _ = queue.create_job(r.main, "k", {})
        cl, canc = r.race([
            lambda conn: queue.claim(conn, "w", 5, faults=faults),
            lambda conn: queue.cancel_job(conn, job["id"]),
        ])
        cancel_outcome = canc[0] if isinstance(canc, tuple) else type(canc).__name__
        if isinstance(cl, queue.Claim):
            queue.finish(r.main, cl.id, cl.epoch, "w", "succeeded", faults=faults)
        final = queue.get_job(r.main, job["id"])["state"]
        n_eff = r.effects(job["id"])
        outcomes[f"claimed={isinstance(cl, queue.Claim)} cancel={cancel_outcome} final={final}"] += 1
        if cancel_outcome in ("cancelled", "cancel_requested") and (final != "cancelled" or n_eff):
            problems.append(f"cancel acknowledged ({cancel_outcome}) but final={final} effects={n_eff}")
    r.close()
    return {"trials": trials, "outcomes": dict(outcomes), "errors_without_retry": dict(r.raw_errors),
            "violations": len(problems), "examples": problems[:3]}


def duplicate_create(isolation="read_committed", faults: Faults = NONE, trials=300, clients=16,
                     retry=True, drop_constraint=False) -> dict:
    """N clients (e.g. a client and its retries) POST the same key at the same instant."""
    url = fresh_database("silent_races", drop_idempotency_constraint=drop_constraint)
    r = Racer(url, clients, isolation, retry)
    rows_per_key, distinct_ids, created_flags = Counter(), Counter(), Counter()
    for _ in range(trials):
        r.reset()
        res = r.race([lambda c: queue.create_job(c, "k", {"x": 1}, faults=faults) for _ in range(clients)])
        ok = [x for x in res if isinstance(x, tuple)]
        distinct_ids[len({j["id"] for j, _ in ok})] += 1
        created_flags[sum(1 for _, created in ok if created)] += 1
        rows_per_key[r.main.execute("SELECT count(*) AS n FROM jobs WHERE idempotency_key='k'").fetchone()["n"]] += 1
    r.close()
    violations = sum(n for k, n in rows_per_key.items() if k != 1) + sum(n for k, n in distinct_ids.items() if k > 1)
    return {"trials": trials, "clients": clients, "rows_per_key": dict(sorted(rows_per_key.items())),
            "distinct_ids_returned": dict(sorted(distinct_ids.items())),
            "created_true_count": dict(sorted(created_flags.items())),
            "errors_without_retry": dict(r.raw_errors), "violations": violations}


def payload_mismatch(trials=100, clients=8) -> dict:
    """Same key, different payload, concurrently: must be rejected, never silently merged."""
    url = fresh_database("silent_races")
    r = Racer(url, clients, "read_committed", True)
    results = Counter()
    for _ in range(trials):
        r.reset()
        queue.create_job(r.main, "k", {"v": "original"})
        res = r.race([lambda c, i=i: queue.create_job(c, "k", {"v": i}) for i in range(clients)])
        for x in res:
            results[type(x).__name__] += 1
    r.close()
    return {"trials": trials, "results": dict(results),
            "violations": sum(n for k, n in results.items() if k != "IdempotencyConflict")}
