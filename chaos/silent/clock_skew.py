"""Clock skew.

1. Measure the real offset between this process's clock and the database's.
2. Claim with one skewed worker clock, reap immediately with another, and
   count how often a live lease is reaped. With the DB clock (default) the
   skew is ignored; with the worker_clock fault it is not.
"""

from __future__ import annotations

import statistics
import time

from app import db, queue
from app.faults import NONE, Faults
from chaos.common import fresh_database


def observed_offset(samples: int = 50) -> dict:
    url = fresh_database("silent_clock")
    conn = db.connect(url)
    offsets, rtts = [], []
    for _ in range(samples):
        t0 = time.time()
        db_now = conn.execute("SELECT extract(epoch FROM clock_timestamp()) AS t").fetchone()["t"]
        t1 = time.time()
        rtts.append((t1 - t0) * 1000)
        offsets.append((float(db_now) - (t0 + t1) / 2) * 1000)
    conn.close()
    return {"db_minus_local_ms": {"min": round(min(offsets), 2), "median": round(statistics.median(offsets), 2),
                                  "max": round(max(offsets), 2)},
            "rtt_ms_median": round(statistics.median(rtts), 2)}


def premature_reaps(mode: str, claimer_skew: float, reaper_skew: float, ttl: float = 5.0, trials: int = 200) -> dict:
    faults = Faults.parse("worker_clock") if mode == "worker_clock" else NONE
    url = fresh_database("silent_clock")
    a, r = db.connect(url), db.connect(url)
    ca, cr = queue.Clock.for_worker(faults, claimer_skew), queue.Clock.for_worker(faults, reaper_skew)
    premature = 0
    for _ in range(trials):
        a.execute("TRUNCATE jobs, job_events, job_effects")
        queue.create_job(a, "k", {})
        queue.claim(a, "A", ttl, clock=ca)
        # The lease was granted a few ms ago with a 5s TTL: any reap is premature.
        premature += len(queue.reap(r, "R", clock=cr))
    a.close(), r.close()
    return {"mode": mode, "claimer_skew_s": claimer_skew, "reaper_skew_s": reaper_skew, "ttl_s": ttl,
            "trials": trials, "premature_reaps": premature, "violations": premature}
