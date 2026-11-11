"""Zombie worker with real processes.

Worker A claims a job, then is frozen with SIGSTOP (a GC pause, a VM steal,
a laptop lid). Its lease expires, worker B reaps and re-runs the job. Then A
is resumed with SIGCONT and, from its point of view, simply finishes the job.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import time

from app import db, queue
from chaos.common import fresh_database


def _spawn(name: str, url: str, faults: str, ttl: float, heartbeat_s: float, log) -> subprocess.Popen:
    env = dict(os.environ, DATABASE_URL=url, FAULTS=faults, WORKER_ID=name, LEASE_TTL_S=str(ttl),
               HEARTBEAT_S=str(heartbeat_s), REAP_INTERVAL_S="0.5", EFFECT_MODE="db")
    return subprocess.Popen([sys.executable, "-m", "app.worker"], env=env, stdout=log, stderr=subprocess.STDOUT)


def _wait(pred, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.05)
    return False


def zombie(pause_s: float = 12.0, faults: str = "", job_ms: int = 4000, ttl: float = 2.0,
           heartbeat_s: float = 0.5) -> dict:
    """heartbeat_s > job duration means A never learns it lost the lease from a
    heartbeat, so the fenced finish is the only thing stopping a second effect."""
    url = fresh_database("silent_zombie")
    conn = db.connect(url)
    job, _ = queue.create_job(conn, "zombie", {"duration_ms": job_ms})
    log = tempfile.NamedTemporaryFile("w+", suffix=".log", delete=False)

    a = _spawn("A", url, faults, ttl, heartbeat_s, log)
    claimed = _wait(lambda: queue.get_job(conn, job["id"])["lease_owner"] == "A", 15)
    time.sleep(0.5)
    os.kill(a.pid, signal.SIGSTOP)
    t_stop = time.monotonic()
    b = _spawn("B", url, faults, ttl, 0.5, log)
    time.sleep(max(0.0, t_stop + pause_s - time.monotonic()))
    state_at_resume = queue.get_job(conn, job["id"])["state"]
    os.kill(a.pid, signal.SIGCONT)
    _wait(lambda: queue.get_job(conn, job["id"])["state"] in queue.TERMINAL, job_ms / 1000 + 10)
    time.sleep(2)  # let A finish whatever it was doing after waking up
    for p in (a, b):
        p.terminate()
        p.wait(10)

    final = queue.get_job(conn, job["id"])
    effects = conn.execute("SELECT epoch, worker_id FROM job_effects WHERE job_id=%s ORDER BY id", (job["id"],)).fetchall()
    events = conn.execute("SELECT kind, epoch, worker_id FROM job_events WHERE job_id=%s ORDER BY id", (job["id"],)).fetchall()
    conn.close()
    log.seek(0)
    interesting = [l.strip().split(" worker ", 1)[-1] for l in log
                   if any(w in l for w in ("stale", "lost lease", "reaped"))]
    return {
        "faults": faults or "none", "pause_s": pause_s, "job_ms": job_ms, "lease_ttl_s": ttl,
        "A_heartbeat_s": heartbeat_s,
        "A_claimed_first": claimed, "state_when_A_resumed": state_at_resume,
        "final_state": final["state"], "final_epoch": final["epoch"],
        "effects": [f"epoch={e['epoch']} by {e['worker_id']}" for e in effects],
        "events": [f"{e['kind']}(e{e['epoch']},{e['worker_id']})" for e in events if e["kind"] != "created"],
        "worker_log": interesting,
        "violations": int(len(effects) != 1 or final["state"] != "succeeded"),
    }
