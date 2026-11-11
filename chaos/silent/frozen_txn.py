"""A worker that freezes *inside* an open transaction.

It holds the job's row lock. The lease expires, but the reaper uses
SKIP LOCKED and silently skips the row: the job is stuck in 'running'
with an expired lease, and a cancel blocks on the lock. Nothing errors.
The mitigation is idle_in_transaction_session_timeout on worker sessions.
"""

from __future__ import annotations

import time

import psycopg

from app import db, queue
from chaos.common import fresh_database


def frozen_in_transaction(idle_timeout_ms: int | None, freeze_s: float = 3.0) -> dict:
    url = fresh_database("silent_frozen")
    main = db.connect(url)
    queue.create_job(main, "f", {})
    kwargs = {"options": f"-c idle_in_transaction_session_timeout={idle_timeout_ms}"} if idle_timeout_ms else {}
    w = db.connect(url, **kwargs)
    c = queue.claim(w, "frozen", lease_ttl_s=1.0)

    # The worker starts its finish transaction, takes the row lock, then freezes.
    tx = w.transaction()
    tx.__enter__()
    w.execute("UPDATE jobs SET updated_at = now() WHERE id = %s", (c.id,))
    time.sleep(freeze_s)  # lease (1s) has long expired

    lease_expired = main.execute(
        "SELECT lease_expires_at < now() AS e FROM jobs WHERE id=%s", (c.id,)).fetchone()["e"]
    reaped = [r["state"] for r in queue.reap(main, "reaper")]
    main.execute("SET lock_timeout = '1s'")
    t0 = time.monotonic()
    try:
        cancel = queue.cancel_job(main, c.id)[0]
    except psycopg.errors.LockNotAvailable:
        cancel = "blocked on row lock (lock_timeout 1s)"
    cancel_ms = round((time.monotonic() - t0) * 1000)
    main.execute("RESET lock_timeout")
    state_while_frozen = queue.get_job(main, c.id)["state"]

    # The worker wakes up and tries to commit.
    try:
        tx.__exit__(None, None, None)
        worker_commit = "committed"
    except psycopg.Error as e:
        worker_commit = f"failed: {type(e).__name__}: {str(e).strip().splitlines()[0]}"
    final = queue.get_job(main, c.id)["state"]
    main.close()
    w.close()
    stuck = lease_expired and not reaped and cancel.startswith("blocked")
    return {"idle_in_transaction_session_timeout_ms": idle_timeout_ms, "freeze_s": freeze_s,
            "lease_expired": lease_expired, "reaper_result": reaped or "nothing reaped (row locked, skipped)",
            "cancel": cancel, "cancel_ms": cancel_ms, "state_while_frozen": state_while_frozen,
            "worker_commit_on_wake": worker_commit, "final_state": final,
            "violations": int(stuck)}
