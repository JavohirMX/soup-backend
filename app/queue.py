"""All job-queue SQL lives here.

Invariant that everything relies on: every state transition is ONE conditional
UPDATE on the job row (so it holds the row lock and Postgres re-checks the
WHERE clause against the latest committed version), plus an audit event written
in the same transaction. No read-then-decide-then-write in application code.

Worker-side writes are fenced by (id, epoch, state='running'); a worker whose
lease was taken over has a stale epoch and its write matches zero rows.
"""

from __future__ import annotations

import base64
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from app.faults import NONE, Faults

TERMINAL = ("succeeded", "failed", "cancelled")

JOB_COLUMNS = """id, idempotency_key, payload, state, epoch, attempts, max_attempts,
    lease_owner, lease_expires_at, cancel_requested, result, error,
    created_at, updated_at, started_at, finished_at"""


class IdempotencyConflict(Exception):
    """Same idempotency key re-used with a different payload."""

    def __init__(self, existing: dict):
        super().__init__("idempotency key reused with a different payload")
        self.existing = existing


@dataclass(frozen=True)
class Claim:
    id: uuid.UUID
    epoch: int
    attempts: int
    payload: dict


@dataclass(frozen=True)
class Clock:
    """Where "now" comes from for lease arithmetic.

    Default: the database's now(). There is exactly one clock (the primary's),
    so worker clock skew cannot affect lease decisions. The worker_clock fault
    switches to the worker's local, possibly skewed, clock.
    """

    use_local: bool = False
    skew_s: float = 0.0

    @classmethod
    def for_worker(cls, faults: Faults, skew_s: float) -> "Clock":
        return cls(use_local="worker_clock" in faults, skew_s=skew_s)

    def sql(self) -> str:
        return "%(client_now)s::timestamptz" if self.use_local else "now()"

    def params(self) -> dict:
        if not self.use_local:
            return {}
        return {"client_now": datetime.now(timezone.utc) + timedelta(seconds=self.skew_s)}


DB_CLOCK = Clock()


def payload_hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _event(conn, job_id, kind, epoch=None, worker_id=None, detail=None) -> None:
    conn.execute(
        "INSERT INTO job_events (job_id, kind, epoch, worker_id, detail) VALUES (%s, %s, %s, %s, %s)",
        (job_id, kind, epoch, worker_id, Jsonb(detail) if detail is not None else None),
    )


# ---------------------------------------------------------------- API side


def create_job(
    conn: psycopg.Connection,
    key: str,
    payload: dict,
    max_attempts: int = 5,
    faults: Faults = NONE,
) -> tuple[dict, bool]:
    """Idempotent create. Returns (job, created)."""
    h = payload_hash(payload)
    if "check_then_insert" in faults:
        job = _create_check_then_insert(conn, key, payload, h, max_attempts)
        created = job.pop("_created")
    else:
        with conn.transaction():
            job = conn.execute(
                f"""INSERT INTO jobs (idempotency_key, payload, payload_hash, max_attempts)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING {JOB_COLUMNS}, payload_hash""",
                (key, Jsonb(payload), h, max_attempts),
            ).fetchone()
            if job:
                _event(conn, job["id"], "created")
        created = job is not None
        if not created:
            # Separate statement (and, under READ COMMITTED, a fresh snapshot):
            # ON CONFLICT DO NOTHING waited for the conflicting insert to
            # commit, so the row is visible now.
            job = conn.execute(
                f"SELECT {JOB_COLUMNS}, payload_hash FROM jobs WHERE idempotency_key = %s", (key,)
            ).fetchone()
            if job is None:
                raise RuntimeError(f"idempotency key {key!r} conflicted but row not visible")
    if job.pop("payload_hash") != h:
        raise IdempotencyConflict(job)
    return job, created


def _create_check_then_insert(conn, key, payload, h, max_attempts) -> dict:
    # FAULT: the classic race. Two concurrent requests both see "no row" and
    # both insert. Only a unique constraint saves you, and the harness drops it.
    existing = conn.execute(
        f"SELECT {JOB_COLUMNS}, payload_hash FROM jobs WHERE idempotency_key = %s LIMIT 1", (key,)
    ).fetchone()
    if existing:
        existing["_created"] = False
        return existing
    with conn.transaction():
        job = conn.execute(
            f"""INSERT INTO jobs (idempotency_key, payload, payload_hash, max_attempts)
                VALUES (%s, %s, %s, %s) RETURNING {JOB_COLUMNS}, payload_hash""",
            (key, Jsonb(payload), h, max_attempts),
        ).fetchone()
        _event(conn, job["id"], "created")
    job["_created"] = True
    return job


def get_job(conn: psycopg.Connection, job_id: uuid.UUID) -> dict | None:
    return conn.execute(f"SELECT {JOB_COLUMNS} FROM jobs WHERE id = %s", (job_id,)).fetchone()


def encode_cursor(job: dict) -> str:
    raw = f"{job['created_at'].isoformat()}|{job['id']}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    created_at, job_id = base64.urlsafe_b64decode(cursor.encode()).decode().split("|")
    return datetime.fromisoformat(created_at), uuid.UUID(job_id)


def list_jobs(
    conn: psycopg.Connection, limit: int = 50, cursor: str | None = None, state: str | None = None
) -> tuple[list[dict], str | None]:
    """Keyset pagination, newest first. Stable under concurrent inserts
    (unlike OFFSET, which skips/duplicates rows when new jobs arrive)."""
    where, params = [], {"limit": limit + 1}
    if cursor:
        params["c_at"], params["c_id"] = decode_cursor(cursor)
        where.append("(created_at, id) < (%(c_at)s, %(c_id)s)")
    if state:
        where.append("state = %(state)s")
        params["state"] = state
    sql = f"SELECT {JOB_COLUMNS} FROM jobs"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC, id DESC LIMIT %(limit)s"
    rows = conn.execute(sql, params).fetchall()
    next_cursor = encode_cursor(rows[limit - 1]) if len(rows) > limit else None
    return rows[:limit], next_cursor


def cancel_job(conn: psycopg.Connection, job_id: uuid.UUID) -> tuple[str, dict | None]:
    """Returns (outcome, job) where outcome is one of:
    'cancelled'        queued job, now cancelled (or already cancelled: idempotent)
    'cancel_requested' running job, flag set; the job WILL end 'cancelled'
    'too_late'         job already succeeded/failed
    'not_found'
    """
    with conn.transaction():
        job = conn.execute(
            f"""UPDATE jobs SET
                    cancel_requested = true,
                    state = CASE WHEN state = 'queued' THEN 'cancelled' ELSE state END,
                    finished_at = CASE WHEN state = 'queued' THEN now() ELSE finished_at END,
                    updated_at = now()
                WHERE id = %s AND state IN ('queued', 'running')
                RETURNING {JOB_COLUMNS}""",
            (job_id,),
        ).fetchone()
        if job:
            kind = "cancelled_queued" if job["state"] == "cancelled" else "cancel_requested"
            _event(conn, job_id, kind, job["epoch"])
            return ("cancelled" if kind == "cancelled_queued" else "cancel_requested"), job
    job = get_job(conn, job_id)
    if job is None:
        return "not_found", None
    if job["state"] == "cancelled":
        return "cancelled", job
    return "too_late", job


# ------------------------------------------------------------- worker side


def claim(
    conn: psycopg.Connection,
    worker_id: str,
    lease_ttl_s: float,
    clock: Clock = DB_CLOCK,
    faults: Faults = NONE,
) -> Claim | None:
    if "naive_claim" in faults:
        return _claim_naive(conn, worker_id, lease_ttl_s, clock)
    now = clock.sql()
    with conn.transaction():
        row = conn.execute(
            f"""UPDATE jobs SET
                    state = 'running',
                    epoch = epoch + 1,
                    attempts = attempts + 1,
                    lease_owner = %(w)s,
                    lease_expires_at = {now} + make_interval(secs => %(ttl)s),
                    started_at = coalesce(started_at, now()),
                    updated_at = now()
                WHERE id = (
                    SELECT id FROM jobs
                    WHERE state = 'queued'
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, epoch, attempts, payload""",
            {"w": worker_id, "ttl": lease_ttl_s, **clock.params()},
        ).fetchone()
        if row is None:
            return None
        _event(conn, row["id"], "claim", row["epoch"], worker_id)
    return Claim(row["id"], row["epoch"], row["attempts"], row["payload"])


def _claim_naive(conn, worker_id, lease_ttl_s, clock) -> Claim | None:
    # FAULT: read, then write without re-checking the state we read.
    now = clock.sql()
    with conn.transaction():
        picked = conn.execute(
            "SELECT id FROM jobs WHERE state = 'queued' ORDER BY created_at LIMIT 1"
        ).fetchone()
        if picked is None:
            return None
        row = conn.execute(
            f"""UPDATE jobs SET state = 'running', epoch = epoch + 1, attempts = attempts + 1,
                    lease_owner = %(w)s,
                    lease_expires_at = {now} + make_interval(secs => %(ttl)s),
                    started_at = coalesce(started_at, now()), updated_at = now()
                WHERE id = %(id)s
                RETURNING id, epoch, attempts, payload""",
            {"w": worker_id, "ttl": lease_ttl_s, "id": picked["id"], **clock.params()},
        ).fetchone()
        _event(conn, row["id"], "claim", row["epoch"], worker_id)
    return Claim(row["id"], row["epoch"], row["attempts"], row["payload"])


def _fence(faults: Faults) -> str:
    if "no_fencing" in faults:
        return "id = %(id)s"
    return "id = %(id)s AND epoch = %(epoch)s AND state = 'running'"


def heartbeat(
    conn: psycopg.Connection,
    job_id: uuid.UUID,
    epoch: int,
    lease_ttl_s: float,
    clock: Clock = DB_CLOCK,
    faults: Faults = NONE,
) -> str:
    """Extend the lease. Returns 'ok', 'cancel' (cancel requested) or 'lost'."""
    row = conn.execute(
        f"""UPDATE jobs SET lease_expires_at = {clock.sql()} + make_interval(secs => %(ttl)s),
                updated_at = now()
            WHERE {_fence(faults)}
            RETURNING cancel_requested""",
        {"id": job_id, "epoch": epoch, "ttl": lease_ttl_s, **clock.params()},
    ).fetchone()
    if row is None:
        return "lost"
    return "cancel" if row["cancel_requested"] else "ok"


def finish(
    conn: psycopg.Connection,
    job_id: uuid.UUID,
    epoch: int,
    worker_id: str,
    outcome: str,
    result: Any = None,
    error: str | None = None,
    write_db_effect: bool = True,
    faults: Faults = NONE,
) -> str | None:
    """Fenced terminal transition. outcome in {'succeeded','failed','cancelled'}.

    If cancellation was requested, the job ends 'cancelled' regardless of the
    worker's outcome: the decision is made under the row lock, so a concurrent
    cancel either lands before (we see the flag) or finds the job terminal.

    The side effect row is inserted in the same transaction, only if the job
    actually transitioned to 'succeeded'. Returns the final state, or None if
    the write was fenced off (stale epoch / no longer running).
    """
    assert outcome in TERMINAL
    if "no_fencing" in faults:
        state_expr = "%(outcome)s::text"
    else:
        state_expr = "CASE WHEN cancel_requested THEN 'cancelled' ELSE %(outcome)s::text END"
    with conn.transaction():
        row = conn.execute(
            f"""UPDATE jobs SET
                    state = {state_expr},
                    result = CASE WHEN {state_expr} = 'succeeded' THEN %(result)s::jsonb END,
                    error = %(error)s,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    finished_at = now(),
                    updated_at = now()
                WHERE {_fence(faults)}
                RETURNING state, epoch""",
            {
                "id": job_id,
                "epoch": epoch,
                "outcome": outcome,
                "result": Jsonb(result) if result is not None else None,
                "error": error,
            },
        ).fetchone()
        if row is None:
            current = conn.execute("SELECT state, epoch FROM jobs WHERE id = %s", (job_id,)).fetchone()
            _event(
                conn, job_id, "stale_write_rejected", epoch, worker_id,
                {"wanted": outcome, "current_state": current and current["state"],
                 "current_epoch": current and current["epoch"]},
            )
            return None
        if row["state"] == "succeeded" and write_db_effect:
            conn.execute(
                "INSERT INTO job_effects (job_id, epoch, worker_id) VALUES (%s, %s, %s)",
                (job_id, epoch, worker_id),
            )
        _event(conn, job_id, row["state"], epoch, worker_id)
        return row["state"]


def reap(
    conn: psycopg.Connection,
    worker_id: str,
    clock: Clock = DB_CLOCK,
    batch: int = 100,
) -> list[dict]:
    """Return expired leases to the queue (or to cancelled / failed).

    FOR UPDATE SKIP LOCKED: never blocks on a job a live worker is currently
    finishing. Under READ COMMITTED, a row whose lease was concurrently
    extended is re-checked against the WHERE clause and dropped.
    """
    now = clock.sql()
    with conn.transaction():
        rows = conn.execute(
            f"""WITH expired AS (
                    SELECT id, lease_expires_at AS old_lease, lease_owner AS old_owner
                    FROM jobs
                    WHERE state = 'running' AND lease_expires_at < {now}
                    ORDER BY lease_expires_at
                    LIMIT %(batch)s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE jobs j SET
                    state = CASE
                        WHEN j.cancel_requested THEN 'cancelled'
                        WHEN j.attempts >= j.max_attempts THEN 'failed'
                        ELSE 'queued' END,
                    error = CASE
                        WHEN NOT j.cancel_requested AND j.attempts >= j.max_attempts
                        THEN 'lease expired after max attempts' ELSE j.error END,
                    finished_at = CASE
                        WHEN j.cancel_requested OR j.attempts >= j.max_attempts
                        THEN now() ELSE NULL END,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = now()
                FROM expired e
                WHERE j.id = e.id
                RETURNING j.id, j.state, j.epoch, e.old_lease, e.old_owner, now() AS db_now""",
            {"batch": batch, **clock.params()},
        ).fetchall()
        for r in rows:
            kind = {"queued": "requeue", "cancelled": "reap_cancelled", "failed": "reap_failed"}[r["state"]]
            _event(
                conn, r["id"], kind, r["epoch"], worker_id,
                {"old_owner": r["old_owner"], "old_lease": r["old_lease"].isoformat(),
                 "db_now": r["db_now"].isoformat()},
            )
    return rows
