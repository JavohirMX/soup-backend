import os
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from app import db, queue
from app.config import Config
from app.faults import Faults

cfg = Config.from_env()
faults = Faults.from_env()
pool: ConnectionPool | None = None
STATES = ("queued", "running", "succeeded", "failed", "cancelled")


@asynccontextmanager
async def lifespan(_: FastAPI):
    global pool
    pool = ConnectionPool(
        cfg.database_url,
        min_size=2,
        max_size=int(os.environ.get("API_POOL_SIZE", 16)),
        configure=lambda c: db.configure(c, cfg.isolation),
        open=True,
    )
    with pool.connection() as conn:
        db.apply_schema(conn)
    yield
    pool.close()


app = FastAPI(title="soup job queue", lifespan=lifespan)


class CreateJob(BaseModel):
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=5, ge=1, le=100)


def _run(fn):
    """Each call gets a pooled connection; whole transaction retried on
    serialization failure (only possible under REPEATABLE READ/SERIALIZABLE)."""

    def attempt():
        with pool.connection() as conn:
            return fn(conn)

    return db.with_retry(attempt)


def _job_json(job: dict) -> dict:
    return jsonable_encoder(job)


@app.get("/healthz")
def healthz():
    _run(lambda c: c.execute("SELECT 1").fetchone())
    return {"ok": True, "faults": str(faults), "isolation": cfg.isolation}


@app.post("/jobs", status_code=201)
def create_job(
    body: CreateJob,
    response: Response,
    idempotency_key_header: str | None = Header(default=None, alias="Idempotency-Key"),
):
    key = body.idempotency_key or idempotency_key_header
    if not key:
        raise HTTPException(422, "idempotency_key (body) or Idempotency-Key header is required")
    try:
        job, created = _run(lambda c: queue.create_job(c, key, body.payload, body.max_attempts, faults))
    except queue.IdempotencyConflict as e:
        return JSONResponse(
            status_code=409,
            content={"detail": "idempotency key reused with a different payload",
                     "job": _job_json(e.existing)},
        )
    if not created:
        response.status_code = 200
    return _job_json(job)


@app.get("/jobs/{job_id}")
def get_job(job_id: uuid.UUID):
    job = _run(lambda c: queue.get_job(c, job_id))
    if job is None:
        raise HTTPException(404, "job not found")
    return _job_json(job)


@app.get("/jobs")
def list_jobs(
    limit: int = Query(default=50, ge=1, le=500),
    cursor: str | None = None,
    state: str | None = Query(default=None),
):
    if state is not None and state not in STATES:
        raise HTTPException(422, f"state must be one of {STATES}")
    try:
        items, next_cursor = _run(lambda c: queue.list_jobs(c, limit, cursor, state))
    except ValueError:
        raise HTTPException(422, "invalid cursor")
    return {"items": [_job_json(j) for j in items], "next_cursor": next_cursor}


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: uuid.UUID):
    outcome, job = _run(lambda c: queue.cancel_job(c, job_id))
    if outcome == "not_found":
        raise HTTPException(404, "job not found")
    if outcome == "too_late":
        return JSONResponse(
            status_code=409,
            content={"detail": f"job already {job['state']}", "job": _job_json(job)},
        )
    # 200: job is cancelled. 202: cancellation requested on a running job;
    # it is guaranteed to end 'cancelled' (the worker cannot complete it).
    status = 200 if outcome == "cancelled" else 202
    return JSONResponse(status_code=status, content={"outcome": outcome, "job": _job_json(job)})


@app.get("/stats")
def stats():
    rows = _run(lambda c: c.execute("SELECT state, count(*) AS n FROM jobs GROUP BY state").fetchall())
    return {r["state"]: r["n"] for r in rows}
