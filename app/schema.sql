-- Applied idempotently at startup under an advisory lock (see app/db.py).

CREATE TABLE IF NOT EXISTS jobs (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key  text NOT NULL,
    payload          jsonb NOT NULL,
    payload_hash     text NOT NULL,
    state            text NOT NULL DEFAULT 'queued'
                     CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    -- Fencing token: incremented on every claim. Every write by a worker must
    -- present the epoch it claimed with; a stale epoch matches zero rows.
    epoch            bigint NOT NULL DEFAULT 0,
    attempts         int NOT NULL DEFAULT 0,
    max_attempts     int NOT NULL DEFAULT 5,
    lease_owner      text,
    lease_expires_at timestamptz,
    cancel_requested boolean NOT NULL DEFAULT false,
    result           jsonb,
    error            text,
    created_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at       timestamptz,
    finished_at      timestamptz,
    CONSTRAINT jobs_idempotency_key_key UNIQUE (idempotency_key),
    CONSTRAINT running_has_lease CHECK (state <> 'running' OR lease_expires_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS jobs_queued_idx  ON jobs (created_at) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS jobs_running_idx ON jobs (lease_expires_at) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS jobs_list_idx    ON jobs (created_at DESC, id DESC);

-- The emulated side effect. Deliberately NO unique constraint on job_id:
-- if the system ever produces two effects for one job we want to see it,
-- not have the database silently reject (and hide) the second one.
CREATE TABLE IF NOT EXISTS job_effects (
    id        bigserial PRIMARY KEY,
    job_id    uuid NOT NULL,
    epoch     bigint NOT NULL,
    worker_id text NOT NULL,
    at        timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS job_effects_job_idx ON job_effects (job_id);

-- Append-only audit log of every state transition, written in the same
-- transaction as the transition itself. Because the transition holds the job's
-- row lock while inserting, event ids are ordered per job in transition order,
-- which lets the harness replay each job's history and detect double claims.
CREATE TABLE IF NOT EXISTS job_events (
    id        bigserial PRIMARY KEY,
    job_id    uuid NOT NULL,
    kind      text NOT NULL,
    epoch     bigint,
    worker_id text,
    detail    jsonb,
    at        timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events (job_id, id);
