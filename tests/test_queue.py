"""Deterministic state-machine tests against a real Postgres.

Run: docker compose up -d postgres && uv run pytest
"""

import pytest

from app import db, queue
from app.faults import Faults
from chaos.common import fresh_database


@pytest.fixture
def conn():
    url = fresh_database("soup_test")
    with db.connect(url) as c:
        yield c


def effects(conn, job_id) -> int:
    return conn.execute("SELECT count(*) AS n FROM job_effects WHERE job_id = %s", (job_id,)).fetchone()["n"]


def expire_lease(conn, job_id):
    conn.execute("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = %s", (job_id,))


def test_create_is_idempotent(conn):
    a, created_a = queue.create_job(conn, "k1", {"x": 1})
    b, created_b = queue.create_job(conn, "k1", {"x": 1})
    assert created_a and not created_b
    assert a["id"] == b["id"]
    assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1


def test_same_key_different_payload_conflicts(conn):
    queue.create_job(conn, "k1", {"x": 1})
    with pytest.raises(queue.IdempotencyConflict):
        queue.create_job(conn, "k1", {"x": 2})


def test_claim_complete_writes_exactly_one_effect(conn):
    job, _ = queue.create_job(conn, "k1", {})
    c = queue.claim(conn, "w1", 5)
    assert c.id == job["id"] and c.epoch == 1
    assert queue.claim(conn, "w2", 5) is None
    assert queue.finish(conn, c.id, c.epoch, "w1", "succeeded") == "succeeded"
    assert effects(conn, c.id) == 1
    # A second finish with the same epoch is fenced off (job no longer running).
    assert queue.finish(conn, c.id, c.epoch, "w1", "succeeded") is None
    assert effects(conn, c.id) == 1


def test_zombie_with_stale_epoch_is_rejected(conn):
    queue.create_job(conn, "k1", {})
    old = queue.claim(conn, "zombie", 5)
    expire_lease(conn, old.id)
    assert [r["state"] for r in queue.reap(conn, "reaper")] == ["queued"]
    new = queue.claim(conn, "w2", 5)
    assert new.epoch == old.epoch + 1
    assert queue.heartbeat(conn, old.id, old.epoch, 5) == "lost"
    assert queue.finish(conn, old.id, old.epoch, "zombie", "succeeded") is None
    assert queue.finish(conn, new.id, new.epoch, "w2", "succeeded") == "succeeded"
    assert effects(conn, new.id) == 1


def test_zombie_without_fencing_double_effect(conn):
    """Negative control: the no_fencing fault must produce a visible bug."""
    f = Faults.parse("no_fencing")
    queue.create_job(conn, "k1", {})
    old = queue.claim(conn, "zombie", 5, faults=f)
    expire_lease(conn, old.id)
    queue.reap(conn, "reaper")
    new = queue.claim(conn, "w2", 5, faults=f)
    queue.finish(conn, new.id, new.epoch, "w2", "succeeded", faults=f)
    queue.finish(conn, old.id, old.epoch, "zombie", "succeeded", faults=f)
    assert effects(conn, new.id) == 2


def test_reaper_does_not_touch_live_lease(conn):
    queue.create_job(conn, "k1", {})
    queue.claim(conn, "w1", 5)
    assert queue.reap(conn, "reaper") == []


def test_cancel_queued(conn):
    job, _ = queue.create_job(conn, "k1", {})
    outcome, j = queue.cancel_job(conn, job["id"])
    assert outcome == "cancelled" and j["state"] == "cancelled"
    assert queue.claim(conn, "w1", 5) is None
    assert queue.cancel_job(conn, job["id"])[0] == "cancelled"  # idempotent


def test_cancel_running_wins_over_completion(conn):
    queue.create_job(conn, "k1", {})
    c = queue.claim(conn, "w1", 5)
    assert queue.cancel_job(conn, c.id)[0] == "cancel_requested"
    assert queue.heartbeat(conn, c.id, c.epoch, 5) == "cancel"
    # Worker did not notice and tries to complete: still ends cancelled, no effect.
    assert queue.finish(conn, c.id, c.epoch, "w1", "succeeded") == "cancelled"
    assert effects(conn, c.id) == 0


def test_cancel_after_success_is_too_late(conn):
    queue.create_job(conn, "k1", {})
    c = queue.claim(conn, "w1", 5)
    queue.finish(conn, c.id, c.epoch, "w1", "succeeded")
    outcome, j = queue.cancel_job(conn, c.id)
    assert outcome == "too_late" and j["state"] == "succeeded"


def test_cancel_requested_then_worker_dies(conn):
    queue.create_job(conn, "k1", {})
    c = queue.claim(conn, "w1", 5)
    queue.cancel_job(conn, c.id)
    expire_lease(conn, c.id)
    assert [r["state"] for r in queue.reap(conn, "reaper")] == ["cancelled"]


def test_max_attempts_then_failed(conn):
    queue.create_job(conn, "k1", {}, max_attempts=2)
    for expected in ["queued", "failed"]:
        c = queue.claim(conn, "w", 5)
        expire_lease(conn, c.id)
        assert [r["state"] for r in queue.reap(conn, "reaper")] == [expected]
    assert queue.claim(conn, "w", 5) is None


def test_keyset_pagination_sees_every_job_once(conn):
    ids = {queue.create_job(conn, f"k{i}", {})[0]["id"] for i in range(25)}
    seen, cursor = [], None
    while True:
        page, cursor = queue.list_jobs(conn, limit=7, cursor=cursor)
        seen += [j["id"] for j in page]
        # Inserting during pagination must not cause duplicates or skips of old rows.
        queue.create_job(conn, f"new{len(seen)}", {})
        if cursor is None:
            break
    assert len(seen) == len(set(seen)) == 25 and set(seen) == ids
