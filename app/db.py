import time
from pathlib import Path
from typing import Callable, TypeVar

import psycopg
from psycopg import errors
from psycopg.rows import dict_row

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
SCHEMA_LOCK_ID = 7_001_001

ISOLATION = {
    "read_committed": psycopg.IsolationLevel.READ_COMMITTED,
    "repeatable_read": psycopg.IsolationLevel.REPEATABLE_READ,
    "serializable": psycopg.IsolationLevel.SERIALIZABLE,
}

T = TypeVar("T")

RETRYABLE = (errors.SerializationFailure, errors.DeadlockDetected)


def configure(conn: psycopg.Connection, isolation: str = "read_committed") -> None:
    conn.autocommit = True
    conn.isolation_level = ISOLATION[isolation]
    conn.row_factory = dict_row


def connect(url: str, isolation: str = "read_committed", **kwargs) -> psycopg.Connection:
    conn = psycopg.connect(url, **kwargs)
    configure(conn, isolation)
    return conn


def apply_schema(conn: psycopg.Connection) -> None:
    # Several processes start at once; CREATE ... IF NOT EXISTS is not safe
    # under concurrency (it can fail on the catalog's unique index).
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_ID,))
        conn.execute(SCHEMA_PATH.read_text())


class RetryStats:
    def __init__(self) -> None:
        self.retries = 0


def with_retry(fn: Callable[[], T], attempts: int = 8, stats: RetryStats | None = None) -> T:
    """Re-run a whole transaction on serialization failure / deadlock.

    Only needed under REPEATABLE READ / SERIALIZABLE (and for deadlocks);
    under READ COMMITTED our single-row conditional UPDATEs never raise these.
    """
    for i in range(attempts):
        try:
            return fn()
        except RETRYABLE:
            if stats is not None:
                stats.retries += 1
            if i == attempts - 1:
                raise
            time.sleep(0.005 * (2**i))
    raise AssertionError("unreachable")
