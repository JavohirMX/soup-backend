import hashlib
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import psycopg

from app import db

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = Path(os.environ.get("RESULTS_DIR", ROOT / "results" / "raw"))


def admin_url() -> str:
    return os.environ.get("ADMIN_DATABASE_URL", "postgresql://soup:soup@localhost:55432/postgres")


def db_url(name: str) -> str:
    return re.sub(r"/[^/?]+(\?|$)", f"/{name}\\1", admin_url(), count=1)


def fresh_database(name: str, drop_idempotency_constraint: bool = False) -> str:
    """Drop + create an isolated database, apply the schema, return its URL."""
    with psycopg.connect(admin_url(), autocommit=True) as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.execute(f'CREATE DATABASE "{name}"')
    url = db_url(name)
    with db.connect(url) as conn:
        db.apply_schema(conn)
        if drop_idempotency_constraint:
            conn.execute("ALTER TABLE jobs DROP CONSTRAINT jobs_idempotency_key_key")
    return url


def code_fingerprint() -> str:
    h = hashlib.sha256()
    for p in sorted((ROOT / "app").glob("*.py")) + [ROOT / "app" / "schema.sql"]:
        h.update(p.read_bytes())
    return h.hexdigest()[:12]


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def next_run_number(prefix: str) -> int:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    nums = [int(m.group(1)) for p in RAW_DIR.glob(f"{prefix}-*")
            if (m := re.match(rf"{prefix}-(\d+)", p.name))]
    return max(nums, default=0) + 1


class Tee:
    """print() to stdout and to a raw results file."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.f = path.open("w")

    def __call__(self, *args) -> None:
        line = " ".join(str(a) for a in args)
        print(line, flush=True)
        self.f.write(line + "\n")
        self.f.flush()

    def close(self) -> None:
        self.f.close()
