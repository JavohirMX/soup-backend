"""Worker process: claim -> execute (with heartbeats) -> fenced finish.

Run: uv run python -m app.worker
"""

import logging
import os
import random
import signal
import threading
import time

import psycopg

from app import db, queue
from app.config import Config
from app.faults import Faults

log = logging.getLogger("worker")


class Heartbeat(threading.Thread):
    """Extends the lease every heartbeat_s on its own connection and reports
    cancel requests / lease loss back to the executing thread."""

    def __init__(self, conn, job: queue.Claim, cfg: Config, clock: queue.Clock, faults: Faults):
        super().__init__(daemon=True)
        self.conn, self.job, self.cfg, self.clock, self.faults = conn, job, cfg, clock, faults
        self.cancel = threading.Event()
        self.lost = threading.Event()
        self._halt = threading.Event()

    def run(self) -> None:
        while not self._halt.wait(self.cfg.heartbeat_s):
            try:
                status = db.with_retry(lambda: queue.heartbeat(
                    self.conn, self.job.id, self.job.epoch, self.cfg.lease_ttl_s, self.clock, self.faults))
            except psycopg.Error as e:
                log.warning("heartbeat error job=%s: %s", self.job.id, e)
                continue
            if status == "cancel":
                self.cancel.set()
            elif status == "lost":
                self.lost.set()
                return

    def stop(self) -> None:
        self._halt.set()
        self.join()


class Worker:
    def __init__(self, cfg: Config, faults: Faults):
        self.cfg, self.faults = cfg, faults
        self.clock = queue.Clock.for_worker(faults, cfg.clock_skew_s)
        self.conn = self._connect()
        self.hb_conn = self._connect()
        self.stopping = False
        self.last_reap = 0.0
        self.retry_stats = db.RetryStats()

    def _connect(self) -> psycopg.Connection:
        # A worker frozen *inside* a transaction keeps the job's row lock, and
        # the reaper (SKIP LOCKED) would skip that job forever. Let the server
        # kill such sessions after one lease TTL, which releases the lock.
        idle_ms = int(self.cfg.lease_ttl_s * 1000)
        return db.connect(self.cfg.database_url, self.cfg.isolation,
                          options=f"-c idle_in_transaction_session_timeout={idle_ms}")

    def _ensure_connections(self) -> None:
        for attr in ("conn", "hb_conn"):
            c = getattr(self, attr)
            if c.closed or c.broken:
                log.warning("reconnecting %s", attr)
                setattr(self, attr, self._connect())

    def run_forever(self) -> None:
        db.apply_schema(self.conn)
        log.info("worker %s started faults=%s isolation=%s effect=%s skew=%s",
                 self.cfg.worker_id, self.faults, self.cfg.isolation, self.cfg.effect_mode,
                 self.cfg.clock_skew_s)
        while not self.stopping:
            try:
                self._ensure_connections()
                self.step()
            except psycopg.Error as e:
                # Includes the server terminating an idle-in-transaction session.
                # Whatever job we held will be reaped when its lease expires.
                log.warning("db error, reconnecting if needed: %s: %s", type(e).__name__, e)
                time.sleep(0.5)
        log.info("worker %s stopped (serialization retries: %d)", self.cfg.worker_id, self.retry_stats.retries)

    def step(self) -> None:
        if time.monotonic() - self.last_reap >= self.cfg.reap_interval_s:
            reaped = db.with_retry(
                lambda: queue.reap(self.conn, self.cfg.worker_id, self.clock), stats=self.retry_stats)
            if reaped:
                log.info("reaped %d expired leases", len(reaped))
            self.last_reap = time.monotonic()
        job = db.with_retry(
            lambda: queue.claim(self.conn, self.cfg.worker_id, self.cfg.lease_ttl_s, self.clock, self.faults),
            stats=self.retry_stats)
        if job is None:
            time.sleep(self.cfg.poll_s * random.uniform(0.5, 1.5))
            return
        self.execute(job)

    def execute(self, job: queue.Claim) -> None:
        hb = Heartbeat(self.hb_conn, job, self.cfg, self.clock, self.faults)
        hb.start()
        trust_lease = "no_fencing" in self.faults
        deadline = time.monotonic() + job.payload.get("duration_ms", 100) / 1000
        while time.monotonic() < deadline:
            if hb.cancel.is_set() or (hb.lost.is_set() and not trust_lease):
                break
            time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))
        hb.stop()

        if hb.lost.is_set() and not trust_lease:
            # Someone else owns the job now. Abandoning is only an
            # optimisation: even if we did write, the fence would reject it.
            log.info("lost lease job=%s epoch=%s, abandoning", job.id, job.epoch)
            return
        if hb.cancel.is_set():
            outcome, error = "cancelled", None
        elif job.payload.get("should_fail"):
            outcome, error = "failed", "simulated failure"
        else:
            outcome, error = "succeeded", None
            if self.cfg.effect_mode == "file":
                self.write_file_effect(job)
                time.sleep(self.cfg.effect_commit_delay_ms / 1000)

        final = db.with_retry(lambda: queue.finish(
            self.conn, job.id, job.epoch, self.cfg.worker_id, outcome,
            result={"worker": self.cfg.worker_id} if outcome == "succeeded" else None,
            error=error,
            write_db_effect=self.cfg.effect_mode == "db",
            faults=self.faults,
        ), stats=self.retry_stats)
        if final is None:
            log.warning("stale write rejected job=%s epoch=%s (lease was taken over)", job.id, job.epoch)

    def write_file_effect(self, job: queue.Claim) -> None:
        # Non-transactional side effect: if we die between this write and the
        # commit in finish(), the job is retried and the effect happens twice.
        os.makedirs(self.cfg.effect_dir, exist_ok=True)
        fd = os.open(os.path.join(self.cfg.effect_dir, "effects.log"), os.O_WRONLY | os.O_APPEND | os.O_CREAT)
        try:
            os.write(fd, f"{job.id} {job.epoch} {self.cfg.worker_id}\n".encode())
        finally:
            os.close(fd)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    worker = Worker(Config.from_env(), Faults.from_env())

    def graceful(*_):
        worker.stopping = True

    signal.signal(signal.SIGTERM, graceful)
    signal.signal(signal.SIGINT, graceful)
    worker.run_forever()


if __name__ == "__main__":
    main()
