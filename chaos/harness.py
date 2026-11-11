"""Chaos harness.

10,000 jobs, 8 worker processes, a random worker kill -9'd every ~2s, while
duplicate POSTs (concurrent, delayed, and after client timeouts) and cancels
(of queued, running and finished jobs) run in parallel. Then the invariants
are checked against the database and the client-side record.

    docker compose --profile chaos run --rm chaos --label clean
    docker compose --profile chaos run --rm chaos --label no-fencing --faults no_fencing --stop-frac 0.5

Everything printed is also written to results/raw/chaos-NNN-<label>.txt.
Exit code 0 = all invariants hold, 1 = at least one violated.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import signal
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime

import httpx
import psycopg
from psycopg.rows import dict_row

from app import db
from app.faults import Faults
from chaos.common import RAW_DIR, Tee, code_fingerprint, fresh_database, next_run_number, utc_stamp

# Per-slot skew used only with the worker_clock fault: pairwise differences
# (up to 8s) exceed the 5s lease TTL.
CLOCK_SKEWS = [0, 4, -4, 2, -2, 3, -3, 1]

INFO_EVENTS = {"created", "cancel_requested", "stale_write_rejected"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--label", default="clean")
    p.add_argument("--jobs", type=int, default=10_000)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--kill-interval", type=float, default=2.0)
    p.add_argument("--kill-jitter", type=float, default=0.5)
    p.add_argument("--stop-frac", type=float, default=0.0,
                   help="fraction of chaos events that SIGSTOP (freeze) a worker instead of SIGKILL")
    p.add_argument("--pause-s", type=float, default=8.0, help="how long a frozen worker stays frozen")
    p.add_argument("--min-ms", type=int, default=50)
    p.add_argument("--max-ms", type=int, default=300)
    p.add_argument("--dup-concurrent-frac", type=float, default=0.15)
    p.add_argument("--dup-burst", type=int, default=4,
                   help="number of simultaneous POSTs (original included) for a dup_concurrent key")
    p.add_argument("--dup-later-frac", type=float, default=0.15)
    p.add_argument("--timeout-retry-frac", type=float, default=0.05)
    p.add_argument("--cancel-early-frac", type=float, default=0.05)
    p.add_argument("--cancel-running-frac", type=float, default=0.04)
    p.add_argument("--cancel-late-frac", type=float, default=0.01)
    p.add_argument("--fail-frac", type=float, default=0.02)
    p.add_argument("--concurrency", type=int, default=64)
    p.add_argument("--faults", default="")
    p.add_argument("--effect-mode", default="db", choices=["db", "file"])
    p.add_argument("--isolation", default="read_committed", choices=list(db.ISOLATION))
    p.add_argument("--lease-ttl", type=float, default=5.0)
    p.add_argument("--timeout", type=float, default=1200.0)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--db-name", default="soup_chaos")
    return p.parse_args()


@dataclass
class Proc:
    slot: int
    gen: int
    popen: subprocess.Popen
    stopped_until: float | None = None
    killed: bool = False

    @property
    def name(self) -> str:
        return f"w{self.slot}.{self.gen}"


@dataclass
class Stats:
    kills: int = 0
    stops: int = 0
    unexpected_exits: list = field(default_factory=list)
    post_status: Counter = field(default_factory=Counter)
    post_client_errors: int = 0
    post_server_errors: int = 0
    cancel_status: Counter = field(default_factory=Counter)
    cancel_errors: int = 0
    max_cancel_latency: float = 0.0
    max_post_latency: float = 0.0


class Harness:
    def __init__(self, args: argparse.Namespace):
        self.a = args
        self.rng = random.Random(args.seed if args.seed is not None else time.time_ns())
        self.faults = Faults.parse(args.faults)
        self.stats = Stats()
        self.procs: list[Proc] = []
        self.done = asyncio.Event()
        # key -> list of (kind, status, job_id or None)
        self.posts: dict[str, list] = defaultdict(list)
        # job_id -> list of (kind, status, state reported)
        self.cancels: dict[str, list] = defaultdict(list)
        self.should_fail: set[str] = set()
        self.key_ids: dict[str, str] = {}

        n = next_run_number("chaos")
        self.run_name = f"chaos-{n:03d}-{args.label}"
        self.out = Tee(RAW_DIR / f"{self.run_name}.txt")
        self.worker_log = open(RAW_DIR / f"{self.run_name}.workers.log", "w")
        self.effect_dir = tempfile.mkdtemp(prefix="soup-effects-")
        self.port = 8100 + self.rng.randrange(800)

    # ------------------------------------------------------------ processes

    def env(self, **extra) -> dict:
        e = dict(os.environ)
        e.update(
            DATABASE_URL=self.url,
            FAULTS=self.a.faults,
            EFFECT_MODE=self.a.effect_mode,
            EFFECT_DIR=self.effect_dir,
            DB_ISOLATION=self.a.isolation,
            LEASE_TTL_S=str(self.a.lease_ttl),
            LOG_LEVEL="INFO",
        )
        e.update({k: str(v) for k, v in extra.items()})
        return e

    def spawn_worker(self, slot: int, gen: int) -> Proc:
        skew = CLOCK_SKEWS[slot % len(CLOCK_SKEWS)]
        # sys.executable, not `uv run`: uv would be the PID we kill, and its
        # python child would survive the SIGKILL as an orphan.
        popen = subprocess.Popen(
            [sys.executable, "-m", "app.worker"],
            env=self.env(WORKER_ID=f"w{slot}.{gen}", WORKER_CLOCK_SKEW_S=skew),
            stdout=self.worker_log, stderr=subprocess.STDOUT,
        )
        return Proc(slot, gen, popen)

    def start_api(self) -> None:
        self.api = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.api:app", "--host", "127.0.0.1",
             "--port", str(self.port), "--log-level", "warning"],
            env=self.env(API_POOL_SIZE=32), stdout=self.worker_log, stderr=subprocess.STDOUT,
        )

    async def chaos_monkey(self) -> None:
        while not self.done.is_set():
            delay = self.a.kill_interval + self.rng.uniform(-self.a.kill_jitter, self.a.kill_jitter)
            try:
                await asyncio.wait_for(self.done.wait(), timeout=max(0.1, delay))
                return
            except asyncio.TimeoutError:
                pass
            candidates = [p for p in self.procs if p.stopped_until is None and p.popen.poll() is None]
            if not candidates:
                continue
            victim = self.rng.choice(candidates)
            if self.rng.random() < self.a.stop_frac:
                os.kill(victim.popen.pid, signal.SIGSTOP)
                victim.stopped_until = time.monotonic() + self.a.pause_s
                self.stats.stops += 1
            else:
                victim.killed = True
                os.kill(victim.popen.pid, signal.SIGKILL)
                victim.popen.wait()
                self.stats.kills += 1
                self.procs[self.procs.index(victim)] = self.spawn_worker(victim.slot, victim.gen + 1)

    async def supervisor(self) -> None:
        while not self.done.is_set():
            now = time.monotonic()
            for i, p in enumerate(self.procs):
                if p.stopped_until is not None and now >= p.stopped_until:
                    os.kill(p.popen.pid, signal.SIGCONT)
                    p.stopped_until = None
                rc = p.popen.poll()
                if rc is not None and not p.killed:
                    self.stats.unexpected_exits.append((p.name, rc))
                    self.out(f"!! worker {p.name} exited unexpectedly rc={rc}; respawning")
                    self.procs[i] = self.spawn_worker(p.slot, p.gen + 1)
            await asyncio.sleep(0.2)

    # --------------------------------------------------------------- client

    async def post(self, key: str, payload: dict, kind: str, first_timeout: float | None = None) -> None:
        for attempt in range(30):
            timeout = first_timeout if (first_timeout and attempt == 0) else 30.0
            t0 = time.monotonic()
            try:
                r = await self.client.post(
                    "/jobs", json={"idempotency_key": key, "payload": payload}, timeout=timeout)
            except httpx.HTTPError:
                self.stats.post_client_errors += 1
                self.posts[key].append((kind, "client_error", None))
                await asyncio.sleep(0.05 * (attempt + 1))
                continue
            self.stats.max_post_latency = max(self.stats.max_post_latency, time.monotonic() - t0)
            self.stats.post_status[r.status_code] += 1
            if r.status_code >= 500:
                self.stats.post_server_errors += 1
                self.posts[key].append((kind, r.status_code, None))
                await asyncio.sleep(0.05 * (attempt + 1))
                continue
            job_id = r.json().get("id") if r.status_code in (200, 201) else None
            self.posts[key].append((kind, r.status_code, job_id))
            if job_id:
                self.key_ids.setdefault(key, job_id)
            return

    async def cancel(self, job_id: str, kind: str) -> None:
        for attempt in range(10):
            t0 = time.monotonic()
            try:
                r = await self.client.post(f"/jobs/{job_id}/cancel", timeout=60.0)
            except httpx.HTTPError:
                self.stats.cancel_errors += 1
                await asyncio.sleep(0.1)
                continue
            self.stats.max_cancel_latency = max(self.stats.max_cancel_latency, time.monotonic() - t0)
            self.stats.cancel_status[r.status_code] += 1
            if r.status_code >= 500:
                self.stats.cancel_errors += 1
                continue
            body = r.json()
            self.cancels[job_id].append((kind, r.status_code, body.get("job", {}).get("state")))
            return

    async def submit_one(self, i: int, sem: asyncio.Semaphore, later: list) -> None:
        key = f"job-{i:05d}"
        r = self.rng.random
        payload = {"n": i, "duration_ms": self.rng.randint(self.a.min_ms, self.a.max_ms)}
        if r() < self.a.fail_frac:
            payload["should_fail"] = True
            self.should_fail.add(key)
        dup_concurrent = r() < self.a.dup_concurrent_frac
        dup_later = r() < self.a.dup_later_frac
        timeout_retry = r() < self.a.timeout_retry_frac
        cancel_early = r() < self.a.cancel_early_frac
        async with sem:
            first = self.post(key, payload, "orig", first_timeout=0.001 if timeout_retry else None)
            if dup_concurrent:
                await asyncio.gather(first, *(self.post(key, payload, "dup_concurrent")
                                              for _ in range(self.a.dup_burst - 1)))
            else:
                await first
        if dup_later:
            later.append(asyncio.create_task(self.delayed(self.rng.uniform(0, 30), self.post(key, payload, "dup_later"))))
        if cancel_early and key in self.key_ids:
            later.append(asyncio.create_task(self.delayed(self.rng.uniform(0, 2), self.cancel(self.key_ids[key], "early"))))

    @staticmethod
    async def delayed(seconds: float, coro) -> None:
        await asyncio.sleep(seconds)
        await coro

    async def cancel_running_loop(self) -> None:
        target = int(self.a.jobs * self.a.cancel_running_frac)
        sent = 0
        while sent < target and not self.done.is_set():
            row = await (await self.aconn.execute(
                "SELECT id FROM jobs WHERE state = 'running' ORDER BY random() LIMIT 1")).fetchone()
            if row:
                await self.cancel(str(row["id"]), "running")
                sent += 1
            await asyncio.sleep(0.05)

    async def cancel_late_loop(self) -> None:
        target = int(self.a.jobs * self.a.cancel_late_frac)
        sent = 0
        while sent < target and not self.done.is_set():
            row = await (await self.aconn.execute(
                "SELECT id FROM jobs WHERE state IN ('succeeded','failed') ORDER BY random() LIMIT 1")).fetchone()
            if row:
                await self.cancel(str(row["id"]), "late")
                sent += 1
            await asyncio.sleep(0.2)

    async def counts(self) -> dict:
        rows = await (await self.aconn.execute("SELECT state, count(*) AS n FROM jobs GROUP BY state")).fetchall()
        return {r["state"]: r["n"] for r in rows}

    # ----------------------------------------------------------------- main

    async def run(self) -> int:
        a = self.a
        o = self.out
        o(f"== {self.run_name}  {utc_stamp()}")
        o(f"code={code_fingerprint()} faults={self.faults} effect_mode={a.effect_mode} isolation={a.isolation}")
        o(f"jobs={a.jobs} workers={a.workers} kill_interval={a.kill_interval}±{a.kill_jitter}s "
          f"stop_frac={a.stop_frac} pause={a.pause_s}s lease_ttl={a.lease_ttl}s job_ms={a.min_ms}-{a.max_ms}")
        o(f"dup_concurrent={a.dup_concurrent_frac}x{a.dup_burst} dup_later={a.dup_later_frac} timeout_retry={a.timeout_retry_frac} "
          f"cancel early/running/late={a.cancel_early_frac}/{a.cancel_running_frac}/{a.cancel_late_frac} "
          f"fail={a.fail_frac}")

        self.url = fresh_database(a.db_name, drop_idempotency_constraint="check_then_insert" in self.faults)
        self.aconn = await psycopg.AsyncConnection.connect(self.url, autocommit=True, row_factory=dict_row)
        self.start_api()
        self.client = httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{self.port}",
            limits=httpx.Limits(max_connections=a.concurrency * 2, max_keepalive_connections=a.concurrency))
        for _ in range(100):
            try:
                if (await self.client.get("/healthz")).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.1)
        self.procs = [self.spawn_worker(s, 0) for s in range(a.workers)]

        t0 = time.monotonic()
        bg = [asyncio.create_task(self.chaos_monkey()), asyncio.create_task(self.supervisor())]
        cancel_loops = [asyncio.create_task(self.cancel_running_loop()), asyncio.create_task(self.cancel_late_loop())]

        sem = asyncio.Semaphore(a.concurrency)
        later: list[asyncio.Task] = []
        submit = asyncio.gather(*(self.submit_one(i, sem, later) for i in range(a.jobs)))

        timed_out = False
        last_print = 0.0
        while True:
            await asyncio.sleep(0.5)
            elapsed = time.monotonic() - t0
            c = await self.counts()
            if elapsed - last_print >= 5:
                alive = sum(1 for p in self.procs if p.popen.poll() is None and p.stopped_until is None)
                o(f"t={elapsed:6.1f}s submitted={sum(c.values()):5d} " +
                  " ".join(f"{s}={c.get(s, 0)}" for s in ("queued", "running", "succeeded", "failed", "cancelled")) +
                  f" kills={self.stats.kills} stops={self.stats.stops} workers_active={alive}")
                last_print = elapsed
            all_submitted = submit.done() and all(t.done() for t in later)
            if all_submitted and c.get("queued", 0) == 0 and c.get("running", 0) == 0:
                # Let the running-cancel loop finish only if it still can; otherwise stop.
                break
            if elapsed > a.timeout:
                timed_out = True
                o(f"!! TIMEOUT after {elapsed:.0f}s; jobs still queued/running")
                break
        wall = time.monotonic() - t0
        self.done.set()
        for t in bg + cancel_loops:
            t.cancel()
        await asyncio.gather(*bg, *cancel_loops, return_exceptions=True)
        if not submit.done():
            submit.cancel()
        await self.shutdown()
        await self.client.aclose()
        await self.aconn.close()

        ok = self.check(wall, timed_out)
        self.worker_log.close()
        o(f"worker/api log: {self.worker_log.name}")
        o(f"RESULT: {'PASS' if ok else 'FAIL'}")
        self.out.close()
        return 0 if ok else 1

    async def shutdown(self) -> None:
        for p in self.procs:
            if p.stopped_until is not None and p.popen.poll() is None:
                os.kill(p.popen.pid, signal.SIGCONT)
            if p.popen.poll() is None:
                p.popen.terminate()
        deadline = time.monotonic() + 15
        for p in self.procs:
            try:
                p.popen.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                p.popen.kill()
        self.api.terminate()
        try:
            self.api.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.api.kill()

    # ----------------------------------------------------------- invariants

    def check(self, wall: float, timed_out: bool) -> bool:
        o = self.out
        conn = db.connect(self.url)
        jobs = {str(r["id"]): r for r in conn.execute(
            "SELECT id, idempotency_key, state, attempts, epoch, payload FROM jobs").fetchall()}
        by_key: dict[str, list] = defaultdict(list)
        for j in jobs.values():
            by_key[j["idempotency_key"]].append(str(j["id"]))
        if self.a.effect_mode == "db":
            effects = Counter(str(r["job_id"]) for r in conn.execute("SELECT job_id FROM job_effects").fetchall())
        else:
            effects = Counter()
            path = os.path.join(self.effect_dir, "effects.log")
            if os.path.exists(path):
                for line in open(path):
                    effects[line.split()[0]] += 1
        events = conn.execute("SELECT job_id, kind, epoch, worker_id, detail FROM job_events ORDER BY job_id, id").fetchall()
        conn.close()

        violations: dict[str, list] = defaultdict(list)
        submitted = {f"job-{i:05d}" for i in range(self.a.jobs)}

        # 1. No job lost, no duplicate job for one key, every POST for a key returned the same id.
        for key in submitted:
            ids = by_key.get(key, [])
            if len(ids) == 0:
                violations["lost: key has no job in DB"].append(key)
            elif len(ids) > 1:
                violations["duplicate: key has >1 job in DB"].append(f"{key} -> {ids}")
            returned = {jid for (_, _, jid) in self.posts.get(key, []) if jid}
            if not returned:
                violations["client never received an id for key"].append(key)
            elif len(returned) > 1:
                violations["duplicate: POSTs for one key returned different ids"].append(f"{key} -> {sorted(returned)}")
            elif ids and returned - set(ids):
                violations["POST returned an id that is not the key's job"].append(key)
        extra = set(by_key) - submitted
        if extra:
            violations["unexpected keys in DB"].extend(sorted(extra)[:10])

        # 2. Nothing stuck.
        state_counts = Counter(j["state"] for j in jobs.values())
        for jid, j in jobs.items():
            if j["state"] in ("queued", "running"):
                violations[f"stuck in {j['state']}"].append(jid)

        # 3. Side effects: <=1 per job, exactly 1 iff succeeded.
        for jid, j in jobs.items():
            n = effects.get(jid, 0)
            if n > 1:
                violations["more than one side effect"].append(f"{jid} effects={n} state={j['state']}")
            if j["state"] == "succeeded" and n == 0:
                violations["succeeded without side effect"].append(jid)
            if j["state"] != "succeeded" and n > 0:
                violations["side effect on a non-succeeded job"].append(f"{jid} state={j['state']} effects={n}")
        orphan_effects = set(effects) - set(jobs)
        if orphan_effects:
            violations["side effect for unknown job"].extend(sorted(orphan_effects)[:10])

        # 4. Cancel promises.
        for jid, resps in self.cancels.items():
            j = jobs.get(jid)
            if j is None:
                violations["cancel for unknown job"].append(jid)
                continue
            for kind, status, reported in resps:
                if status in (200, 202) and j["state"] != "cancelled":
                    violations["cancel acknowledged (200/202) but job not cancelled"].append(
                        f"{jid} kind={kind} status={status} final={j['state']}")
                if status == 409 and (reported != j["state"] or j["state"] == "cancelled"):
                    violations["cancel rejected (409) but final state disagrees"].append(
                        f"{jid} reported={reported} final={j['state']}")

        # 5. Jobs designed to fail never succeed.
        for key in self.should_fail:
            for jid in by_key.get(key, []):
                if jobs[jid]["state"] == "succeeded":
                    violations["should_fail job succeeded"].append(jid)

        # 6. Replay each job's event history.
        ev_by_job: dict[str, list] = defaultdict(list)
        for e in events:
            ev_by_job[str(e["job_id"])].append(e)
        kinds = Counter(e["kind"] for e in events)
        premature = []
        for jid, evs in ev_by_job.items():
            held, last_epoch, last_state = False, 0, "queued"
            for e in evs:
                k = e["kind"]
                if k in INFO_EVENTS:
                    continue
                if k == "claim":
                    if held:
                        violations["double claim (claimed while already held)"].append(f"{jid} epoch={e['epoch']}")
                    if e["epoch"] != last_epoch + 1:
                        violations["epoch not monotonic +1 per claim"].append(f"{jid} {last_epoch}->{e['epoch']}")
                    held, last_epoch, last_state = True, e["epoch"], "running"
                elif k in ("requeue", "reap_cancelled", "reap_failed", "succeeded", "failed", "cancelled"):
                    if not held:
                        violations["terminal/requeue transition by non-holder"].append(f"{jid} kind={k} epoch={e['epoch']}")
                    held = False
                    last_state = {"requeue": "queued", "reap_cancelled": "cancelled", "reap_failed": "failed"}.get(k, k)
                    if k == "requeue" and e["detail"] and (
                            datetime.fromisoformat(e["detail"]["old_lease"])
                            > datetime.fromisoformat(e["detail"]["db_now"])):
                        premature.append(f"{jid} lease={e['detail']['old_lease']} db_now={e['detail']['db_now']}")
                elif k == "cancelled_queued":
                    if held:
                        violations["queued-cancel while held"].append(jid)
                    last_state = "cancelled"
            if jid in jobs and last_state != jobs[jid]["state"]:
                violations["event history disagrees with final state"].append(
                    f"{jid} history={last_state} row={jobs[jid]['state']}")
        if premature:
            violations["lease reaped before it expired (by DB clock)"].extend(premature)

        if timed_out:
            violations["harness timed out"].append(f"after {wall:.0f}s")

        # ---- report
        s = self.stats
        claims = kinds["claim"]
        claimed_jobs = len({jid for jid, evs in ev_by_job.items() if any(e["kind"] == "claim" for e in evs)})
        attempts_hist = Counter(j["attempts"] for j in jobs.values())
        o("")
        o("== summary")
        o(f"wall={wall:.1f}s throughput={len(jobs) / wall:.1f} jobs/s")
        o(f"final states: " + " ".join(f"{k}={state_counts.get(k, 0)}" for k in
                                      ("succeeded", "failed", "cancelled", "queued", "running")) + f" total={len(jobs)}")
        o(f"chaos: kill -9={s.kills} SIGSTOP={s.stops} unexpected_worker_exits={len(s.unexpected_exits)} {s.unexpected_exits[:5]}")
        o(f"POST status={dict(s.post_status)} client_errors(timeouts etc)={s.post_client_errors} 5xx={s.post_server_errors} "
          f"max_latency={s.max_post_latency:.2f}s")
        o(f"POST kinds sent: {dict(Counter(k for v in self.posts.values() for (k, _, _) in v))}")
        o(f"cancel status={dict(s.cancel_status)} errors={s.cancel_errors} max_latency={s.max_cancel_latency:.2f}s "
          f"kinds={dict(Counter(k for v in self.cancels.values() for (k, _, _) in v))}")
        o(f"events: {dict(sorted(kinds.items()))}")
        o(f"executions: claims={claims} jobs_claimed={claimed_jobs} re-executions={claims - claimed_jobs} "
          f"(at-least-once) stale_writes_rejected={kinds['stale_write_rejected']}")
        o(f"attempts histogram: {dict(sorted(attempts_hist.items()))}")
        o(f"side effects: total={sum(effects.values())} jobs_with_effect={len(effects)} "
          f"max_per_job={max(effects.values(), default=0)}")
        o("")
        o("== invariants")
        checks = [
            ("no job lost", ["lost: key has no job in DB", "client never received an id for key"]),
            ("no duplicate jobs / ids per idempotency key", ["duplicate: key has >1 job in DB",
                                                            "duplicate: POSTs for one key returned different ids",
                                                            "POST returned an id that is not the key's job",
                                                            "unexpected keys in DB"]),
            ("at most one side effect per job", ["more than one side effect", "side effect for unknown job"]),
            ("effects consistent with final state", ["succeeded without side effect", "side effect on a non-succeeded job",
                                                     "should_fail job succeeded"]),
            ("none stuck in running/queued", ["stuck in running", "stuck in queued", "harness timed out"]),
            ("cancel responses honoured", ["cancel acknowledged (200/202) but job not cancelled",
                                           "cancel rejected (409) but final state disagrees", "cancel for unknown job"]),
            ("no double claim, epochs monotonic", ["double claim (claimed while already held)",
                                                   "epoch not monotonic +1 per claim", "queued-cancel while held"]),
            ("only the lease holder transitions a running job", ["terminal/requeue transition by non-holder"]),
            ("history consistent with final state", ["event history disagrees with final state"]),
            ("no lease reaped early (clock)", ["lease reaped before it expired (by DB clock)"]),
        ]
        covered = {name for _, names in checks for name in names}
        for name in violations:
            if name not in covered:
                checks.append((name, [name]))
        all_ok = True
        for title, names in checks:
            bad = [(n, violations[n]) for n in names if violations.get(n)]
            all_ok &= not bad
            o(f"[{'PASS' if not bad else 'FAIL'}] {title}")
            for n, items in bad:
                o(f"       {n}: {len(items)}  e.g. {items[:3]}")
        return all_ok


def main() -> None:
    args = parse_args()
    h = Harness(args)
    sys.exit(asyncio.run(h.run()))


if __name__ == "__main__":
    main()
