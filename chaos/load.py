"""Open-loop load generator against the compose stack (api + 4 workers).

Requests are scheduled at a fixed rate regardless of how fast earlier ones
complete (no coordinated omission): if the server slows down, latency and
in-flight count grow instead of the load quietly dropping.

    docker compose up -d
    docker compose --profile chaos run --rm tools python -m chaos.load --rps 100 --duration 120
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import statistics
import time
import uuid
from collections import Counter, defaultdict

import httpx
import psycopg
from psycopg.rows import dict_row

from chaos.common import RAW_DIR, Tee, code_fingerprint, next_run_number, utc_stamp


def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--api", default="http://api:8000")
    p.add_argument("--db-url", default=os.environ.get("DATABASE_URL", "postgresql://soup:soup@localhost:55432/soup"))
    p.add_argument("--rps", type=float, default=100)
    p.add_argument("--duration", type=float, default=120)
    p.add_argument("--min-ms", type=int, default=20)
    p.add_argument("--max-ms", type=int, default=80)
    p.add_argument("--mix", default="create=0.5,get=0.3,list=0.15,cancel=0.05")
    p.add_argument("--label", default="")
    p.add_argument("--drain-timeout", type=float, default=120)
    a = p.parse_args()

    mix = {k: float(v) for k, v in (x.split("=") for x in a.mix.split(","))}
    ops, weights = list(mix), list(mix.values())
    n = next_run_number("load")
    name = f"load-{n:03d}-{int(a.rps)}rps" + (f"-{a.label}" if a.label else "")
    out = Tee(RAW_DIR / f"{name}.txt")
    out(f"== {name}  {utc_stamp()}  code={code_fingerprint()}")
    out(f"target={a.rps} rps for {a.duration}s mix={mix} job_ms={a.min_ms}-{a.max_ms} api={a.api}")

    rng = random.Random()
    run_id = uuid.uuid4().hex[:6]
    lat: dict[str, list[float]] = defaultdict(list)
    status: dict[str, Counter] = defaultdict(Counter)
    errors: Counter = Counter()
    ids: list[str] = []
    keys: list[str] = []
    in_flight = 0
    max_in_flight = 0

    aconn = await psycopg.AsyncConnection.connect(a.db_url, autocommit=True, row_factory=dict_row)

    async def sample() -> dict:
        r = await (await aconn.execute(
            """SELECT (SELECT count(*) FROM jobs WHERE state='queued') AS queued,
                      (SELECT count(*) FROM jobs WHERE state='running') AS running,
                      (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()) AS conns""")).fetchone()
        return r

    async with httpx.AsyncClient(base_url=a.api, timeout=10.0,
                                 limits=httpx.Limits(max_connections=1000, max_keepalive_connections=200)) as client:

        async def one(op: str) -> None:
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            t0 = time.monotonic()
            try:
                if op == "create" or not ids:
                    op = "create"
                    if keys and rng.random() < 0.1:
                        key = rng.choice(keys)  # client retry of an earlier request
                    else:
                        key = f"load-{run_id}-{len(keys)}"
                        keys.append(key)
                    payload = {"duration_ms": abs(hash(key)) % (a.max_ms - a.min_ms + 1) + a.min_ms}
                    r = await client.post("/jobs", json={"idempotency_key": key, "payload": payload})
                    if r.status_code in (200, 201):
                        ids.append(r.json()["id"])
                elif op == "get":
                    r = await client.get(f"/jobs/{rng.choice(ids)}")
                elif op == "list":
                    params = {"limit": 50}
                    if rng.random() < 0.5:
                        params["state"] = rng.choice(["queued", "succeeded"])
                    r = await client.get("/jobs", params=params)
                else:
                    r = await client.post(f"/jobs/{rng.choice(ids)}/cancel")
                status[op][r.status_code] += 1
                lat[op].append((time.monotonic() - t0) * 1000)
            except httpx.HTTPError as e:
                errors[f"{op}:{type(e).__name__}"] += 1
            finally:
                in_flight -= 1

        tasks = []
        t_start = time.monotonic()
        total = int(a.rps * a.duration)
        next_sample = t_start
        samples = []
        for k in range(total):
            target = t_start + k / a.rps
            now = time.monotonic()
            if now >= next_sample:
                s = await sample()
                samples.append(s["queued"])
                out(f"t={now - t_start:6.1f}s sent={k:6d} in_flight={in_flight:4d} queued={s['queued']:6d} "
                    f"running={s['running']:3d} db_conns={s['conns']:3d}")
                next_sample += 10
            if target > now:
                await asyncio.sleep(target - now)
            tasks.append(asyncio.create_task(one(rng.choices(ops, weights)[0])))
        send_lag = time.monotonic() - (t_start + total / a.rps)
        await asyncio.gather(*tasks)
        elapsed = time.monotonic() - t_start

        t_drain = time.monotonic()
        while True:
            s = await sample()
            if s["queued"] == 0 and s["running"] == 0:
                drain = time.monotonic() - t_drain
                break
            if time.monotonic() - t_drain > a.drain_timeout:
                drain = None
                break
            await asyncio.sleep(0.5)
    await aconn.close()

    sent = sum(sum(c.values()) for c in status.values()) + sum(errors.values())
    out("")
    out("== summary")
    out(f"requests={sent} wall={elapsed:.1f}s achieved={sent / elapsed:.1f} rps generator_lag_at_end={send_lag:.2f}s "
        f"max_in_flight={max_in_flight}")
    for op in ops:
        xs = lat.get(op, [])
        if xs:
            out(f"{op:7s} n={len(xs):6d} p50={statistics.median(xs):7.1f}ms p95={pct(xs, 95):7.1f}ms "
                f"p99={pct(xs, 99):7.1f}ms max={max(xs):7.1f}ms status={dict(status[op])}")
    non2xx = sum(n for op in status for code, n in status[op].items() if code >= 500)
    out(f"transport errors={dict(errors)} 5xx={non2xx}")
    out(f"queue depth samples (every 10s): {samples}")
    out(f"backlog drain after load stopped: {f'{drain:.1f}s' if drain is not None else 'NOT drained within timeout'}")
    out.close()


if __name__ == "__main__":
    asyncio.run(main())
