"""Measurement 3: throughput and p99 latency under concurrent load, per
algorithm. Distinct keys round-robin across workers so this measures raw
algorithm+Lua+Redis round-trip cost, not artificial single-key contention.
A very high limit keeps every request in the 200 path so latency isn't
skewed by the (cheaper) rejection branch.
"""

import argparse
import asyncio
import time

import httpx
import numpy as np

from benchmark.common import ALGORITHMS, DEFAULT_BASE_URL, RESULTS_DIR, write_csv

NUM_WORKERS = 50
NUM_KEYS = 100
REQUESTS_PER_WORKER = 20
HIGH_LIMIT = 1_000_000
WINDOW = 60


async def _worker(client: httpx.AsyncClient, algorithm: str, worker_id: int, latencies: list[float]) -> None:
    for i in range(REQUESTS_PER_WORKER):
        key = f"bench-throughput-{algorithm}-{(worker_id * REQUESTS_PER_WORKER + i) % NUM_KEYS}"
        payload = {"key": key, "algorithm": algorithm, "limit": HIGH_LIMIT, "window": WINDOW}
        start = time.perf_counter()
        await client.post("/check", json=payload)
        latencies.append(time.perf_counter() - start)


async def measure_throughput(base_url: str) -> list[dict]:
    rows = []
    async with httpx.AsyncClient(
        base_url=base_url, timeout=30.0, limits=httpx.Limits(max_connections=NUM_WORKERS)
    ) as client:
        for algorithm in ALGORITHMS:
            latencies: list[float] = []
            start = time.perf_counter()
            await asyncio.gather(*[_worker(client, algorithm, w, latencies) for w in range(NUM_WORKERS)])
            elapsed = time.perf_counter() - start

            total_requests = len(latencies)
            latencies_ms = [latency * 1000 for latency in latencies]

            rows.append(
                {
                    "algorithm": algorithm,
                    "total_requests": total_requests,
                    "elapsed_s": round(elapsed, 3),
                    "rps": round(total_requests / elapsed, 1),
                    "p50_ms": round(float(np.percentile(latencies_ms, 50)), 2),
                    "p95_ms": round(float(np.percentile(latencies_ms, 95)), 2),
                    "p99_ms": round(float(np.percentile(latencies_ms, 99)), 2),
                }
            )

    return rows


async def run(base_url: str) -> list[dict]:
    rows = await measure_throughput(base_url)
    write_csv(RESULTS_DIR / "throughput.csv", rows)
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    args = parser.parse_args()

    for row in asyncio.run(run(args.base_url)):
        print(row)
