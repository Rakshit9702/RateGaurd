"""Measurement 1: send exactly the quota across a window boundary and
count what got through, per algorithm. Same timing technique as
tests/test_boundary.py, run over HTTP against a live server instead of
calling algorithm functions directly.
"""

import argparse
import asyncio
import math

import httpx

from benchmark.common import ALGORITHMS, DEFAULT_BASE_URL, DEFAULT_REDIS_URL, RESULTS_DIR, get_redis_client, write_csv

LIMIT = 5
WINDOW = 4


async def measure_accuracy(base_url: str, redis_url: str) -> list[dict]:
    redis_client = get_redis_client(redis_url)
    rows = []

    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        for algorithm in ALGORITHMS:
            key = f"bench-accuracy-{algorithm}"
            payload = {"key": key, "algorithm": algorithm, "limit": LIMIT, "window": WINDOW}

            server_time = await redis_client.time()
            now = server_time[0] + server_time[1] / 1_000_000
            next_boundary = (math.floor(now / WINDOW) + 1) * WINDOW
            await asyncio.sleep(max(0.0, next_boundary - now - 0.3))

            first_batch = await asyncio.gather(*[client.post("/check", json=payload) for _ in range(LIMIT)])
            await asyncio.sleep(0.5)
            second_batch = await asyncio.gather(*[client.post("/check", json=payload) for _ in range(LIMIT)])

            total_sent = LIMIT * 2
            total_allowed = sum(1 for r in first_batch + second_batch if r.json()["allowed"])
            over_admit = max(0, total_allowed - LIMIT)

            rows.append(
                {
                    "algorithm": algorithm,
                    "limit": LIMIT,
                    "window": WINDOW,
                    "total_sent": total_sent,
                    "total_allowed": total_allowed,
                    "over_admit_count": over_admit,
                    "over_admit_pct": round(100 * over_admit / total_sent, 1),
                }
            )

    await redis_client.aclose()
    return rows


async def run(base_url: str, redis_url: str) -> list[dict]:
    rows = await measure_accuracy(base_url, redis_url)
    write_csv(RESULTS_DIR / "accuracy.csv", rows)
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    args = parser.parse_args()

    for row in asyncio.run(run(args.base_url, args.redis_url)):
        print(row)
