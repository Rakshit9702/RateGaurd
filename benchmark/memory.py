"""Measurement 2: Redis MEMORY USAGE per key at limits of 10, 100, and 1000.
This is where sliding-window-log's O(limit) growth shows up against the
flat O(1) cost of the other three. Reads Redis directly (MEMORY USAGE
isn't exposed over HTTP) alongside driving traffic through the live server.
"""

import argparse
import asyncio

import httpx
import redis.asyncio as redis

from benchmark.common import ALGORITHMS, DEFAULT_BASE_URL, DEFAULT_REDIS_URL, RESULTS_DIR, get_redis_client, write_csv

LIMITS = [10, 100, 1000]
WINDOW = 3600  # long window so population traffic can't self-evict mid-run

KEY_PATTERNS = {
    "fixed_window": "rl:fw:{key}:*",
    "sliding_window_log": "rl:swl:{key}",
    "token_bucket": "rl:tb:{key}",
    "gcra": "rl:gcra:{key}",
}


async def _resolve_redis_key(redis_client: redis.Redis, algorithm: str, key: str) -> str | None:
    pattern = KEY_PATTERNS[algorithm].format(key=key)
    if "*" in pattern:
        keys = [k async for k in redis_client.scan_iter(match=pattern)]
        return keys[0] if keys else None
    exists = await redis_client.exists(pattern)
    return pattern if exists else None


async def measure_memory(base_url: str, redis_url: str) -> list[dict]:
    redis_client = get_redis_client(redis_url)
    rows = []

    async with httpx.AsyncClient(
        base_url=base_url, timeout=30.0, limits=httpx.Limits(max_connections=100)
    ) as client:
        for algorithm in ALGORITHMS:
            for limit in LIMITS:
                key = f"bench-memory-{algorithm}-{limit}"
                payload = {"key": key, "algorithm": algorithm, "limit": limit, "window": WINDOW}

                # Concurrent, not sequential: the atomic Lua scripts guarantee
                # no more than `limit` get admitted even under a concurrent flood
                # (proven in tests/test_*.py), so this reaches the same end state
                # as `limit` sequential calls, just far faster at limit=1000.
                await asyncio.gather(*[client.post("/check", json=payload) for _ in range(limit)])

                redis_key = await _resolve_redis_key(redis_client, algorithm, key)
                key_bytes = await redis_client.memory_usage(redis_key) if redis_key else 0

                rows.append({"algorithm": algorithm, "limit": limit, "key_bytes": key_bytes})

    await redis_client.aclose()
    return rows


async def run(base_url: str, redis_url: str) -> list[dict]:
    rows = await measure_memory(base_url, redis_url)
    write_csv(RESULTS_DIR / "memory.csv", rows)
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    args = parser.parse_args()

    for row in asyncio.run(run(args.base_url, args.redis_url)):
        print(row)
