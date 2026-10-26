#!/usr/bin/env python3
"""Proves the rate limit holds globally across app instances sharing one
Redis, not per-instance -- the entire reason the state lives in Redis
rather than in each process. Round-robins requests across three app
instances (docker-compose app1/app2/app3) against the same key and
asserts total admitted equals the configured limit, not 3x.
"""

import asyncio
import sys

import httpx
import redis.asyncio as redis

INSTANCE_URLS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]

REDIS_URL = "redis://localhost:6379/0"
KEY = "multi-instance-verify"
ALGORITHM = "gcra"
LIMIT = 10
WINDOW = 60


async def wait_for_instances(clients: list[httpx.AsyncClient]) -> None:
    for client in clients:
        for _attempt in range(30):
            try:
                resp = await client.get("/healthz")
                if resp.status_code == 200:
                    break
            except httpx.TransportError:
                pass
            await asyncio.sleep(1)
        else:
            raise RuntimeError(f"{client.base_url} never became healthy")


async def main() -> int:
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.delete(f"rl:gcra:{KEY}")
    await redis_client.aclose()

    clients = [httpx.AsyncClient(base_url=url, timeout=10.0) for url in INSTANCE_URLS]
    await wait_for_instances(clients)

    payload = {"key": KEY, "algorithm": ALGORITHM, "limit": LIMIT, "window": WINDOW}
    per_instance_allowed = [0] * len(clients)

    total_requests = LIMIT * 3
    for i in range(total_requests):
        client = clients[i % len(clients)]
        resp = await client.post("/check", json=payload)
        if resp.json()["allowed"]:
            per_instance_allowed[i % len(clients)] += 1

    for client in clients:
        await client.aclose()

    total_allowed = sum(per_instance_allowed)

    print("Per-instance breakdown:")
    for url, count in zip(INSTANCE_URLS, per_instance_allowed, strict=True):
        print(f"  {url}: {count} allowed")
    print(f"Total allowed across all instances: {total_allowed} (limit={LIMIT})")

    if total_allowed != LIMIT:
        print(f"FAIL: expected exactly {LIMIT} total admitted across all instances, got {total_allowed}")
        return 1

    print("PASS: limit held globally across 3 instances, not per-instance")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
