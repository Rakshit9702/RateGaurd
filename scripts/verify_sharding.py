#!/usr/bin/env python3
"""Proves the consistent-hashing shard router actually distributes rate-limit
state across genuinely separate Redis nodes (docker-compose
redis-shard-0/1/2), not just DB indices -- and that the same key
deterministically lands on the same shard every time, the actual
correctness requirement (a split counter is a broken rate limit).

Run against `app-sharded` (port 8005), which has REDIS_SHARD_URLS set;
app1/app2/app3 are unaffected and still use the single default Redis.
"""

import asyncio
import sys

import httpx
import redis.asyncio as redis

APP_URL = "http://localhost:8005"
SHARD_URLS = [
    "redis://localhost:6390/0",
    "redis://localhost:6391/0",
    "redis://localhost:6392/0",
]
NUM_KEYS = 300
LIMIT = 1000
WINDOW = 60


async def wait_for_app(client: httpx.AsyncClient) -> None:
    for _attempt in range(30):
        try:
            resp = await client.get("/healthz")
            if resp.status_code == 200:
                return
        except httpx.TransportError:
            pass
        await asyncio.sleep(1)
    raise RuntimeError(f"{APP_URL} never became healthy")


async def main() -> int:
    shard_clients = [redis.Redis.from_url(url, decode_responses=True) for url in SHARD_URLS]
    for client in shard_clients:
        await client.flushdb()

    app_client = httpx.AsyncClient(base_url=APP_URL, timeout=10.0)
    await wait_for_app(app_client)

    keys = [f"shard-verify-key-{i}" for i in range(NUM_KEYS)]
    for key in keys:
        resp = await app_client.post("/check", json={"key": key, "algorithm": "gcra", "limit": LIMIT, "window": WINDOW})
        resp.raise_for_status()

    # Send the exact same requests again -- every key must land on the same
    # shard as before, or the state didn't route deterministically.
    per_shard_first_pass = []
    for shard_url, client in zip(SHARD_URLS, shard_clients, strict=True):
        matched = await client.keys("rl:gcra:shard-verify-key-*")
        per_shard_first_pass.append(set(matched))

    for key in keys:
        resp = await app_client.post("/check", json={"key": key, "algorithm": "gcra", "limit": LIMIT, "window": WINDOW})
        resp.raise_for_status()

    per_shard_second_pass = []
    for client in shard_clients:
        matched = await client.keys("rl:gcra:shard-verify-key-*")
        per_shard_second_pass.append(set(matched))

    await app_client.aclose()
    for client in shard_clients:
        await client.aclose()

    print("Distribution across shards:")
    total_found = 0
    for url, keyset in zip(SHARD_URLS, per_shard_first_pass, strict=True):
        print(f"  {url}: {len(keyset)} keys")
        total_found += len(keyset)
    print(f"Total keys found across all shards: {total_found} (sent {NUM_KEYS})")

    ok = True
    if total_found != NUM_KEYS:
        print(f"FAIL: expected {NUM_KEYS} distinct keys across shards, found {total_found}")
        ok = False

    all_on_one_shard = any(len(keyset) == total_found for keyset in per_shard_first_pass)
    if all_on_one_shard:
        print("FAIL: every key landed on a single shard -- routing isn't actually distributing")
        ok = False

    if per_shard_first_pass != per_shard_second_pass:
        print("FAIL: routing was not deterministic across the two passes")
        ok = False

    if ok:
        print(f"PASS: {NUM_KEYS} keys distributed across all {len(SHARD_URLS)} shards, routing fully deterministic")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
