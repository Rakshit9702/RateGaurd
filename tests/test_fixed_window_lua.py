import asyncio

import redis.asyncio as redis

from app.algorithms import fixed_window


async def test_concurrency_atomic_fixed_window(redis_client: redis.Redis) -> None:
    limit = 10
    window = 60
    key = "concurrency-lua"

    results = await asyncio.gather(
        *[fixed_window.check(redis_client, key, limit, window) for _ in range(limit * 10)]
    )
    allowed_count = sum(1 for decision in results if decision.allowed)

    assert allowed_count == limit
