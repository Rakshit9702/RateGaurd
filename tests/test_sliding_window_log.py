import asyncio

import redis.asyncio as redis

from app.algorithms import sliding_window_log


async def test_concurrency_bounded(redis_client: redis.Redis) -> None:
    limit = 10
    window = 60
    key = "concurrency-swl"

    results = await asyncio.gather(
        *[sliding_window_log.check(redis_client, key, limit, window) for _ in range(limit * 10)]
    )
    allowed_count = sum(1 for decision in results if decision.allowed)

    assert allowed_count == limit
