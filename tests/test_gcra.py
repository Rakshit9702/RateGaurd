import asyncio

import redis.asyncio as redis

from app.algorithms import gcra


async def test_concurrency_bounded(redis_client: redis.Redis) -> None:
    limit = 5
    window = 5  # emission_interval = 1s, burst defaults to limit
    key = "concurrency-gcra"

    results = await asyncio.gather(*[gcra.check(redis_client, key, limit, window) for _ in range(limit * 10)])
    allowed_count = sum(1 for decision in results if decision.allowed)

    assert allowed_count == limit


async def test_pacing(redis_client: redis.Redis) -> None:
    limit = 5
    window = 5  # emission_interval = 1s
    key = "pacing-gcra"

    first = await gcra.check(redis_client, key, limit, window)
    assert first.allowed

    for _ in range(3):
        await asyncio.sleep(1.05)
        decision = await gcra.check(redis_client, key, limit, window)
        assert decision.allowed


async def test_retry_after_accuracy(redis_client: redis.Redis) -> None:
    limit = 5
    window = 5
    key = "retry-gcra"

    for _ in range(limit):
        decision = await gcra.check(redis_client, key, limit, window)
        assert decision.allowed

    decision = await gcra.check(redis_client, key, limit, window)
    assert not decision.allowed
    assert decision.retry_after > 0

    await asyncio.sleep(decision.retry_after + 0.2)

    decision = await gcra.check(redis_client, key, limit, window)
    assert decision.allowed
