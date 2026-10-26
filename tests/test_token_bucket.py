import asyncio

import redis.asyncio as redis

from app.algorithms import token_bucket


async def test_concurrency_bounded(redis_client: redis.Redis) -> None:
    limit = 5
    window = 5
    key = "concurrency-tb"

    results = await asyncio.gather(
        *[token_bucket.check(redis_client, key, limit, window) for _ in range(limit * 10)]
    )
    allowed_count = sum(1 for decision in results if decision.allowed)

    assert allowed_count == limit


async def test_refill_over_time(redis_client: redis.Redis) -> None:
    limit = 5
    window = 5  # refill_rate = 1 token/sec
    key = "refill-tb"

    for _ in range(limit):
        decision = await token_bucket.check(redis_client, key, limit, window)
        assert decision.allowed

    decision = await token_bucket.check(redis_client, key, limit, window)
    assert not decision.allowed

    await asyncio.sleep(1.1)

    decision = await token_bucket.check(redis_client, key, limit, window)
    assert decision.allowed


async def test_retry_after_accuracy(redis_client: redis.Redis) -> None:
    limit = 5
    window = 5
    key = "retry-tb"

    for _ in range(limit):
        await token_bucket.check(redis_client, key, limit, window)

    decision = await token_bucket.check(redis_client, key, limit, window)
    assert not decision.allowed
    assert decision.retry_after > 0

    await asyncio.sleep(decision.retry_after + 0.2)

    decision = await token_bucket.check(redis_client, key, limit, window)
    assert decision.allowed
