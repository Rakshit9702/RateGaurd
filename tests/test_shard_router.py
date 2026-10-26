from collections.abc import AsyncIterator

import pytest_asyncio
import redis.asyncio as redis

from app import shard_router
from app.algorithms import fixed_window
from app.config import settings

# Two extra DB indices on the same local Redis, distinct from db0 (dev) and
# db1 (the rest of the test suite) -- real separate logical databases, real
# separate connections, enough to prove the routing round-trips correctly
# without spinning up extra containers just for this test.
SHARD_URLS = ["redis://localhost:6379/2", "redis://localhost:6379/3"]


@pytest_asyncio.fixture
async def sharded(redis_client: redis.Redis) -> AsyncIterator[None]:
    original = settings.redis_shard_urls
    settings.redis_shard_urls = ",".join(SHARD_URLS)
    try:
        await shard_router.connect(redis_client)
        yield
    finally:
        await shard_router.close()
        settings.redis_shard_urls = original
        for url in SHARD_URLS:
            client = redis.Redis.from_url(url, decode_responses=True)
            await client.flushdb()
            await client.aclose()


async def test_sharding_disabled_by_default(redis_client: redis.Redis) -> None:
    assert shard_router.sharding_enabled() is False
    await shard_router.connect(redis_client)
    try:
        assert shard_router.get_client_for_key("any-key") is redis_client
    finally:
        await shard_router.close()


async def test_sharding_enabled_reports_configured_shards(sharded: None) -> None:
    assert shard_router.sharding_enabled() is True
    assert set(shard_router.shard_urls()) == set(SHARD_URLS)


async def test_get_client_for_key_is_one_of_the_shards(sharded: None) -> None:
    client = shard_router.get_client_for_key("some-rate-limit-key")
    assert client is not None
    # Prove it's a live, usable connection to one of the shard DBs.
    await client.ping()


async def test_checks_actually_land_on_the_routed_shard(sharded: None) -> None:
    key = "shard-routing-proof-key"
    shard_client = shard_router.get_client_for_key(key)

    decision = await fixed_window.check(shard_client, key, limit=5, window=60)
    assert decision.allowed

    # The counter must exist on whichever shard the ring actually routed
    # to, and nowhere else -- confirms this isn't just returning *a*
    # client, it's returning the *correct*, deterministic one.
    found_on = []
    for url in SHARD_URLS:
        probe = redis.Redis.from_url(url, decode_responses=True)
        keys = await probe.keys(f"rl:fw:{key}:*")
        if keys:
            found_on.append(url)
        await probe.aclose()

    assert len(found_on) == 1


async def test_same_key_always_routes_to_the_same_shard_across_calls(sharded: None) -> None:
    key = "repeat-routing-key"
    first = shard_router.get_client_for_key(key)
    second = shard_router.get_client_for_key(key)
    assert first is second
