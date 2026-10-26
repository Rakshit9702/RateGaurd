from collections.abc import AsyncIterator

import redis.asyncio as redis

from app.config import settings

_pool: redis.ConnectionPool | None = None


def get_pool() -> redis.ConnectionPool:
    global _pool
    if _pool is None:
        _pool = redis.ConnectionPool.from_url(settings.redis_url, decode_responses=True)
    return _pool


def get_redis_client() -> redis.Redis:
    return redis.Redis(connection_pool=get_pool())


async def get_redis() -> AsyncIterator[redis.Redis]:
    yield get_redis_client()


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.disconnect()
        _pool = None
