import redis.asyncio as redis

STREAM_KEY = "rl:events:rejections"
MAX_STREAM_LENGTH = 10_000


async def publish_rejection(client: redis.Redis, algorithm: str, key: str) -> None:
    """Fire-and-forget: append a rejection event to a capped Redis Stream.

    The entry ID is left as "*" so Redis assigns it from its own clock --
    same principle as every Lua script's use of TIME, never trust an
    app-side clock, even for a timestamp that isn't safety-critical.

    Deliberately not atomic with the rate-limit decision itself: losing an
    occasional event on a crash is an acceptable trade-off for an
    observability signal, unlike the rate-limit state, which must be exact.
    A real consumer (alerting, a dashboard) tails this stream independently,
    off the /check request path -- this only ever writes to it.
    """
    await client.xadd(
        STREAM_KEY,
        {"algorithm": algorithm, "key": key},
        maxlen=MAX_STREAM_LENGTH,
        approximate=True,
    )


async def recent_rejections(client: redis.Redis, limit: int = 50) -> list[dict[str, str]]:
    entries = await client.xrevrange(STREAM_KEY, count=limit)
    return [{"id": entry_id, **fields} for entry_id, fields in entries]
