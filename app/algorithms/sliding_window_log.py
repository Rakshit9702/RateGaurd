"""Sliding window log: a Redis sorted set of request timestamps per key.
Perfectly accurate at boundaries, at the cost of O(limit) memory per key.
"""

import urllib.parse

import redis.asyncio as redis

from app.algorithms.base import Decision
from app.scripts_loader import get_script


async def check(
    client: redis.Redis,
    key: str,
    limit: int,
    window: int,
    burst: int | None = None,
) -> Decision:
    quoted_key = urllib.parse.quote(key, safe="")
    base_key = f"rl:swl:{quoted_key}"

    script = get_script("sliding_window_log")
    allowed, remaining, limit_result, reset, retry_after = await script(
        keys=[base_key], args=[limit, window], client=client
    )

    return Decision(
        allowed=bool(allowed),
        limit=int(limit_result),
        remaining=int(remaining),
        reset=int(reset),
        retry_after=int(retry_after),
    )
