"""Atomic fixed-window counter: the naive check-then-act race from
fixed_window_naive.py moved into a single Lua/EVALSHA round-trip.
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
    base_key = f"rl:fw:{quoted_key}"

    script = get_script("fixed_window")
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
