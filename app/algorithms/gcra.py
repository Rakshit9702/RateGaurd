"""GCRA: stores a single number per key, the theoretical arrival time (TAT)
of the next permitted request. O(1) memory, smooth pacing, and an exact
Retry-After for free.
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
    base_key = f"rl:gcra:{quoted_key}"

    resolved_burst = burst or limit

    script = get_script("gcra")
    allowed, remaining, limit_result, reset, retry_after = await script(
        keys=[base_key], args=[limit, window, resolved_burst], client=client
    )

    return Decision(
        allowed=bool(allowed),
        limit=int(limit_result),
        remaining=int(remaining),
        reset=int(reset),
        retry_after=int(retry_after),
    )
