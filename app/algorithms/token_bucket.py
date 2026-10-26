"""Token bucket: stores token count + last-refill timestamp, two values
per key regardless of limit. Refills lazily on read, not via a background job.
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
    base_key = f"rl:tb:{quoted_key}"

    capacity = burst or limit
    refill_rate = limit / window

    script = get_script("token_bucket")
    allowed, remaining, capacity_result, reset, retry_after = await script(
        keys=[base_key], args=[capacity, refill_rate], client=client
    )

    return Decision(
        allowed=bool(allowed),
        limit=int(capacity_result),
        remaining=int(remaining),
        reset=int(reset),
        retry_after=int(retry_after),
    )
