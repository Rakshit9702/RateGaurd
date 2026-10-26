"""Deliberately non-atomic fixed-window counter.

Not wired into the HTTP API. Exists only so its check-then-act race
(GET the count, decide, then INCR) can be isolated, tested, and captured
before being superseded by the Lua-atomic version in app/algorithms/fixed_window.py.
"""

import urllib.parse

import redis.asyncio as redis

from app.algorithms.base import Decision


async def check(
    client: redis.Redis,
    key: str,
    limit: int,
    window: int,
    burst: int | None = None,
) -> Decision:
    quoted_key = urllib.parse.quote(key, safe="")

    server_time = await client.time()
    now = server_time[0] + server_time[1] / 1_000_000
    window_index = int(now // window)
    full_key = f"rl:fw_naive:{quoted_key}:{window_index}"

    raw = await client.get(full_key)
    count = int(raw) if raw is not None else 0

    allowed = count < limit
    if allowed:
        count = await client.incr(full_key)
        if count == 1:
            await client.expire(full_key, window)

    reset = (window_index + 1) * window
    remaining = max(0, limit - count)
    retry_after = 0 if allowed else int(reset - now)

    return Decision(
        allowed=allowed,
        limit=limit,
        remaining=remaining,
        reset=reset,
        retry_after=retry_after,
    )
