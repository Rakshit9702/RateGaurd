"""Boundary-flaw test, shared across algorithms as they're added.

Sends a full quota just before a window edge and again just after, and
checks that no more than `limit` total requests were allowed. This is an
algorithm-design flaw distinct from the concurrency race covered in
test_fixed_window_naive.py / test_fixed_window_lua.py: fixed-window stays
xfail here even after the Lua atomicity fix, because the flaw isn't about
atomicity, it's about the algorithm only ever looking at "this window."
"""

import asyncio
import math

import pytest
import redis.asyncio as redis

from app.algorithms import fixed_window, fixed_window_naive, sliding_window_log
from app.algorithms.base import RateLimiter

ALGORITHMS: dict[str, RateLimiter] = {
    "fixed_window_naive": fixed_window_naive.check,
    "fixed_window": fixed_window.check,
    "sliding_window_log": sliding_window_log.check,
}

# Algorithms expected to still exhibit the boundary flaw. fixed_window (Lua)
# stays here even after the atomicity fix: the boundary flaw is an
# algorithm-design limitation, not a concurrency bug, and is independent of it.
XFAIL_ALGORITHMS: set[str] = {"fixed_window_naive", "fixed_window"}

LIMIT = 5
WINDOW = 4


@pytest.mark.parametrize("algorithm_name", list(ALGORITHMS))
async def test_boundary(
    algorithm_name: str,
    redis_client: redis.Redis,
    request: pytest.FixtureRequest,
) -> None:
    if algorithm_name in XFAIL_ALGORITHMS:
        request.node.add_marker(
            pytest.mark.xfail(strict=True, reason="boundary flaw not fixed for this algorithm")
        )

    check = ALGORITHMS[algorithm_name]
    key = f"boundary-{algorithm_name}"

    server_time = await redis_client.time()
    now = server_time[0] + server_time[1] / 1_000_000
    next_boundary = (math.floor(now / WINDOW) + 1) * WINDOW

    await asyncio.sleep(max(0.0, next_boundary - now - 0.3))
    first_batch = await asyncio.gather(*[check(redis_client, key, LIMIT, WINDOW) for _ in range(LIMIT)])

    await asyncio.sleep(0.5)
    second_batch = await asyncio.gather(*[check(redis_client, key, LIMIT, WINDOW) for _ in range(LIMIT)])

    total_allowed = sum(1 for decision in first_batch + second_batch if decision.allowed)

    assert total_allowed <= LIMIT
