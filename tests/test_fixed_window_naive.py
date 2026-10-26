import asyncio
import json
from pathlib import Path

import pytest
import redis.asyncio as redis

from app.algorithms import fixed_window_naive

ARTIFACTS_DIR = Path(__file__).parent / "artifacts"


@pytest.mark.xfail(strict=True, reason="naive fixed-window is a non-atomic check-then-act race")
async def test_concurrency_over_admits_naive_fixed_window(redis_client: redis.Redis) -> None:
    limit = 10
    window = 60
    key = "concurrency-naive"

    results = await asyncio.gather(
        *[fixed_window_naive.check(redis_client, key, limit, window) for _ in range(limit * 10)]
    )
    allowed_count = sum(1 for decision in results if decision.allowed)

    ARTIFACTS_DIR.mkdir(exist_ok=True)
    (ARTIFACTS_DIR / "naive_race_result.json").write_text(
        json.dumps(
            {
                "limit": limit,
                "requests_sent": limit * 10,
                "allowed_count": allowed_count,
                "over_admit_count": max(0, allowed_count - limit),
            },
            indent=2,
        )
    )

    assert allowed_count == limit
