import asyncpg
import redis.asyncio as redis
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from app.algorithms import REGISTRY
from app.db import get_db
from app.events import publish_rejection
from app.policies import get_policy
from app.redis_client import get_redis
from app.schemas import CheckRequest, CheckResponse
from app.shard_router import get_client_for_key

router = APIRouter()


@router.post("/check")
async def check(
    payload: CheckRequest, client: redis.Redis = Depends(get_redis), db: asyncpg.Pool = Depends(get_db)
) -> JSONResponse:
    """Consume one unit of quota for `key`. Deliberately NOT idempotent --
    consuming quota is the entire point, so replaying a request must count again.
    """
    if payload.policy is not None:
        policy = await get_policy(db, payload.policy)
        if policy is None:
            raise HTTPException(status_code=404, detail=f"policy '{payload.policy}' not found")
        algorithm, limit, window, burst = policy.algorithm, policy.limit, policy.window, policy.burst
    else:
        assert payload.algorithm is not None
        assert payload.limit is not None
        assert payload.window is not None
        algorithm, limit, window, burst = payload.algorithm, payload.limit, payload.window, payload.burst

    check_fn = REGISTRY[algorithm]
    shard_client = get_client_for_key(payload.key)
    decision = await check_fn(shard_client, payload.key, limit, window, burst)

    body = CheckResponse(
        allowed=decision.allowed,
        limit=decision.limit,
        remaining=decision.remaining,
        reset=decision.reset,
        retry_after=decision.retry_after,
        algorithm=algorithm,
        key=payload.key,
    )

    headers = {
        "X-RateLimit-Limit": str(decision.limit),
        "X-RateLimit-Remaining": str(decision.remaining),
        "X-RateLimit-Reset": str(decision.reset),
    }
    status_code = 200
    if not decision.allowed:
        status_code = 429
        headers["Retry-After"] = str(decision.retry_after)
        await publish_rejection(client, algorithm, payload.key)

    return JSONResponse(status_code=status_code, content=body.model_dump(), headers=headers)
