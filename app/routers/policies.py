import asyncpg
import redis.asyncio as redis
from fastapi import APIRouter, Depends, Header, HTTPException, Request

from app.db import get_db
from app.idempotency import IdempotencyConflict, get_cached_response, store_response
from app.policies import create_policy, get_policy, get_policy_history
from app.redis_client import get_redis
from app.schemas import PolicyCreateRequest, PolicyHistoryEntry, PolicyResponse

router = APIRouter()


@router.post("/policies", response_model=PolicyResponse, status_code=201)
async def create_policy_endpoint(
    payload: PolicyCreateRequest,
    request: Request,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    redis_client: redis.Redis = Depends(get_redis),
    db: asyncpg.Pool = Depends(get_db),
) -> PolicyResponse:
    # Idempotency-key dedup is inherently time-bounded (24h TTL) and stays in
    # Redis, which is exactly its sweet spot. The policy itself is durable
    # config, not a TTL'd cache entry -- that's what's now in Postgres.
    body = await request.body()

    try:
        cached = await get_cached_response(redis_client, idempotency_key, body)
    except IdempotencyConflict:
        raise HTTPException(
            status_code=409, detail="Idempotency-Key already used with a different request body"
        ) from None

    if cached is not None:
        return PolicyResponse.model_validate(cached)

    await create_policy(db, payload, idempotency_key=idempotency_key)
    response = PolicyResponse(
        name=payload.name,
        algorithm=payload.algorithm,
        limit=payload.limit,
        window=payload.window,
        burst=payload.burst,
    )
    await store_response(redis_client, idempotency_key, body, response.model_dump())
    return response


@router.get("/policies/{name}", response_model=PolicyResponse)
async def get_policy_endpoint(name: str, db: asyncpg.Pool = Depends(get_db)) -> PolicyResponse:
    policy = await get_policy(db, name)
    if policy is None:
        raise HTTPException(status_code=404, detail=f"policy '{name}' not found")
    return policy


@router.get("/policies/{name}/history", response_model=list[PolicyHistoryEntry])
async def get_policy_history_endpoint(
    name: str, limit: int = 50, db: asyncpg.Pool = Depends(get_db)
) -> list[PolicyHistoryEntry]:
    """Every create/update of this policy, most recent first. Returns an
    empty list for a policy that's never existed -- same as an existing
    policy with no history wouldn't be distinguishable anyway, so this
    endpoint doesn't 404 the way GET /policies/{name} does.
    """
    return await get_policy_history(db, name, limit)
