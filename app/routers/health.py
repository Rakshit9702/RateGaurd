import asyncpg
import redis.asyncio as redis
from fastapi import APIRouter, Depends

from app.db import get_db
from app.redis_client import get_redis

router = APIRouter()


@router.get("/healthz")
async def healthz(client: redis.Redis = Depends(get_redis), db: asyncpg.Pool = Depends(get_db)) -> dict[str, str]:
    await client.ping()
    async with db.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok"}
