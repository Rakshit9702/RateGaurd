import redis.asyncio as redis
from fastapi import APIRouter, Depends, Query

from app.events import recent_rejections
from app.redis_client import get_redis

router = APIRouter()


@router.get("/events/rejections")
async def list_recent_rejections(
    limit: int = Query(default=50, gt=0, le=500), client: redis.Redis = Depends(get_redis)
) -> list[dict[str, str]]:
    """Reads the rejection-event stream. A real consumer (alerting, a
    dashboard) would tail this independently of any /check call -- this
    endpoint exists to make the stream inspectable, not as the intended
    consumption pattern.
    """
    return await recent_rejections(client, limit)
