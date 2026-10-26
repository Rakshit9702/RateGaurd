import redis.asyncio as redis
from httpx import AsyncClient

from app.events import publish_rejection, recent_rejections


async def test_publish_and_read_rejection(redis_client: redis.Redis) -> None:
    await publish_rejection(redis_client, "gcra", "some-key")
    await publish_rejection(redis_client, "token_bucket", "another-key")

    events = await recent_rejections(redis_client, limit=10)

    assert len(events) == 2
    assert events[0]["algorithm"] == "token_bucket"  # xrevrange: most recent first
    assert events[1]["algorithm"] == "gcra"


async def test_check_rejection_appears_in_event_log(app_client: AsyncClient) -> None:
    payload = {"key": "event-log-test", "algorithm": "fixed_window", "limit": 1, "window": 60}
    await app_client.post("/check", json=payload)  # admitted, no event
    await app_client.post("/check", json=payload)  # rejected, publishes an event

    resp = await app_client.get("/events/rejections")
    assert resp.status_code == 200
    events = resp.json()
    assert any(e["key"] == "event-log-test" and e["algorithm"] == "fixed_window" for e in events)


async def test_allowed_requests_do_not_publish_events(app_client: AsyncClient) -> None:
    payload = {"key": "no-rejection-here", "algorithm": "gcra", "limit": 100, "window": 60}
    await app_client.post("/check", json=payload)

    resp = await app_client.get("/events/rejections")
    events = resp.json()
    assert not any(e["key"] == "no-rejection-here" for e in events)
