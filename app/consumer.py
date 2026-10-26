"""Standalone rejection-event consumer: tails the Redis Stream `events.py`
writes to and fires a webhook once a key crosses a rejection threshold
within a sliding window. Runs as its own process (docker-compose service
`alerter`), never inside the FastAPI app -- alerting must never sit on the
/check hot path.

Uses XREADGROUP with a consumer group, not a plain XREAD, so a crashed
consumer resumes from where it left off (unacked entries get redelivered)
instead of silently missing whatever arrived while it was down.
"""

import argparse
import asyncio
import logging
import os

import httpx
import redis.asyncio as redis

from app.events import STREAM_KEY

logger = logging.getLogger("rateguard.alerter")

CONSUMER_GROUP = "alerters"
CONSUMER_NAME = "alerter-1"

ALERT_THRESHOLD = 5  # rejections...
ALERT_WINDOW_SECONDS = 60  # ...within this many seconds...
ALERT_COOLDOWN_SECONDS = 300  # ...then stay quiet on this key for this long


async def ensure_group(client: redis.Redis) -> None:
    try:
        await client.xgroup_create(STREAM_KEY, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def send_alert(webhook_url: str | None, key: str, algorithm: str, count: int) -> None:
    message = f"RateGuard alert: key '{key}' ({algorithm}) rejected {count}x in the last {ALERT_WINDOW_SECONDS}s"
    if webhook_url:
        async with httpx.AsyncClient(timeout=5.0) as http_client:
            await http_client.post(webhook_url, json={"text": message})
    else:
        logger.warning(message)


async def process_entry(client: redis.Redis, webhook_url: str | None, fields: dict[str, str]) -> None:
    key = fields["key"]
    algorithm = fields["algorithm"]

    cooldown_key = f"rl:alert_cooldown:{key}"
    if await client.exists(cooldown_key):
        return

    count_key = f"rl:alert_count:{key}"
    count = await client.incr(count_key)
    if count == 1:
        await client.expire(count_key, ALERT_WINDOW_SECONDS)

    if count >= ALERT_THRESHOLD:
        await send_alert(webhook_url, key, algorithm, count)
        await client.set(cooldown_key, "1", ex=ALERT_COOLDOWN_SECONDS)
        await client.delete(count_key)


async def process_available_entries(client: redis.Redis, webhook_url: str | None) -> int:
    """One read+process pass. Returns the number of entries processed --
    kept separate from `run()`'s infinite loop so it's directly testable.
    """
    response = await client.xreadgroup(CONSUMER_GROUP, CONSUMER_NAME, {STREAM_KEY: ">"}, count=100, block=100)
    if not response:
        return 0

    processed = 0
    for _stream_key, entries in response:
        for entry_id, fields in entries:
            await process_entry(client, webhook_url, fields)
            await client.xack(STREAM_KEY, CONSUMER_GROUP, entry_id)
            processed += 1
    return processed


async def run(redis_url: str, webhook_url: str | None) -> None:
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    await ensure_group(client)
    logger.info(
        "Alerter started, watching %s (threshold=%d/%ds, cooldown=%ds, webhook=%s)",
        STREAM_KEY,
        ALERT_THRESHOLD,
        ALERT_WINDOW_SECONDS,
        ALERT_COOLDOWN_SECONDS,
        "configured" if webhook_url else "none (logging instead)",
    )
    while True:
        await process_available_entries(client, webhook_url)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--webhook-url", default=os.environ.get("ALERT_WEBHOOK_URL"))
    args = parser.parse_args()
    asyncio.run(run(args.redis_url, args.webhook_url))
