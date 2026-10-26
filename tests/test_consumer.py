import redis.asyncio as redis

from app.consumer import ALERT_THRESHOLD, ensure_group, process_available_entries
from app.events import publish_rejection


async def test_below_threshold_does_not_alert(redis_client: redis.Redis, webhook_receiver: tuple[list, str]) -> None:
    received, webhook_url = webhook_receiver
    await ensure_group(redis_client)

    for _ in range(ALERT_THRESHOLD - 1):
        await publish_rejection(redis_client, "gcra", "below-threshold-key")

    processed = await process_available_entries(redis_client, webhook_url)

    assert processed == ALERT_THRESHOLD - 1
    assert received == []


async def test_crossing_threshold_fires_webhook(redis_client: redis.Redis, webhook_receiver: tuple[list, str]) -> None:
    received, webhook_url = webhook_receiver
    await ensure_group(redis_client)

    for _ in range(ALERT_THRESHOLD):
        await publish_rejection(redis_client, "token_bucket", "over-threshold-key")

    await process_available_entries(redis_client, webhook_url)

    assert len(received) == 1
    assert "over-threshold-key" in received[0]["text"]
    assert "token_bucket" in received[0]["text"]


async def test_cooldown_suppresses_repeat_alerts(redis_client: redis.Redis, webhook_receiver: tuple[list, str]) -> None:
    received, webhook_url = webhook_receiver
    await ensure_group(redis_client)

    for _ in range(ALERT_THRESHOLD * 3):
        await publish_rejection(redis_client, "gcra", "cooldown-key")

    await process_available_entries(redis_client, webhook_url)

    # ALERT_THRESHOLD * 3 events, threshold ALERT_THRESHOLD: only the first
    # crossing should fire -- the cooldown set immediately after must
    # suppress every subsequent event in the same batch.
    assert len(received) == 1


async def test_no_webhook_url_logs_instead_of_raising(
    redis_client: redis.Redis, caplog
) -> None:
    await ensure_group(redis_client)

    for _ in range(ALERT_THRESHOLD):
        await publish_rejection(redis_client, "fixed_window", "log-fallback-key")

    with caplog.at_level("WARNING", logger="rateguard.alerter"):
        await process_available_entries(redis_client, webhook_url=None)

    assert any("log-fallback-key" in record.message for record in caplog.records)


async def test_entries_are_acked_not_redelivered(redis_client: redis.Redis, webhook_receiver: tuple[list, str]) -> None:
    _received, webhook_url = webhook_receiver
    await ensure_group(redis_client)

    await publish_rejection(redis_client, "gcra", "ack-check-key")
    first_pass = await process_available_entries(redis_client, webhook_url)
    second_pass = await process_available_entries(redis_client, webhook_url)

    assert first_pass == 1
    assert second_pass == 0
