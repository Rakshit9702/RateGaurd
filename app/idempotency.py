import hashlib
import json
from typing import Any

import redis.asyncio as redis

IDEMPOTENCY_KEY_PREFIX = "idempotency:"
IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60


class IdempotencyConflict(Exception):
    """Same Idempotency-Key reused with a different request body."""


def _body_hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


async def get_cached_response(client: redis.Redis, idempotency_key: str, body: bytes) -> dict[str, Any] | None:
    key = f"{IDEMPOTENCY_KEY_PREFIX}{idempotency_key}"
    raw = await client.get(key)
    if raw is None:
        return None

    record = json.loads(raw)
    if record["body_hash"] != _body_hash(body):
        raise IdempotencyConflict()
    return record["response"]


async def store_response(client: redis.Redis, idempotency_key: str, body: bytes, response: dict[str, Any]) -> None:
    key = f"{IDEMPOTENCY_KEY_PREFIX}{idempotency_key}"
    record = {"body_hash": _body_hash(body), "response": response}
    await client.set(key, json.dumps(record), ex=IDEMPOTENCY_TTL_SECONDS)
