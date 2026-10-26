"""Optional horizontal sharding for rate-limit hot-path state across
multiple independent Redis nodes, via consistent hashing (app/sharding.py).

Off by default -- REDIS_SHARD_URLS unset means every existing behavior,
test, and docker-compose service is completely unaffected; this is purely
additive. When set, `/check` routes each key's counter to the shard the
ring assigns it to, instead of the single default Redis.

Scope: only the rate-limit algorithm state gets sharded. The idempotency
cache, the policy registry, and the rejection-event stream all stay on the
single default Redis regardless -- they're low-volume/metadata concerns,
not the part that scales with key cardinality.
"""

import redis.asyncio as redis

from app.config import settings
from app.scripts_loader import load_scripts
from app.sharding import ConsistentHashRing

_ring: ConsistentHashRing | None = None
_shard_clients: dict[str, redis.Redis] = {}
_default_client: redis.Redis | None = None


def sharding_enabled() -> bool:
    return bool(settings.redis_shard_urls.strip())


async def connect(default_client: redis.Redis) -> None:
    global _ring, _shard_clients, _default_client
    _default_client = default_client

    if not sharding_enabled():
        return

    urls = [url.strip() for url in settings.redis_shard_urls.split(",") if url.strip()]
    _shard_clients = {url: redis.Redis.from_url(url, decode_responses=True) for url in urls}
    _ring = ConsistentHashRing(list(_shard_clients.keys()))

    # Every shard needs the Lua scripts loaded on it too -- EVALSHA only
    # works against a server that's actually had SCRIPT LOAD run on it.
    # Re-running load_scripts() per client is safe/idempotent: same source
    # text -> same SHA, it just re-registers the (functionally identical)
    # AsyncScript objects and re-issues SCRIPT LOAD, which Redis itself
    # treats as a no-op if the script is already cached.
    for client in _shard_clients.values():
        await load_scripts(client)


def get_client_for_key(key: str) -> redis.Redis:
    """The client the rate-limit check for `key` should run against --
    the routed shard if sharding is enabled, otherwise the single default
    client unchanged. Callers never need an `if sharding_enabled()` branch
    of their own.
    """
    assert _default_client is not None, "shard_router.connect() must run in the lifespan first"
    if _ring is None:
        return _default_client
    node_url = _ring.get_node(key)
    return _shard_clients[node_url]


def shard_urls() -> list[str]:
    return _ring.nodes if _ring is not None else []


async def close() -> None:
    global _ring, _shard_clients, _default_client
    for client in _shard_clients.values():
        await client.aclose()
    _shard_clients = {}
    _ring = None
    _default_client = None
