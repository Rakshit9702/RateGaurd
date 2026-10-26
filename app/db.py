from collections.abc import AsyncIterator

import asyncpg

from app.config import settings

# Durable, low-write-volume config lives in Postgres -- unlike the ephemeral
# rate-limit counters (Redis), a registered policy is expected to survive a
# restart. `"limit"`/`"window"` are avoided as column names since both are
# reserved/ambiguous in SQL.
SCHEMA = """
CREATE TABLE IF NOT EXISTS policies (
    name TEXT PRIMARY KEY,
    algorithm TEXT NOT NULL,
    rate_limit INT NOT NULL,
    window_seconds INT NOT NULL,
    burst INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every create/update of a policy gets one row here, in the same
-- transaction as the write itself -- an audit trail is only trustworthy if
-- it can't silently fall out of sync with the table it's auditing.
CREATE TABLE IF NOT EXISTS policy_audit_log (
    id BIGSERIAL PRIMARY KEY,
    policy_name TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('created', 'updated')),
    algorithm TEXT NOT NULL,
    rate_limit INT NOT NULL,
    window_seconds INT NOT NULL,
    burst INT,
    idempotency_key TEXT,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_policy_audit_log_name ON policy_audit_log (policy_name, changed_at DESC);
"""

_pool: asyncpg.Pool | None = None


async def connect() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(settings.database_url)
        async with _pool.acquire() as conn:
            await conn.execute(SCHEMA)
    return _pool


def get_pool() -> asyncpg.Pool:
    assert _pool is not None, "Postgres pool not initialized -- connect() must run in the lifespan first"
    return _pool


async def get_db() -> AsyncIterator[asyncpg.Pool]:
    yield get_pool()


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
