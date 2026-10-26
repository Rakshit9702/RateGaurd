import asyncpg

from app.schemas import PolicyCreateRequest, PolicyHistoryEntry, PolicyResponse

# Upsert (not plain INSERT): matches the prior Redis-SET-backed behavior,
# where re-registering an existing name silently replaced it -- swapping the
# storage backend shouldn't introduce a new error case that wasn't there before.
#
# `xmax = 0` is a standard Postgres idiom for telling INSERT and the
# ON CONFLICT DO UPDATE branch apart from the RETURNING clause alone: a
# freshly inserted row's xmax is unset (0); a row touched by the UPDATE
# branch gets its xmax set to the current transaction. No separate SELECT
# needed to know which branch fired -- which matters here because the audit
# row's `action` depends on it.
_UPSERT_SQL = """
INSERT INTO policies (name, algorithm, rate_limit, window_seconds, burst)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (name) DO UPDATE SET
    algorithm = EXCLUDED.algorithm,
    rate_limit = EXCLUDED.rate_limit,
    window_seconds = EXCLUDED.window_seconds,
    burst = EXCLUDED.burst
RETURNING (xmax = 0) AS inserted
"""

_INSERT_AUDIT_SQL = """
INSERT INTO policy_audit_log (policy_name, action, algorithm, rate_limit, window_seconds, burst, idempotency_key)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"""

_SELECT_SQL = "SELECT name, algorithm, rate_limit, window_seconds, burst FROM policies WHERE name = $1"

_SELECT_HISTORY_SQL = """
SELECT action, algorithm, rate_limit, window_seconds, burst, idempotency_key, changed_at
FROM policy_audit_log
WHERE policy_name = $1
ORDER BY changed_at DESC, id DESC
LIMIT $2
"""


async def create_policy(pool: asyncpg.Pool, policy: PolicyCreateRequest, idempotency_key: str | None = None) -> None:
    async with pool.acquire() as conn, conn.transaction():
        inserted = await conn.fetchval(
            _UPSERT_SQL, policy.name, policy.algorithm, policy.limit, policy.window, policy.burst
        )
        await conn.execute(
            _INSERT_AUDIT_SQL,
            policy.name,
            "created" if inserted else "updated",
            policy.algorithm,
            policy.limit,
            policy.window,
            policy.burst,
            idempotency_key,
        )


async def get_policy(pool: asyncpg.Pool, name: str) -> PolicyResponse | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(_SELECT_SQL, name)
    if row is None:
        return None
    return PolicyResponse(
        name=row["name"],
        algorithm=row["algorithm"],
        limit=row["rate_limit"],
        window=row["window_seconds"],
        burst=row["burst"],
    )


async def get_policy_history(pool: asyncpg.Pool, name: str, limit: int = 50) -> list[PolicyHistoryEntry]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(_SELECT_HISTORY_SQL, name, limit)
    return [
        PolicyHistoryEntry(
            action=row["action"],
            algorithm=row["algorithm"],
            limit=row["rate_limit"],
            window=row["window_seconds"],
            burst=row["burst"],
            idempotency_key=row["idempotency_key"],
            changed_at=row["changed_at"],
        )
        for row in rows
    ]
