from dataclasses import dataclass
from typing import Protocol

import redis.asyncio as redis


@dataclass(frozen=True, slots=True)
class Decision:
    allowed: bool
    limit: int
    remaining: int
    reset: int
    retry_after: int


class RateLimiter(Protocol):
    async def __call__(
        self,
        client: redis.Redis,
        key: str,
        limit: int,
        window: int,
        burst: int | None = None,
    ) -> Decision: ...
