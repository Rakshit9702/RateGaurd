import os
from collections.abc import AsyncIterator

TEST_REDIS_URL = "redis://localhost:6379/1"
TEST_DATABASE_URL = "postgresql://rateguard:rateguard@localhost:5432/rateguard_test"
os.environ["REDIS_URL"] = TEST_REDIS_URL  # must be set before app.config is imported anywhere
os.environ["DATABASE_URL"] = TEST_DATABASE_URL

import asyncio  # noqa: E402

import asyncpg  # noqa: E402
import pytest_asyncio  # noqa: E402
import redis.asyncio as redis  # noqa: E402
import uvicorn  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from starlette.applications import Starlette  # noqa: E402
from starlette.requests import Request  # noqa: E402
from starlette.responses import JSONResponse  # noqa: E402
from starlette.routing import Route  # noqa: E402

from app.db import SCHEMA  # noqa: E402
from app.scripts_loader import load_scripts  # noqa: E402

WEBHOOK_RECEIVER_PORT = 8999


@pytest_asyncio.fixture(scope="session", autouse=True)
async def _load_scripts() -> AsyncIterator[None]:
    client = redis.Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    await load_scripts(client)
    yield
    await client.aclose()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def _init_db_schema() -> AsyncIterator[None]:
    conn = await asyncpg.connect(TEST_DATABASE_URL)
    await conn.execute(SCHEMA)
    await conn.close()
    yield


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[redis.Redis]:
    client = redis.Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def db_pool() -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(TEST_DATABASE_URL)
    yield pool
    await pool.close()


@pytest_asyncio.fixture(autouse=True)
async def flush_redis(redis_client: redis.Redis) -> AsyncIterator[None]:
    await redis_client.flushdb()
    yield
    await redis_client.flushdb()


@pytest_asyncio.fixture(autouse=True)
async def flush_db() -> AsyncIterator[None]:
    conn = await asyncpg.connect(TEST_DATABASE_URL)
    await conn.execute("TRUNCATE policies, policy_audit_log")
    await conn.close()
    yield
    conn = await asyncpg.connect(TEST_DATABASE_URL)
    await conn.execute("TRUNCATE policies, policy_audit_log")
    await conn.close()


@pytest_asyncio.fixture
async def webhook_receiver() -> AsyncIterator[tuple[list[dict], str]]:
    """A real local HTTP server that records POSTed JSON bodies -- consistent
    with this project's "hit real infra, don't mock" philosophy: the
    consumer's webhook POST is a genuine network call to a genuine server,
    just one that exists only for the test instead of being a real Slack
    workspace.
    """
    received: list[dict] = []

    async def capture(request: Request) -> JSONResponse:
        received.append(await request.json())
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/webhook", capture, methods=["POST"])])
    config = uvicorn.Config(app, host="127.0.0.1", port=WEBHOOK_RECEIVER_PORT, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.02)

    yield received, f"http://127.0.0.1:{WEBHOOK_RECEIVER_PORT}/webhook"

    server.should_exit = True
    await task


@pytest_asyncio.fixture
async def app_client() -> AsyncIterator[AsyncClient]:
    from app.main import app, lifespan  # deferred: REDIS_URL must already be set

    async with lifespan(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client
