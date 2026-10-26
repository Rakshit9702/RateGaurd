from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db import close_pool as close_db_pool
from app.db import connect as connect_db
from app.db import get_pool as get_db_pool
from app.grpc_server import start_grpc_server, stop_grpc_server
from app.redis_client import close_pool as close_redis_pool
from app.redis_client import get_redis_client
from app.routers import check, events, health, policies
from app.scripts_loader import load_scripts
from app.shard_router import close as close_shard_router
from app.shard_router import connect as connect_shard_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = get_redis_client()
    await client.ping()
    await load_scripts(client)
    await connect_db()
    await connect_shard_router(client)
    grpc_server = await start_grpc_server(client, get_db_pool())
    yield
    await stop_grpc_server(grpc_server)
    await close_shard_router()
    await close_redis_pool()
    await close_db_pool()


app = FastAPI(title="Rate Limiter", lifespan=lifespan)
app.include_router(health.router)
app.include_router(check.router)
app.include_router(policies.router)
app.include_router(events.router)
