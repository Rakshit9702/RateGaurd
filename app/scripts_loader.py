from pathlib import Path

import redis.asyncio as redis
from redis.commands.core import AsyncScript

LUA_DIR = Path(__file__).parent.parent / "lua"

_scripts: dict[str, AsyncScript] = {}


async def load_scripts(client: redis.Redis) -> None:
    """Register every lua/*.lua script and eagerly SCRIPT LOAD it.

    Called once from the FastAPI lifespan at startup so a broken script
    fails at boot, and no request pays a cold-load penalty. redis-py's
    Script object executes via EVALSHA and transparently falls back to
    EVAL (re-caching the SHA) on a NOSCRIPT error.
    """
    global _scripts
    _scripts = {path.stem: client.register_script(path.read_text()) for path in sorted(LUA_DIR.glob("*.lua"))}
    for script in _scripts.values():
        await client.script_load(script.script)


def get_script(name: str) -> AsyncScript:
    return _scripts[name]
