import csv
import json
from pathlib import Path
from typing import Any

import redis.asyncio as redis

RESULTS_DIR = Path(__file__).parent / "results"
PLOTS_DIR = RESULTS_DIR / "plots"

DEFAULT_BASE_URL = "http://localhost:8000"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"

ALGORITHMS = ["fixed_window", "sliding_window_log", "token_bucket", "gcra"]


def get_redis_client(redis_url: str = DEFAULT_REDIS_URL) -> redis.Redis:
    return redis.Redis.from_url(redis_url, decode_responses=True)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: Any) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
