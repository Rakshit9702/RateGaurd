"""Measurement 4: plot allowed requests over time under a sustained flood,
per algorithm, binned into 100ms buckets. Expected shapes: token bucket
spikes then flattens to the refill rate, GCRA paces evenly from the
start, fixed-window sawtooths at each window reset, sliding-window-log
stays smooth near the limit.
"""

import argparse
import asyncio
import time

import httpx

from benchmark.common import ALGORITHMS, DEFAULT_BASE_URL, RESULTS_DIR, write_csv

DURATION_S = 8
CONCURRENCY = 20
LIMIT = 20
WINDOW = 5
BIN_MS = 100

# token_bucket keeps its default burst (== limit, a full capacity head start).
# gcra is given a tight burst so its distinguishing behavior -- steady pacing
# with no head start -- is actually visible; at matching default bursts the
# two algorithms are mathematically equivalent and their curves overlap
# exactly, which is true but tells you nothing about *why* you'd pick one
# over the other for smoother enforcement.
BURST_OVERRIDES = {"gcra": 1}


async def _flood(
    client: httpx.AsyncClient, algorithm: str, key: str, start_time: float, records: list[tuple[float, bool]]
) -> None:
    payload = {"key": key, "algorithm": algorithm, "limit": LIMIT, "window": WINDOW}
    if algorithm in BURST_OVERRIDES:
        payload["burst"] = BURST_OVERRIDES[algorithm]
    while time.perf_counter() - start_time < DURATION_S:
        resp = await client.post("/check", json=payload)
        elapsed = time.perf_counter() - start_time
        records.append((elapsed, resp.json()["allowed"]))


async def measure_burst_shape(base_url: str) -> list[dict]:
    rows = []
    async with httpx.AsyncClient(
        base_url=base_url, timeout=30.0, limits=httpx.Limits(max_connections=CONCURRENCY)
    ) as client:
        for algorithm in ALGORITHMS:
            key = f"bench-burst-{algorithm}"
            records: list[tuple[float, bool]] = []
            start_time = time.perf_counter()

            await asyncio.gather(*[_flood(client, algorithm, key, start_time, records) for _ in range(CONCURRENCY)])

            num_bins = int(DURATION_S * 1000 / BIN_MS) + 1
            bin_counts = [0] * num_bins
            for elapsed, allowed in records:
                if allowed:
                    bin_index = int(elapsed * 1000 / BIN_MS)
                    if bin_index < num_bins:
                        bin_counts[bin_index] += 1

            rows.extend(
                {"algorithm": algorithm, "bin_start_ms": bin_index * BIN_MS, "allowed_count": count}
                for bin_index, count in enumerate(bin_counts)
            )

    return rows


async def run(base_url: str) -> list[dict]:
    rows = await measure_burst_shape(base_url)
    write_csv(RESULTS_DIR / "burst_shape.csv", rows)
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    args = parser.parse_args()

    asyncio.run(run(args.base_url))
    print(f"wrote {RESULTS_DIR / 'burst_shape.csv'}")
