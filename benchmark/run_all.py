"""Orchestrates all four benchmark measurements sequentially (not interleaved,
to avoid cross-measurement Redis-load interference) against a live server,
writes benchmark/results/*.csv + results.json, and renders the three
comparison plots referenced from the README.
"""

import argparse
import asyncio

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from benchmark import accuracy, burst_shape, memory, throughput
from benchmark.common import ALGORITHMS, DEFAULT_BASE_URL, DEFAULT_REDIS_URL, PLOTS_DIR, RESULTS_DIR, write_json

# Validated (dataviz skill, all-pairs, light mode) categorical palette,
# in ALGORITHMS order: blue, orange, aqua, violet.
ALGORITHM_COLORS = {
    "fixed_window": "#2a78d6",
    "sliding_window_log": "#eb6834",
    "token_bucket": "#1baf7a",
    "gcra": "#4a3aa7",
}

SURFACE = "#fcfcfb"
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
AXIS = "#c3c2b7"

ALGORITHM_LABELS = {
    "fixed_window": "Fixed window",
    "sliding_window_log": "Sliding window log",
    "token_bucket": "Token bucket",
    "gcra": "GCRA",
}


def _style_axes(ax: plt.Axes) -> None:
    ax.set_facecolor(SURFACE)
    ax.figure.set_facecolor(SURFACE)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(AXIS)
    ax.tick_params(colors=INK_MUTED, labelsize=9)
    ax.grid(True, color=GRIDLINE, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    ax.xaxis.label.set_color(INK_SECONDARY)
    ax.yaxis.label.set_color(INK_SECONDARY)
    ax.title.set_color(INK_PRIMARY)


def plot_memory_growth(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7, 4.5), dpi=150)
    for algorithm in ALGORITHMS:
        sub = df[df["algorithm"] == algorithm].sort_values("limit")
        ax.plot(
            sub["limit"],
            sub["key_bytes"],
            marker="o",
            markersize=6,
            linewidth=2,
            color=ALGORITHM_COLORS[algorithm],
            label=ALGORITHM_LABELS[algorithm],
        )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("limit")
    ax.set_ylabel("bytes per key (MEMORY USAGE)")
    ax.set_title("Per-key memory vs. limit")
    _style_axes(ax)
    ax.legend(frameon=False, labelcolor=INK_PRIMARY, fontsize=9)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "memory_growth.png")
    plt.close(fig)


def plot_throughput_p99(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7, 4.5), dpi=150)
    ordered = df.set_index("algorithm").loc[ALGORITHMS].reset_index()
    colors = [ALGORITHM_COLORS[a] for a in ordered["algorithm"]]
    labels = [ALGORITHM_LABELS[a] for a in ordered["algorithm"]]
    bars = ax.bar(labels, ordered["p99_ms"], color=colors, width=0.6, zorder=3)
    for bar, value in zip(bars, ordered["p99_ms"], strict=True):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:.0f}ms",
            ha="center",
            va="bottom",
            fontsize=9,
            color=INK_PRIMARY,
        )
    ax.set_ylabel("p99 latency (ms)")
    ax.set_title("p99 latency under concurrent load")
    _style_axes(ax)
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "throughput_p99.png")
    plt.close(fig)


def plot_burst_shape(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=150)
    for algorithm in ALGORITHMS:
        sub = df[df["algorithm"] == algorithm].sort_values("bin_start_ms")
        ax.plot(
            sub["bin_start_ms"] / 1000,
            sub["allowed_count"],
            linewidth=2,
            alpha=0.75,
            color=ALGORITHM_COLORS[algorithm],
            label=ALGORITHM_LABELS[algorithm],
        )
    ax.set_xlabel("seconds into sustained flood")
    ax.set_ylabel("allowed requests / 100ms bin")
    ax.set_title("Admission shape under a sustained flood")
    _style_axes(ax)
    ax.legend(frameon=False, labelcolor=INK_PRIMARY, fontsize=9)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "burst_shape.png")
    plt.close(fig)


async def run(base_url: str, redis_url: str) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    print("Running accuracy...")
    accuracy_rows = await accuracy.run(base_url, redis_url)

    print("Running memory...")
    memory_rows = await memory.run(base_url, redis_url)

    print("Running throughput...")
    throughput_rows = await throughput.run(base_url)

    print("Running burst shape...")
    burst_rows = await burst_shape.run(base_url)

    plot_memory_growth(pd.DataFrame(memory_rows))
    plot_throughput_p99(pd.DataFrame(throughput_rows))
    plot_burst_shape(pd.DataFrame(burst_rows))

    write_json(
        RESULTS_DIR / "results.json",
        {
            "accuracy": accuracy_rows,
            "memory": memory_rows,
            "throughput": throughput_rows,
        },
    )
    print(f"Done. Results in {RESULTS_DIR}, plots in {PLOTS_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    args = parser.parse_args()

    asyncio.run(run(args.base_url, args.redis_url))
