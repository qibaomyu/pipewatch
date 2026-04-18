"""Trend command: show error-rate and latency trends over time."""
from __future__ import annotations

import argparse
from collections import defaultdict
from typing import List

from pipewatch.history import RunHistory, HistoryEntry


def _bucket_entries(entries: List[HistoryEntry], buckets: int = 5):
    """Split entries into equal time buckets, return list of bucket averages."""
    if not entries:
        return []
    entries = sorted(entries, key=lambda e: e.timestamp)
    size = max(1, len(entries) // buckets)
    result = []
    for i in range(0, len(entries), size):
        chunk = entries[i : i + size]
        avg_err = sum(e.error_rate for e in chunk) / len(chunk)
        avg_lat = sum(e.latency for e in chunk) / len(chunk)
        result.append({"error_rate": round(avg_err, 4), "latency": round(avg_lat, 2)})
    return result


def _trend_symbol(values: List[float]) -> str:
    if len(values) < 2:
        return "~"
    delta = values[-1] - values[0]
    if delta > 0.01:
        return "↑"
    if delta < -0.01:
        return "↓"
    return "→"


def _pipeline_trend(name: str, entries: List[HistoryEntry], buckets: int) -> str:
    bkts = _bucket_entries(entries, buckets)
    err_vals = [b["error_rate"] for b in bkts]
    lat_vals = [b["latency"] for b in bkts]
    err_sym = _trend_symbol(err_vals)
    lat_sym = _trend_symbol(lat_vals)
    lines = [f"  {name}"]
    lines.append(f"    error_rate : {err_sym}  " + "  ".join(f"{v:.3f}" for v in err_vals))
    lines.append(f"    latency    : {lat_sym}  " + "  ".join(f"{v:.1f}" for v in lat_vals))
    return "\n".join(lines)


def run_trend_cmd(args: argparse.Namespace) -> int:
    history = RunHistory(args.history_file)
    entries = history.all()
    if not entries:
        print("No history available.")
        return 0

    grouped: dict = defaultdict(list)
    for e in entries:
        if args.pipeline and e.pipeline != args.pipeline:
            continue
        grouped[e.pipeline].append(e)

    if not grouped:
        print("No entries match the given filter.")
        return 0

    print(f"Trend report (buckets={args.buckets})")
    print("=" * 40)
    for name, ents in sorted(grouped.items()):
        print(_pipeline_trend(name, ents, args.buckets))
    return 0
