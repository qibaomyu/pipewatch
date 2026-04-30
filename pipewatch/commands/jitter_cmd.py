"""jitter_cmd.py – measure run-time jitter (variance in inter-run intervals) per pipeline."""
from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

from pipewatch.history import RunHistory


def _pipeline_jitter(
    history: RunHistory,
    pipeline: Optional[str],
    hours: int,
) -> list[dict]:
    """Return jitter stats (std-dev of inter-run gaps in seconds) per pipeline."""
    from datetime import datetime, timezone
    import math

    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    entries = [
        e for e in history.all()
        if e.timestamp >= cutoff and (pipeline is None or e.pipeline == pipeline)
    ]

    by_pipeline: dict[str, list[float]] = {}
    for e in sorted(entries, key=lambda x: (x.pipeline, x.timestamp)):
        by_pipeline.setdefault(e.pipeline, []).append(e.timestamp)

    results = []
    for name, timestamps in sorted(by_pipeline.items()):
        if len(timestamps) < 2:
            results.append({"pipeline": name, "jitter_seconds": None, "runs": len(timestamps)})
            continue
        gaps = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
        mean = sum(gaps) / len(gaps)
        variance = sum((g - mean) ** 2 for g in gaps) / len(gaps)
        std_dev = math.sqrt(variance)
        results.append({
            "pipeline": name,
            "jitter_seconds": round(std_dev, 2),
            "mean_interval_seconds": round(mean, 2),
            "runs": len(timestamps),
        })
    return results


def run_jitter_cmd(args: argparse.Namespace) -> int:
    history = RunHistory(args.history_file)
    rows = _pipeline_jitter(history, getattr(args, "pipeline", None), args.hours)

    if not rows:
        print("No history entries found.")
        return 0

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    print(f"{'Pipeline':<30} {'Jitter (s)':>12} {'Mean Interval (s)':>18} {'Runs':>6}")
    print("-" * 70)
    for r in rows:
        jitter = f"{r['jitter_seconds']:.2f}" if r["jitter_seconds"] is not None else "N/A"
        mean = f"{r['mean_interval_seconds']:.2f}" if r.get("mean_interval_seconds") is not None else "N/A"
        print(f"{r['pipeline']:<30} {jitter:>12} {mean:>18} {r['runs']:>6}")
    return 0


def register_jitter_subcommand(subparsers) -> None:
    p = subparsers.add_parser("jitter", help="Show run-time jitter per pipeline")
    p.add_argument("--hours", type=int, default=24, help="Look-back window in hours (default: 24)")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.set_defaults(func=run_jitter_cmd)
