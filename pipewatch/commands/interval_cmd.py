"""interval_cmd: report the average run interval (time between runs) per pipeline."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import Optional

from pipewatch.history import RunHistory


def _pipeline_interval(
    history: RunHistory,
    pipeline: Optional[str],
    hours: int,
) -> list[dict]:
    """Return avg interval in seconds between consecutive runs per pipeline."""
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    entries = [
        e for e in history.all()
        if e.timestamp >= cutoff and (pipeline is None or e.pipeline == pipeline)
    ]

    pipelines: dict[str, list[float]] = {}
    for e in entries:
        pipelines.setdefault(e.pipeline, []).append(e.timestamp)

    results = []
    for name, timestamps in sorted(pipelines.items()):
        timestamps.sort()
        if len(timestamps) < 2:
            results.append({"pipeline": name, "avg_interval_s": None, "run_count": len(timestamps)})
            continue
        gaps = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
        avg = sum(gaps) / len(gaps)
        results.append({"pipeline": name, "avg_interval_s": round(avg, 2), "run_count": len(timestamps)})
    return results


def run_interval_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    pipeline = getattr(args, "pipeline", None)
    results = _pipeline_interval(history, pipeline, args.hours)

    if not results:
        print("No history entries found.")
        return 0

    if getattr(args, "json", False):
        print(json.dumps(results, indent=2))
        return 0

    print(f"{'Pipeline':<30} {'Runs':>6} {'Avg Interval':>15}")
    print("-" * 55)
    for row in results:
        interval = row["avg_interval_s"]
        interval_str = f"{interval:.1f}s" if interval is not None else "N/A"
        print(f"{row['pipeline']:<30} {row['run_count']:>6} {interval_str:>15}")
    return 0


def register_interval_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "interval", help="Show average run interval per pipeline"
    )
    p.add_argument("--hours", type=int, default=24, help="Lookback window in hours (default: 24)")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.set_defaults(func=run_interval_cmd)
