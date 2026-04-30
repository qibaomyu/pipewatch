"""frequency_cmd: report how often each pipeline runs within a time window."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone, timedelta
from typing import Optional

from pipewatch.history import RunHistory


def _pipeline_frequency(
    history: RunHistory,
    hours: int,
    pipeline: Optional[str] = None,
) -> list[dict]:
    """Return run-count and average interval (minutes) per pipeline."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    entries = [
        e for e in history.all()
        if e.timestamp >= cutoff
        and (pipeline is None or e.pipeline == pipeline)
    ]

    grouped: dict[str, list[datetime]] = {}
    for e in entries:
        grouped.setdefault(e.pipeline, []).append(e.timestamp)

    results = []
    for name, timestamps in sorted(grouped.items()):
        timestamps.sort()
        count = len(timestamps)
        if count >= 2:
            gaps = [
                (timestamps[i] - timestamps[i - 1]).total_seconds() / 60
                for i in range(1, count)
            ]
            avg_interval = round(sum(gaps) / len(gaps), 2)
        else:
            avg_interval = None
        results.append({
            "pipeline": name,
            "run_count": count,
            "avg_interval_minutes": avg_interval,
        })
    return results


def run_frequency_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    rows = _pipeline_frequency(history, args.hours, getattr(args, "pipeline", None))

    if not rows:
        print("No data available.")
        return 0

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Runs':>6} {'Avg Interval (min)':>20}"
    print(header)
    print("-" * len(header))
    for r in rows:
        interval = f"{r['avg_interval_minutes']:.2f}" if r["avg_interval_minutes"] is not None else "  n/a"
        print(f"{r['pipeline']:<30} {r['run_count']:>6} {interval:>20}")
    return 0


def register_frequency_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "frequency", help="Report how often pipelines run within a time window"
    )
    p.add_argument("--hours", type=int, default=24, help="Look-back window in hours (default: 24)")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.set_defaults(func=run_frequency_cmd)
