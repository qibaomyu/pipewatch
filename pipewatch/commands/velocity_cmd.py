"""velocity_cmd: report the rate of pipeline runs over time (runs per hour)."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import List

from pipewatch.history import HistoryEntry, RunHistory


def _pipeline_velocity(
    entries: List[HistoryEntry],
    pipeline: str,
    hours: int,
) -> dict:
    """Return velocity stats for a single pipeline."""
    now = datetime.now(tz=timezone.utc)
    cutoff = now.timestamp() - hours * 3600
    relevant = [
        e for e in entries
        if e.pipeline == pipeline and e.timestamp >= cutoff
    ]
    total = len(relevant)
    rate = round(total / hours, 3) if hours > 0 else 0.0
    failed = sum(1 for e in relevant if not e.healthy)
    return {
        "pipeline": pipeline,
        "total_runs": total,
        "failed_runs": failed,
        "runs_per_hour": rate,
        "window_hours": hours,
    }


def run_velocity_cmd(args: Namespace) -> int:
    history = RunHistory(path=args.history_file)
    entries = history.all()

    if not entries:
        print("No history entries found.")
        return 0

    pipelines = (
        [args.pipeline]
        if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    results = [_pipeline_velocity(entries, p, args.hours) for p in pipelines]

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Runs':>6} {'Failed':>7} {'Runs/hr':>9}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['pipeline']:<30} {r['total_runs']:>6} "
            f"{r['failed_runs']:>7} {r['runs_per_hour']:>9.3f}"
        )
    return 0


def register_velocity_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "velocity", help="Show pipeline run-rate (runs per hour)"
    )
    p.add_argument(
        "--hours", type=int, default=24,
        help="Look-back window in hours (default: 24)"
    )
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--history-file", default=".pipewatch_history.json",
        help="Path to history file"
    )
    p.set_defaults(func=run_velocity_cmd)
