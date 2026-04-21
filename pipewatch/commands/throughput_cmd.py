"""Throughput command: reports runs-per-hour for each pipeline over a time window."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_throughput(
    entries: List[HistoryEntry],
    pipeline: str,
    hours: int,
) -> Dict[str, Any]:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    relevant = [
        e for e in entries
        if e.pipeline == pipeline and e.timestamp >= cutoff
    ]
    total = len(relevant)
    rate = round(total / hours, 4) if hours > 0 else 0.0
    failed = sum(1 for e in relevant if not e.healthy)
    return {
        "pipeline": pipeline,
        "hours": hours,
        "total_runs": total,
        "runs_per_hour": rate,
        "failed_runs": failed,
    }


def run_throughput_cmd(args: Namespace) -> int:
    history = RunHistory(path=args.history_file)
    entries = history.all()

    pipelines = (
        [args.pipeline]
        if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    if not pipelines:
        print("No history entries found.")
        return 0

    results = [
        _pipeline_throughput(entries, p, args.hours)
        for p in pipelines
    ]

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Hours':>6} {'Total':>7} {'Runs/hr':>9} {'Failed':>7}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['pipeline']:<30} {r['hours']:>6} {r['total_runs']:>7} "
            f"{r['runs_per_hour']:>9.4f} {r['failed_runs']:>7}"
        )
    return 0


def register_throughput_subcommand(subparsers) -> None:
    parser: ArgumentParser = subparsers.add_parser(
        "throughput",
        help="Show pipeline run throughput (runs per hour) over a time window.",
    )
    parser.add_argument(
        "--hours", type=int, default=24,
        help="Time window in hours (default: 24).",
    )
    parser.add_argument(
        "--pipeline", default=None,
        help="Filter to a single pipeline.",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output results as JSON.",
    )
    parser.add_argument(
        "--history-file", default=".pipewatch_history.json",
        help="Path to history file.",
    )
    parser.set_defaults(func=run_throughput_cmd)
