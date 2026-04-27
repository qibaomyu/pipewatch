"""saturation_cmd: report how close each pipeline is to its run quota ceiling."""
from __future__ import annotations

import argparse
import json
from typing import List, Optional

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_saturation(
    entries: List[HistoryEntry],
    pipeline: str,
    hours: float,
    limit: int,
) -> dict:
    """Return saturation info for a single pipeline."""
    import datetime

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=hours)
    relevant = [
        e for e in entries
        if e.pipeline == pipeline and e.timestamp >= cutoff
    ]
    run_count = len(relevant)
    pct = round(run_count / limit * 100, 1) if limit > 0 else 0.0
    return {
        "pipeline": pipeline,
        "runs": run_count,
        "limit": limit,
        "saturation_pct": pct,
        "saturated": run_count >= limit,
    }


def run_saturation_cmd(args: argparse.Namespace) -> int:
    history = RunHistory(path=args.history_file)
    entries = history.all()

    pipelines = (
        [args.pipeline] if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    if not pipelines:
        print("No history entries found.")
        return 0

    results = [
        _pipeline_saturation(entries, p, args.hours, args.limit)
        for p in pipelines
    ]

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Runs':>6} {'Limit':>6} {'Saturation':>12} {'Saturated':>10}"
    print(header)
    print("-" * len(header))
    for r in results:
        flag = "YES" if r["saturated"] else "no"
        print(
            f"{r['pipeline']:<30} {r['runs']:>6} {r['limit']:>6}"
            f" {r['saturation_pct']:>11.1f}% {flag:>10}"
        )

    if args.exit_code and any(r["saturated"] for r in results):
        return 1
    return 0


def register_saturation_subcommand(subparsers) -> None:
    p = subparsers.add_parser(
        "saturation",
        help="Show how close pipelines are to their run-count ceiling.",
    )
    p.add_argument("--hours", type=float, default=24.0, help="Lookback window in hours (default: 24).")
    p.add_argument("--limit", type=int, default=100, help="Maximum expected runs per window (default: 100).")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline.")
    p.add_argument("--json", action="store_true", help="Output as JSON.")
    p.add_argument("--exit-code", action="store_true", help="Exit 1 if any pipeline is saturated.")
    p.add_argument("--history-file", default=".pipewatch_history.json", help="Path to history file.")
    p.set_defaults(func=run_saturation_cmd)
