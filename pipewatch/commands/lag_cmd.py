"""lag_cmd: report processing lag (difference between event time and processing time) per pipeline."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import Optional

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_lag(
    entries: list[HistoryEntry],
    pipeline: str,
    hours: float,
) -> Optional[dict]:
    """Compute average, min, and max lag (seconds) for a pipeline over the last *hours*."""
    now = datetime.now(tz=timezone.utc)
    cutoff = now.timestamp() - hours * 3600

    relevant = [
        e for e in entries
        if e.pipeline == pipeline
        and e.timestamp >= cutoff
        and e.lag_seconds is not None
    ]

    if not relevant:
        return None

    lags = [e.lag_seconds for e in relevant]
    return {
        "pipeline": pipeline,
        "count": len(lags),
        "avg_lag": round(sum(lags) / len(lags), 2),
        "min_lag": round(min(lags), 2),
        "max_lag": round(max(lags), 2),
    }


def run_lag_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    entries = history.all()

    pipelines = (
        [args.pipeline] if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    results = []
    for name in pipelines:
        info = _pipeline_lag(entries, name, args.hours)
        if info is not None:
            results.append(info)

    if not results:
        print("No lag data found.")
        return 0

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Count':>6} {'Avg Lag (s)':>12} {'Min':>10} {'Max':>10}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['pipeline']:<30} {r['count']:>6} "
            f"{r['avg_lag']:>12.2f} {r['min_lag']:>10.2f} {r['max_lag']:>10.2f}"
        )
    return 0


def register_lag_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "lag", help="Show processing lag statistics per pipeline"
    )
    p.add_argument("--hours", type=float, default=24.0, help="Look-back window in hours (default: 24)")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.set_defaults(func=run_lag_cmd)
