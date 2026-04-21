"""retry_cmd: show pipelines that have exceeded a failure retry threshold."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Optional

from pipewatch.history import RunHistory


def _pipeline_retry(
    history: RunHistory,
    pipeline: Optional[str],
    hours: float,
    max_retries: int,
) -> list[dict]:
    """Return pipelines whose consecutive failure count exceeds *max_retries*."""
    now = datetime.now(tz=timezone.utc)
    cutoff = now.timestamp() - hours * 3600

    pipelines: dict[str, list] = {}
    for entry in history.entries:
        if entry.timestamp < cutoff:
            continue
        if pipeline and entry.pipeline != pipeline:
            continue
        pipelines.setdefault(entry.pipeline, []).append(entry)

    results = []
    for name, entries in pipelines.items():
        sorted_entries = sorted(entries, key=lambda e: e.timestamp)
        # count trailing consecutive failures
        consecutive = 0
        for entry in reversed(sorted_entries):
            if not entry.healthy:
                consecutive += 1
            else:
                break
        results.append(
            {
                "pipeline": name,
                "consecutive_failures": consecutive,
                "exceeds_threshold": consecutive > max_retries,
            }
        )

    results.sort(key=lambda r: r["consecutive_failures"], reverse=True)
    return results


def run_retry_cmd(args: argparse.Namespace) -> int:
    history = RunHistory(path=args.history_file)
    history.load()

    rows = _pipeline_retry(
        history,
        pipeline=args.pipeline,
        hours=args.hours,
        max_retries=args.max_retries,
    )

    if not rows:
        print("No pipeline history found.")
        return 0

    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        print(f"{'Pipeline':<30} {'Consec. Failures':>17} {'Exceeds Threshold':>18}")
        print("-" * 68)
        for row in rows:
            flag = "YES" if row["exceeds_threshold"] else "no"
            print(
                f"{row['pipeline']:<30} {row['consecutive_failures']:>17} {flag:>18}"
            )

    if args.exit_code and any(r["exceeds_threshold"] for r in rows):
        return 1
    return 0


def register_retry_subcommand(subparsers) -> None:
    p = subparsers.add_parser(
        "retry",
        help="Show pipelines exceeding consecutive-failure retry threshold",
    )
    p.add_argument("--hours", type=float, default=24.0, help="Lookback window in hours")
    p.add_argument("--max-retries", type=int, default=3, help="Consecutive failure limit")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--exit-code", action="store_true", help="Exit 1 if any pipeline exceeds threshold")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.set_defaults(func=run_retry_cmd)
