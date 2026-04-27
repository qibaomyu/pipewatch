"""backlog_cmd: report pipelines with a growing backlog of failures."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import List, Dict, Any

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_backlog(
    entries: List[HistoryEntry],
    pipeline: str,
    min_consecutive: int,
) -> Dict[str, Any]:
    """Return backlog info for a single pipeline."""
    pipeline_entries = [
        e for e in entries if e.pipeline == pipeline
    ]
    pipeline_entries.sort(key=lambda e: e.timestamp)

    consecutive = 0
    max_consecutive = 0
    for e in pipeline_entries:
        if not e.healthy:
            consecutive += 1
            max_consecutive = max(max_consecutive, consecutive)
        else:
            consecutive = 0

    in_backlog = consecutive >= min_consecutive
    return {
        "pipeline": pipeline,
        "current_streak": consecutive,
        "max_streak": max_consecutive,
        "in_backlog": in_backlog,
    }


def run_backlog_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    entries = history.all()

    if not entries:
        print("No history entries found.")
        return 0

    pipelines = (
        [args.pipeline] if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    results = [
        _pipeline_backlog(entries, p, args.min_consecutive)
        for p in pipelines
    ]

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    any_backlog = False
    for r in results:
        status = "BACKLOG" if r["in_backlog"] else "ok"
        print(
            f"{r['pipeline']:<30} streak={r['current_streak']:>4}  "
            f"max={r['max_streak']:>4}  [{status}]"
        )
        if r["in_backlog"]:
            any_backlog = True

    if args.exit_code and any_backlog:
        return 1
    return 0


def register_backlog_subcommand(subparsers) -> None:
    parser: ArgumentParser = subparsers.add_parser(
        "backlog",
        help="Report pipelines with a growing consecutive-failure backlog.",
    )
    parser.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file.",
    )
    parser.add_argument(
        "--pipeline", default=None, help="Filter to a single pipeline."
    )
    parser.add_argument(
        "--min-consecutive",
        type=int,
        default=3,
        help="Minimum consecutive failures to flag as backlog (default: 3).",
    )
    parser.add_argument(
        "--json", action="store_true", help="Output as JSON."
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        help="Return exit code 1 if any pipeline is in backlog.",
    )
    parser.set_defaults(func=run_backlog_cmd)
