"""streak_cmd: report consecutive healthy/failing runs per pipeline."""
from __future__ import annotations

import argparse
import json
from typing import Dict, List

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_streak(entries: List[HistoryEntry]) -> Dict:
    """Return current streak info for a sorted (oldest-first) entry list."""
    if not entries:
        return {"streak": 0, "state": None}

    last_state = entries[-1].healthy
    count = 0
    for entry in reversed(entries):
        if entry.healthy == last_state:
            count += 1
        else:
            break

    return {"streak": count, "state": "healthy" if last_state else "failing"}


def run_streak_cmd(args: argparse.Namespace) -> int:
    history = RunHistory(args.history_file)
    pipelines = history.pipelines()

    if not pipelines:
        print("No history found.")
        return 0

    if args.pipeline:
        pipelines = [p for p in pipelines if p == args.pipeline]
        if not pipelines:
            print(f"No history for pipeline: {args.pipeline}")
            return 2

    results = {}
    for name in sorted(pipelines):
        entries = sorted(history.get(name), key=lambda e: e.timestamp)
        results[name] = _pipeline_streak(entries)

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    for name, info in results.items():
        state = info["state"] or "unknown"
        streak = info["streak"]
        symbol = "✔" if state == "healthy" else "✘"
        print(f"{symbol} {name}: {streak} consecutive {state} run(s)")

    return 0


def register_streak_subcommand(subparsers) -> None:
    p = subparsers.add_parser("streak", help="Show consecutive run streaks per pipeline")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.set_defaults(func=run_streak_cmd)
