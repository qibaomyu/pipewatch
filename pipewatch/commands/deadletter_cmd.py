"""Dead-letter queue command: surfaces pipelines whose failures exceed a
configurable consecutive-failure threshold without any intervening success."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import Any

from pipewatch.history import RunHistory


def _pipeline_deadletter(
    entries: list,
    pipeline: str,
    threshold: int,
) -> dict[str, Any]:
    """Return dead-letter info for a single pipeline."""
    pipeline_entries = [
        e for e in entries if e.pipeline == pipeline
    ]
    pipeline_entries.sort(key=lambda e: e.timestamp)

    consecutive = 0
    max_consecutive = 0
    dead = False

    for entry in pipeline_entries:
        if not entry.healthy:
            consecutive += 1
            max_consecutive = max(max_consecutive, consecutive)
        else:
            consecutive = 0

    if consecutive >= threshold:
        dead = True

    return {
        "pipeline": pipeline,
        "consecutive_failures": consecutive,
        "max_consecutive": max_consecutive,
        "dead": dead,
        "threshold": threshold,
    }


def run_deadletter_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    entries = history.all()

    pipelines = (
        [args.pipeline]
        if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    if not pipelines:
        print("No history found.")
        return 0

    results = [
        _pipeline_deadletter(entries, p, args.threshold)
        for p in pipelines
    ]

    dead_results = [r for r in results if r["dead"]]

    if args.json:
        print(json.dumps(results, indent=2))
        return 1 if dead_results and args.exit_code else 0

    if not any(r["consecutive_failures"] > 0 for r in results):
        print("No dead-letter pipelines detected.")
        return 0

    for r in results:
        status = "DEAD" if r["dead"] else "ok"
        print(
            f"{r['pipeline']:<30} consecutive={r['consecutive_failures']:>4} "
            f"max={r['max_consecutive']:>4}  [{status}]"
        )

    return 1 if dead_results and args.exit_code else 0


def register_deadletter_subcommand(sub) -> None:
    p: ArgumentParser = sub.add_parser(
        "deadletter",
        help="Show pipelines stuck in repeated failure (dead-letter state)",
    )
    p.add_argument(
        "--threshold",
        type=int,
        default=5,
        help="Consecutive failures required to mark a pipeline dead (default: 5)",
    )
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--exit-code",
        action="store_true",
        help="Return exit code 1 if any dead-letter pipelines found",
    )
    p.set_defaults(func=run_deadletter_cmd)
