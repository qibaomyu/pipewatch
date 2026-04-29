"""idle_cmd: detect pipelines that have not run within a given time window."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import Optional

from pipewatch.history import RunHistory


def _pipeline_idle(
    history: RunHistory,
    pipeline: Optional[str],
    hours: float,
) -> list[dict]:
    """Return a list of idle pipeline records.

    A pipeline is considered idle when its most-recent run timestamp is older
    than *hours* hours ago (or it has never run at all within the window).
    """
    now = datetime.now(tz=timezone.utc)
    cutoff_seconds = hours * 3600

    entries = history.all()
    if pipeline:
        entries = [e for e in entries if e.pipeline == pipeline]

    # Group entries by pipeline name, keeping the most-recent timestamp.
    latest: dict[str, float] = {}
    for entry in entries:
        ts = entry.timestamp
        if entry.pipeline not in latest or ts > latest[entry.pipeline]:
            latest[entry.pipeline] = ts

    results = []
    for name, last_ts in latest.items():
        age_seconds = (now.timestamp() - last_ts)
        if age_seconds >= cutoff_seconds:
            results.append(
                {
                    "pipeline": name,
                    "last_run_age_hours": round(age_seconds / 3600, 2),
                    "idle": True,
                }
            )

    results.sort(key=lambda r: r["last_run_age_hours"], reverse=True)
    return results


def run_idle_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    results = _pipeline_idle(history, args.pipeline, args.hours)

    if not results:
        print("No idle pipelines detected.")
        return 0

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print(f"{'Pipeline':<30} {'Idle (hours)':>14}")
        print("-" * 46)
        for row in results:
            print(f"{row['pipeline']:<30} {row['last_run_age_hours']:>14.2f}")

    if args.exit_code and results:
        return 1
    return 0


def register_idle_subcommand(subparsers) -> None:  # type: ignore[type-arg]
    parser: ArgumentParser = subparsers.add_parser(
        "idle",
        help="List pipelines that have not run within a specified time window.",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=24.0,
        help="Inactivity threshold in hours (default: 24).",
    )
    parser.add_argument("--pipeline", default=None, help="Filter to a single pipeline.")
    parser.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file.",
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON.")
    parser.add_argument(
        "--exit-code",
        action="store_true",
        help="Return exit code 1 if any idle pipelines are found.",
    )
    parser.set_defaults(func=run_idle_cmd)
