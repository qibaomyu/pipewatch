"""aging_cmd: report how long each pipeline has been in its current state."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pipewatch.history import RunHistory


def _pipeline_aging(history: RunHistory, pipeline: str | None) -> list[dict[str, Any]]:
    """Return aging info (time in current state) for each pipeline."""
    all_entries = history.all()
    if not all_entries:
        return []

    # Collect pipelines to inspect
    pipelines = (
        [pipeline] if pipeline
        else sorted({e.pipeline for e in all_entries})
    )

    now = datetime.now(tz=timezone.utc)
    results = []

    for name in pipelines:
        entries = sorted(
            [e for e in all_entries if e.pipeline == name],
            key=lambda e: e.timestamp,
        )
        if not entries:
            continue

        latest = entries[-1]
        current_state = "healthy" if latest.healthy else "failing"

        # Walk backwards to find where the state last changed
        streak_start = latest.timestamp
        for entry in reversed(entries[:-1]):
            entry_state = "healthy" if entry.healthy else "failing"
            if entry_state != current_state:
                break
            streak_start = entry.timestamp

        duration_seconds = (now - streak_start).total_seconds()
        results.append(
            {
                "pipeline": name,
                "state": current_state,
                "since": streak_start.isoformat(),
                "duration_seconds": round(duration_seconds),
            }
        )

    return results


def run_aging_cmd(args: Any) -> int:
    history = RunHistory(path=args.history_file)
    rows = _pipeline_aging(history, getattr(args, "pipeline", None))

    if not rows:
        print("No history entries found.")
        return 0

    if getattr(args, "json", False):
        print(json.dumps(rows, indent=2))
        return 0

    # Text output
    header = f"{'Pipeline':<30} {'State':<10} {'Duration (s)':>14}  Since"
    print(header)
    print("-" * len(header))
    for row in rows:
        print(
            f"{row['pipeline']:<30} {row['state']:<10} "
            f"{row['duration_seconds']:>14}  {row['since']}"
        )

    if getattr(args, "exit_code", False):
        if any(r["state"] == "failing" for r in rows):
            return 1

    return 0


def register_aging_subcommand(subparsers: Any) -> None:  # pragma: no cover
    p = subparsers.add_parser("aging", help="Show how long each pipeline has been in its current state")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--exit-code", action="store_true", help="Exit 1 if any pipeline is failing")
    p.set_defaults(func=run_aging_cmd)
