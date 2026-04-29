"""cooldown_cmd: track and report pipeline cooldown periods after failures."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pipewatch.history import RunHistory


def _pipeline_cooldown(
    history: RunHistory,
    pipeline: str,
    cooldown_minutes: int,
) -> dict[str, Any] | None:
    """Return cooldown info for a single pipeline."""
    entries = [
        e for e in history.get_all() if e.pipeline == pipeline
    ]
    if not entries:
        return None

    entries_sorted = sorted(entries, key=lambda e: e.timestamp)

    # Find the last failure and whether we are still in cooldown
    last_failure_ts: datetime | None = None
    for entry in reversed(entries_sorted):
        if not entry.healthy:
            last_failure_ts = entry.timestamp
            break

    if last_failure_ts is None:
        return {
            "pipeline": pipeline,
            "in_cooldown": False,
            "last_failure": None,
            "cooldown_remaining_seconds": 0,
        }

    now = datetime.now(timezone.utc)
    elapsed = (now - last_failure_ts).total_seconds()
    cooldown_seconds = cooldown_minutes * 60
    remaining = max(0.0, cooldown_seconds - elapsed)

    return {
        "pipeline": pipeline,
        "in_cooldown": remaining > 0,
        "last_failure": last_failure_ts.isoformat(),
        "cooldown_remaining_seconds": round(remaining, 1),
    }


def run_cooldown_cmd(args: Any) -> int:
    """Entry point for the cooldown subcommand."""
    history = RunHistory(args.history_file)
    all_entries = history.get_all()

    pipelines = (
        [args.pipeline]
        if args.pipeline
        else sorted({e.pipeline for e in all_entries})
    )

    results = [
        r
        for p in pipelines
        if (r := _pipeline_cooldown(history, p, args.cooldown_minutes)) is not None
    ]

    if not results:
        print("No pipeline history found.")
        return 0

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'In Cooldown':<14} {'Remaining (s)':<16} Last Failure"
    print(header)
    print("-" * len(header))
    for r in results:
        flag = "YES" if r["in_cooldown"] else "no"
        remaining = r["cooldown_remaining_seconds"]
        last = r["last_failure"] or "—"
        print(f"{r['pipeline']:<30} {flag:<14} {remaining:<16} {last}")

    if args.exit_code:
        if any(r["in_cooldown"] for r in results):
            return 1
    return 0


def register_cooldown_subcommand(subparsers: Any) -> None:  # pragma: no cover
    p = subparsers.add_parser(
        "cooldown",
        help="Show pipelines currently in post-failure cooldown.",
    )
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file.",
    )
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline.")
    p.add_argument(
        "--cooldown-minutes",
        type=int,
        default=30,
        help="Cooldown window in minutes (default: 30).",
    )
    p.add_argument("--json", action="store_true", help="Output as JSON.")
    p.add_argument(
        "--exit-code",
        action="store_true",
        help="Return exit code 1 if any pipeline is in cooldown.",
    )
    p.set_defaults(func=run_cooldown_cmd)
