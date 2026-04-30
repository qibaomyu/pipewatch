"""overdue_cmd: report pipelines that have not run within their expected schedule."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from pipewatch.history import RunHistory


def _pipeline_overdue(
    history: RunHistory,
    pipeline: Optional[str],
    max_hours: float,
    now: Optional[datetime] = None,
) -> list[dict]:
    """Return a list of overdue pipeline records.

    A pipeline is considered overdue when its most-recent run is older than
    *max_hours* hours (or it has never run).
    """
    if now is None:
        now = datetime.now(tz=timezone.utc)

    entries = history.all()
    if pipeline:
        entries = [e for e in entries if e.pipeline == pipeline]

    # Group by pipeline – keep only the latest entry per pipeline
    latest: dict[str, datetime] = {}
    for entry in entries:
        ts = entry.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if entry.pipeline not in latest or ts > latest[entry.pipeline]:
            latest[entry.pipeline] = ts

    results = []
    for name, last_run in sorted(latest.items()):
        age_hours = (now - last_run).total_seconds() / 3600.0
        if age_hours > max_hours:
            results.append(
                {
                    "pipeline": name,
                    "last_run": last_run.isoformat(),
                    "age_hours": round(age_hours, 2),
                    "max_hours": max_hours,
                    "overdue": True,
                }
            )

    return results


def run_overdue_cmd(args) -> int:
    history = RunHistory(args.history_file)
    results = _pipeline_overdue(
        history,
        pipeline=getattr(args, "pipeline", None),
        max_hours=args.max_hours,
    )

    if not results:
        print("No overdue pipelines found.")
        return 0

    if getattr(args, "json", False):
        print(json.dumps(results, indent=2))
    else:
        print(f"{'Pipeline':<30} {'Last Run':<30} {'Age (h)':>8}")
        print("-" * 72)
        for r in results:
            print(f"{r['pipeline']:<30} {r['last_run']:<30} {r['age_hours']:>8.2f}")

    if getattr(args, "exit_code", False):
        return 1
    return 0


def register_overdue_subcommand(subparsers) -> None:
    p = subparsers.add_parser("overdue", help="Report pipelines overdue for a run")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument(
        "--max-hours",
        dest="max_hours",
        type=float,
        default=24.0,
        help="Hours since last run before a pipeline is considered overdue (default: 24)",
    )
    p.add_argument("--json", action="store_true", default=False, help="JSON output")
    p.add_argument(
        "--exit-code",
        dest="exit_code",
        action="store_true",
        default=False,
        help="Return exit code 1 when overdue pipelines are found",
    )
    p.add_argument(
        "--history-file",
        dest="history_file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.set_defaults(func=run_overdue_cmd)
