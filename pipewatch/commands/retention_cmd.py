"""retention_cmd: report how long pipeline history is being retained."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import Any

from pipewatch.history import RunHistory


def _pipeline_retention(
    history: RunHistory,
    pipeline: str | None,
    top: int,
) -> list[dict[str, Any]]:
    """Return retention stats per pipeline."""
    entries = history.all()
    if pipeline:
        entries = [e for e in entries if e.pipeline == pipeline]

    pipelines: dict[str, list[datetime]] = {}
    for e in entries:
        pipelines.setdefault(e.pipeline, []).append(e.timestamp)

    now = datetime.now(tz=timezone.utc)
    rows: list[dict[str, Any]] = []
    for name, timestamps in pipelines.items():
        oldest = min(timestamps)
        newest = max(timestamps)
        span_hours = round((now - oldest).total_seconds() / 3600, 2)
        rows.append(
            {
                "pipeline": name,
                "run_count": len(timestamps),
                "oldest_run": oldest.isoformat(),
                "newest_run": newest.isoformat(),
                "span_hours": span_hours,
            }
        )

    rows.sort(key=lambda r: r["span_hours"], reverse=True)
    return rows[:top]


def run_retention_cmd(args: Namespace) -> int:
    history = RunHistory(path=args.history_file)
    rows = _pipeline_retention(history, args.pipeline, args.top)

    if not rows:
        print("No history found.")
        return 0

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Runs':>6} {'Span (h)':>10} {'Oldest':<26} {'Newest':<26}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r['pipeline']:<30} {r['run_count']:>6} {r['span_hours']:>10} "
            f"{r['oldest_run']:<26} {r['newest_run']:<26}"
        )
    return 0


def register_retention_subcommand(subparsers: Any) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "retention", help="Show how long history is retained per pipeline."
    )
    p.add_argument("--history-file", default="pipewatch_history.json")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline.")
    p.add_argument("--top", type=int, default=20, help="Max pipelines to show.")
    p.add_argument("--json", action="store_true", help="Output as JSON.")
    p.set_defaults(func=run_retention_cmd)
