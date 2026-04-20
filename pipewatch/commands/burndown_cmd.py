"""burndown_cmd: show how many failing pipelines remain over a time window."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from pipewatch.history import RunHistory, HistoryEntry


def _bucket_burndown(
    entries: List[HistoryEntry],
    hours: int,
    buckets: int,
) -> List[dict]:
    """Divide [now-hours, now] into *buckets* equal slots.

    For each slot return the number of distinct failing pipelines
    that had at least one failure recorded inside that slot.
    """
    now = datetime.now(tz=timezone.utc)
    window_start = now - timedelta(hours=hours)
    slot_size = timedelta(hours=hours) / buckets

    result = []
    for i in range(buckets):
        slot_start = window_start + i * slot_size
        slot_end = slot_start + slot_size
        failing = {
            e.pipeline
            for e in entries
            if slot_start <= e.timestamp < slot_end and not e.healthy
        }
        result.append(
            {
                "slot_start": slot_start.isoformat(),
                "slot_end": slot_end.isoformat(),
                "failing_count": len(failing),
                "failing_pipelines": sorted(failing),
            }
        )
    return result


def run_burndown_cmd(args) -> int:  # noqa: ANN001
    history = RunHistory(path=args.history_file)
    entries = history.all()

    if args.pipeline:
        entries = [e for e in entries if e.pipeline == args.pipeline]

    buckets = _bucket_burndown(entries, hours=args.hours, buckets=args.buckets)

    if args.json:
        print(json.dumps(buckets, indent=2))
        return 0

    if not any(b["failing_count"] for b in buckets):
        print("No failures recorded in the selected window.")
        return 0

    print(f"{'Slot start':<32} {'Failing':>7}  Pipelines")
    print("-" * 72)
    for b in buckets:
        pipes = ", ".join(b["failing_pipelines"]) if b["failing_pipelines"] else "-"
        print(f"{b['slot_start']:<32} {b['failing_count']:>7}  {pipes}")
    return 0


def register_burndown_subcommand(sub) -> None:  # noqa: ANN001
    p = sub.add_parser("burndown", help="Show failing-pipeline count over time slots")
    p.add_argument("--hours", type=int, default=24, help="Look-back window in hours")
    p.add_argument("--buckets", type=int, default=8, help="Number of time buckets")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.set_defaults(func=run_burndown_cmd)
