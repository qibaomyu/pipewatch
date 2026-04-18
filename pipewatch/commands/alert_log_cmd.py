"""Command to display the alert history log."""
from __future__ import annotations

import json
from argparse import Namespace
from typing import List

from pipewatch.history import RunHistory, HistoryEntry


def _collect_alerts(entries: List[HistoryEntry], pipeline: str | None):
    rows = []
    for entry in entries:
        if pipeline and entry.pipeline != pipeline:
            continue
        for alert in entry.alerts:
            rows.append({
                "timestamp": entry.timestamp,
                "pipeline": entry.pipeline,
                "level": alert.get("level", "UNKNOWN"),
                "message": alert.get("message", ""),
            })
    return rows


def run_alert_log_cmd(args: Namespace) -> int:
    history = RunHistory(path=args.history_file)
    entries = history.all()

    if not entries:
        print("No history found.")
        return 0

    rows = _collect_alerts(entries, getattr(args, "pipeline", None))

    if not rows:
        print("No alerts recorded.")
        return 0

    if getattr(args, "json", False):
        print(json.dumps(rows, indent=2))
        return 0

    header = f"{'Timestamp':<26} {'Pipeline':<20} {'Level':<10} Message"
    print(header)
    print("-" * len(header))
    for row in rows:
        ts = row["timestamp"][:19].replace("T", " ")
        print(f"{ts:<26} {row['pipeline']:<20} {row['level']:<10} {row['message']}")

    return 0
