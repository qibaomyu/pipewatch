"""recovery_cmd: show recovery events (healthy runs after failures) per pipeline."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import List, Dict, Any

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_recovery(entries: List[HistoryEntry]) -> Dict[str, Any]:
    """Return recovery event count and timestamps for a list of entries (single pipeline)."""
    recoveries: List[str] = []
    prev_healthy = True
    for entry in sorted(entries, key=lambda e: e.timestamp):
        if not prev_healthy and entry.healthy:
            recoveries.append(entry.timestamp)
        prev_healthy = entry.healthy
    return {"recoveries": len(recoveries), "timestamps": recoveries}


def run_recovery_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    all_entries = history.all()

    pipelines: Dict[str, List[HistoryEntry]] = {}
    for entry in all_entries:
        if args.pipeline and entry.pipeline != args.pipeline:
            continue
        pipelines.setdefault(entry.pipeline, []).append(entry)

    if not pipelines:
        print("No history entries found.")
        return 0

    results = [
        {"pipeline": name, **_pipeline_recovery(entries)}
        for name, entries in sorted(pipelines.items())
    ]

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    print(f"{'Pipeline':<30} {'Recoveries':>12} {'Last Recovery'}")
    print("-" * 65)
    for row in results:
        last = row["timestamps"][-1] if row["timestamps"] else "—"
        print(f"{row['pipeline']:<30} {row['recoveries']:>12} {last}")
    return 0


def register_recovery_subcommand(sub) -> None:
    p: ArgumentParser = sub.add_parser("recovery", help="Show recovery events per pipeline")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.set_defaults(func=run_recovery_cmd)
