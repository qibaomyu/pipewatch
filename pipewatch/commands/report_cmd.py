"""report_cmd: generate a health report across all pipelines over a time window."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from typing import List

from pipewatch.history import RunHistory


def _pipeline_report(pipeline: str, entries: list) -> dict:
    total = len(entries)
    if total == 0:
        return {"pipeline": pipeline, "total_runs": 0, "failures": 0, "failure_rate": 0.0}
    failures = sum(1 for e in entries if not e.healthy)
    return {
        "pipeline": pipeline,
        "total_runs": total,
        "failures": failures,
        "failure_rate": round(failures / total * 100, 1),
    }


def run_report_cmd(args: Namespace) -> int:
    history = RunHistory(path=args.history_file)
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=args.hours)

    all_entries = history.all()
    recent = [e for e in all_entries if e.timestamp >= cutoff]

    if not recent:
        print("No history entries found in the given window.")
        return 0

    pipelines: dict[str, list] = {}
    for entry in recent:
        pipelines.setdefault(entry.pipeline, []).append(entry)

    reports = [_pipeline_report(p, entries) for p, entries in sorted(pipelines.items())]

    if args.format == "json":
        print(json.dumps({"window_hours": args.hours, "pipelines": reports}, indent=2))
    else:
        print(f"Health report — last {args.hours}h")
        print(f"{'Pipeline':<30} {'Runs':>6} {'Failures':>9} {'Failure %':>10}")
        print("-" * 60)
        for r in reports:
            print(f"{r['pipeline']:<30} {r['total_runs']:>6} {r['failures']:>9} {r['failure_rate']:>9.1f}%")

    any_failing = any(r["failure_rate"] > 0 for r in reports)
    return 1 if (args.exit_code and any_failing) else 0
