"""quota_cmd: alert when a pipeline exceeds a run-count quota within a time window."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone, timedelta
from typing import List

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_quota(
    entries: List[HistoryEntry],
    pipeline: str,
    hours: int,
    limit: int,
) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    runs = [
        e for e in entries
        if e.pipeline == pipeline and e.timestamp >= cutoff
    ]
    count = len(runs)
    exceeded = count > limit
    return {
        "pipeline": pipeline,
        "runs": count,
        "limit": limit,
        "hours": hours,
        "exceeded": exceeded,
    }


def run_quota_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    entries = history.all()

    pipelines = (
        [args.pipeline] if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    if not pipelines:
        print("No history found.")
        return 0

    results = [
        _pipeline_quota(entries, p, args.hours, args.limit)
        for p in pipelines
    ]

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for r in results:
            status = "EXCEEDED" if r["exceeded"] else "ok"
        print(
                f"{r['pipeline']}: {r['runs']}/{r['limit']} runs "
                f"in last {r['hours']}h [{status}]"
            )

    if args.exit_code and any(r["exceeded"] for r in results):
        return 1
    return 0


def register_quota_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "quota", help="Check run-count quotas for pipelines"
    )
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--hours", type=int, default=24, help="Time window in hours")
    p.add_argument("--limit", type=int, default=100, help="Max allowed runs")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--exit-code", action="store_true", help="Exit 1 if any quota exceeded")
    p.add_argument(
        "--history-file", default=".pipewatch_history.json", help="Path to history file"
    )
    p.set_defaults(func=run_quota_cmd)
