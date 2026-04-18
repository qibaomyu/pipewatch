"""uptime_cmd: report pipeline uptime percentage over a time window."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta, timezone
from typing import List

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_uptime(entries: List[HistoryEntry]) -> dict:
    if not entries:
        return {"total": 0, "healthy": 0, "uptime_pct": None}
    healthy = sum(1 for e in entries if e.healthy)
    total = len(entries)
    return {
        "total": total,
        "healthy": healthy,
        "uptime_pct": round(healthy / total * 100, 2),
    }


def _format_text(results: dict) -> str:
    lines = []
    for pipeline, stats in results.items():
        if stats["uptime_pct"] is None:
            lines.append(f"  {pipeline}: no data")
        else:
            lines.append(
                f"  {pipeline}: {stats['uptime_pct']}% "
                f"({stats['healthy']}/{stats['total']} healthy)"
            )
    return "\n".join(lines)


def run_uptime_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    pipelines = args.pipeline or None
    all_entries = history.all()
    filtered = [e for e in all_entries if e.timestamp >= cutoff]

    pipeline_names = pipelines if pipelines else sorted({e.pipeline for e in filtered})
    if not pipeline_names:
        print("No history found.")
        return 0

    results = {
        p: _pipeline_uptime([e for e in filtered if e.pipeline == p])
        for p in pipeline_names
    }

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print(f"Uptime report (last {args.hours}h):")
        print(_format_text(results))

    if args.exit_code:
        failing = [p for p, s in results.items() if s["uptime_pct"] is not None and s["uptime_pct"] < args.min_uptime]
        return 1 if failing else 0
    return 0


def register_uptime_subcommand(sub) -> None:
    p: ArgumentParser = sub.add_parser("uptime", help="Show pipeline uptime over a window")
    p.add_argument("--hours", type=float, default=24.0, help="Window in hours (default: 24)")
    p.add_argument("--pipeline", nargs="+", metavar="NAME", help="Filter to specific pipelines")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--exit-code", action="store_true", help="Exit 1 if any pipeline below threshold")
    p.add_argument("--min-uptime", type=float, default=95.0, metavar="PCT", help="Minimum uptime %% (default: 95)")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.set_defaults(func=run_uptime_cmd)
