"""bottleneck_cmd: identify pipelines with consistently high latency relative to peers."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import List, Dict, Any

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_bottleneck(
    entries: List[HistoryEntry], pipeline: str, hours: int
) -> Dict[str, Any]:
    from datetime import datetime, timezone, timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    relevant = [
        e for e in entries
        if e.pipeline == pipeline and e.timestamp >= cutoff and e.latency is not None
    ]
    if not relevant:
        return {"pipeline": pipeline, "avg_latency": None, "max_latency": None, "count": 0}

    latencies = [e.latency for e in relevant]
    return {
        "pipeline": pipeline,
        "avg_latency": round(sum(latencies) / len(latencies), 3),
        "max_latency": round(max(latencies), 3),
        "count": len(latencies),
    }


def run_bottleneck_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    entries = history.all()

    pipelines = (
        [args.pipeline] if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    rows = [
        _pipeline_bottleneck(entries, p, args.hours)
        for p in pipelines
    ]
    rows = [r for r in rows if r["count"] > 0]

    if not rows:
        print("No latency data found.")
        return 0

    rows.sort(key=lambda r: r["avg_latency"] or 0, reverse=True)

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Avg Latency':>12} {'Max Latency':>12} {'Samples':>8}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r['pipeline']:<30} {r['avg_latency']:>12.3f} {r['max_latency']:>12.3f} {r['count']:>8}"
        )
    return 0


def register_bottleneck_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "bottleneck", help="Identify pipelines with high latency"
    )
    p.add_argument("--hours", type=int, default=24, help="Look-back window in hours")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.set_defaults(func=run_bottleneck_cmd)
