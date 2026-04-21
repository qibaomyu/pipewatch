"""window_cmd: show aggregated pipeline stats over a rolling time window."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone, timedelta
from typing import Any

from pipewatch.history import RunHistory


def _pipeline_window(entries: list, pipeline: str, hours: int) -> dict[str, Any]:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    relevant = [
        e for e in entries
        if e.pipeline == pipeline and e.timestamp >= cutoff
    ]
    if not relevant:
        return {"pipeline": pipeline, "hours": hours, "runs": 0,
                "healthy": 0, "failing": 0, "avg_error_rate": None,
                "avg_latency_ms": None}

    healthy = sum(1 for e in relevant if e.healthy)
    failing = len(relevant) - healthy
    error_rates = [e.error_rate for e in relevant if e.error_rate is not None]
    latencies = [e.latency_ms for e in relevant if e.latency_ms is not None]

    return {
        "pipeline": pipeline,
        "hours": hours,
        "runs": len(relevant),
        "healthy": healthy,
        "failing": failing,
        "avg_error_rate": round(sum(error_rates) / len(error_rates), 4) if error_rates else None,
        "avg_latency_ms": round(sum(latencies) / len(latencies), 2) if latencies else None,
    }


def run_window_cmd(args: Namespace) -> int:
    history = RunHistory(path=args.history_file)
    entries = history.all()

    pipelines = (
        [args.pipeline] if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    if not pipelines:
        print("No history entries found.")
        return 0

    results = [_pipeline_window(entries, p, args.hours) for p in pipelines]

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Hours':>5} {'Runs':>5} {'OK':>5} {'Fail':>5} {'Avg Err%':>10} {'Avg Lat ms':>12}"
    print(header)
    print("-" * len(header))
    for r in results:
        err = f"{r['avg_error_rate']:.4f}" if r['avg_error_rate'] is not None else "n/a"
        lat = f"{r['avg_latency_ms']:.1f}" if r['avg_latency_ms'] is not None else "n/a"
        print(f"{r['pipeline']:<30} {r['hours']:>5} {r['runs']:>5} {r['healthy']:>5} {r['failing']:>5} {err:>10} {lat:>12}")
    return 0


def register_window_subcommand(sub) -> None:
    p: ArgumentParser = sub.add_parser(
        "window", help="Aggregate pipeline stats over a rolling time window"
    )
    p.add_argument("--hours", type=int, default=24, help="Window size in hours (default: 24)")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--history-file", default=".pipewatch_history.json",
                   help="Path to history file")
    p.set_defaults(func=run_window_cmd)
