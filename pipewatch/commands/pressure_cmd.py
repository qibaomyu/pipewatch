"""pressure_cmd: report queue pressure (error-rate × latency) per pipeline."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import Dict, List, Optional

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_pressure(
    entries: List[HistoryEntry],
    pipeline: str,
    hours: float,
) -> Optional[Dict]:
    """Compute pressure score = avg_error_rate * avg_latency_ms for a pipeline."""
    from datetime import datetime, timezone, timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    relevant = [
        e for e in entries
        if e.pipeline == pipeline and e.timestamp >= cutoff
    ]
    if not relevant:
        return None

    avg_error_rate = sum(e.error_rate for e in relevant) / len(relevant)
    avg_latency = sum(e.latency_ms for e in relevant) / len(relevant)
    score = avg_error_rate * avg_latency

    return {
        "pipeline": pipeline,
        "avg_error_rate": round(avg_error_rate, 4),
        "avg_latency_ms": round(avg_latency, 2),
        "pressure_score": round(score, 4),
        "sample_size": len(relevant),
    }


def run_pressure_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    entries = history.all()

    pipelines = (
        [args.pipeline] if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    results = []
    for p in pipelines:
        row = _pipeline_pressure(entries, p, args.hours)
        if row:
            results.append(row)

    if not results:
        print("No data available.")
        return 0

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Err Rate':>10} {'Latency ms':>12} {'Pressure':>12} {'N':>6}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['pipeline']:<30} {r['avg_error_rate']:>10.4f}"
            f" {r['avg_latency_ms']:>12.2f} {r['pressure_score']:>12.4f}"
            f" {r['sample_size']:>6}"
        )
    return 0


def register_pressure_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "pressure",
        help="Report queue pressure score (error_rate × latency) per pipeline.",
    )
    p.add_argument("--hours", type=float, default=24.0, help="Lookback window in hours.")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline.")
    p.add_argument("--json", action="store_true", help="Output as JSON.")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file.",
    )
    p.set_defaults(func=run_pressure_cmd)
