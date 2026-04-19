"""error_rate_cmd: show error rate statistics per pipeline over a time window."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_error_rate(
    entries: List[HistoryEntry], pipeline: str
) -> Dict[str, Any]:
    pipe_entries = [e for e in entries if e.pipeline == pipeline]
    if not pipe_entries:
        return {"pipeline": pipeline, "count": 0, "avg_error_rate": None, "max_error_rate": None}
    rates = [e.error_rate for e in pipe_entries if e.error_rate is not None]
    if not rates:
        return {"pipeline": pipeline, "count": len(pipe_entries), "avg_error_rate": None, "max_error_rate": None}
    return {
        "pipeline": pipeline,
        "count": len(pipe_entries),
        "avg_error_rate": round(sum(rates) / len(rates), 4),
        "max_error_rate": round(max(rates), 4),
    }


def run_error_rate_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    entries = [e for e in history.all() if e.timestamp >= cutoff]

    pipelines = (
        [args.pipeline]
        if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    if not pipelines:
        print("No history entries found.")
        return 0

    results = [_pipeline_error_rate(entries, p) for p in pipelines]

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Runs':>6} {'Avg Err%':>10} {'Max Err%':>10}"
    print(header)
    print("-" * len(header))
    for r in results:
        avg = f"{r['avg_error_rate']*100:.2f}" if r['avg_error_rate'] is not None else "N/A"
        mx = f"{r['max_error_rate']*100:.2f}" if r['max_error_rate'] is not None else "N/A"
        print(f"{r['pipeline']:<30} {r['count']:>6} {avg:>10} {mx:>10}")
    return 0


def register_error_rate_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "error-rate", help="Show error rate statistics per pipeline"
    )
    p.add_argument("--hours", type=int, default=24, help="Lookback window in hours (default: 24)")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.set_defaults(func=run_error_rate_cmd)
