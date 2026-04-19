"""spike_cmd: detect sudden error-rate spikes compared to a rolling baseline."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.history import RunHistory, HistoryEntry


def _rolling_avg(entries: List[HistoryEntry]) -> Optional[float]:
    if not entries:
        return None
    return sum(e.error_rate for e in entries) / len(entries)


def _pipeline_spike(pipeline: str, entries: List[HistoryEntry], window: int, multiplier: float):
    pipeline_entries = [e for e in entries if e.pipeline == pipeline]
    pipeline_entries.sort(key=lambda e: e.timestamp)
    if len(pipeline_entries) < 2:
        return {"pipeline": pipeline, "spike": False, "reason": "insufficient data"}

    baseline_entries = pipeline_entries[:-window] if len(pipeline_entries) > window else []
    recent_entries = pipeline_entries[-window:]

    baseline_avg = _rolling_avg(baseline_entries)
    recent_avg = _rolling_avg(recent_entries)

    if baseline_avg is None or baseline_avg == 0:
        return {"pipeline": pipeline, "spike": False, "reason": "no baseline"}

    ratio = recent_avg / baseline_avg
    spike = ratio >= multiplier
    return {
        "pipeline": pipeline,
        "spike": spike,
        "baseline_avg": round(baseline_avg, 4),
        "recent_avg": round(recent_avg, 4),
        "ratio": round(ratio, 4),
        "reason": f"ratio {ratio:.2f}x (threshold {multiplier}x)",
    }


def run_spike_cmd(args: argparse.Namespace) -> int:
    history = RunHistory(args.history_file)
    entries = history.all()

    pipelines = (
        [args.pipeline]
        if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    results = [_pipeline_spike(p, entries, args.window, args.multiplier) for p in pipelines]

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        if not results:
            print("No pipeline data found.")
            return 0
        for r in results:
            flag = "SPIKE" if r["spike"] else "ok"
            if "baseline_avg" in r:
                print(f"{r['pipeline']:<30} {flag:<6}  {r['reason']}")
            else:
                print(f"{r['pipeline']:<30} {flag:<6}  {r['reason']}")

    if args.exit_code and any(r["spike"] for r in results):
        return 1
    return 0


def register_spike_subcommand(subparsers):
    p = subparsers.add_parser("spike", help="Detect sudden error-rate spikes")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.add_argument("--pipeline", default=None)
    p.add_argument("--window", type=int, default=5, help="Recent entries to compare")
    p.add_argument("--multiplier", type=float, default=2.0, help="Spike threshold multiplier")
    p.add_argument("--json", action="store_true")
    p.add_argument("--exit-code", action="store_true")
    p.set_defaults(func=run_spike_cmd)
