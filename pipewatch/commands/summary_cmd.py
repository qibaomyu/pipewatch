"""Summary command: show aggregated pipeline health across all pipelines."""
from __future__ import annotations

import json
from argparse import Namespace
from typing import List

from pipewatch.history import RunHistory
from pipewatch.alerts import AlertLevel


def _pipeline_summary(history: RunHistory, pipeline: str, last_n: int) -> dict:
    entries = history.get(pipeline, limit=last_n)
    if not entries:
        return {"pipeline": pipeline, "runs": 0, "failures": 0, "warnings": 0, "healthy": 0}

    failures = sum(1 for e in entries if e.status == "failing")
    warnings = sum(1 for e in entries if e.status == "warning")
    healthy = sum(1 for e in entries if e.status == "healthy")
    return {
        "pipeline": pipeline,
        "runs": len(entries),
        "failures": failures,
        "warnings": warnings,
        "healthy": healthy,
        "failure_rate": round(failures / len(entries), 3),
    }


def run_summary_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    pipelines = history.pipelines()

    if not pipelines:
        print("No history found.")
        return 0

    summaries = [_pipeline_summary(history, p, args.last) for p in sorted(pipelines)]

    if getattr(args, "format", "text") == "json":
        print(json.dumps(summaries, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Runs':>5} {'Healthy':>8} {'Warnings':>9} {'Failures':>9} {'Fail%':>7}"
    print(header)
    print("-" * len(header))
    for s in summaries:
        rate = f"{s.get('failure_rate', 0)*100:.1f}%"
        print(f"{s['pipeline']:<30} {s['runs']:>5} {s['healthy']:>8} {s['warnings']:>9} {s['failures']:>9} {rate:>7}")

    return 0
