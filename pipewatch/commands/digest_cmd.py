"""digest_cmd: email-style digest summary of pipeline health over a window."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import List

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_digest(entries: List[HistoryEntry], pipeline: str):
    pipe_entries = [e for e in entries if e.pipeline == pipeline]
    if not pipe_entries:
        return None
    total = len(pipe_entries)
    failures = sum(1 for e in pipe_entries if not e.healthy)
    avg_latency = sum(e.latency for e in pipe_entries) / total
    avg_error_rate = sum(e.error_rate for e in pipe_entries) / total
    last_run = max(e.timestamp for e in pipe_entries)
    return {
        "pipeline": pipeline,
        "total_runs": total,
        "failures": failures,
        "failure_rate": round(failures / total, 3),
        "avg_latency": round(avg_latency, 3),
        "avg_error_rate": round(avg_error_rate, 3),
        "last_run": last_run.isoformat(),
    }


def run_digest_cmd(args: argparse.Namespace) -> int:
    history = RunHistory(args.history_file)
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=args.hours)
    entries = [e for e in history.all() if e.timestamp >= cutoff]

    pipelines = sorted({e.pipeline for e in entries})
    if args.pipeline:
        pipelines = [p for p in pipelines if p == args.pipeline]

    if not pipelines:
        print("No data found for the specified window.")
        return 0

    digests = [_pipeline_digest(entries, p) for p in pipelines]
    digests = [d for d in digests if d]

    if args.format == "json":
        print(json.dumps(digests, indent=2))
        return 0

    print(f"=== Pipeline Digest (last {args.hours}h) ===")
    for d in digests:
        status = "FAIL" if d["failures"] > 0 else "OK"
        print(
            f"  [{status}] {d['pipeline']}: "
            f"{d['failures']}/{d['total_runs']} failures, "
            f"avg latency {d['avg_latency']}s, "
            f"avg error rate {d['avg_error_rate']}"
        )
    return 0
