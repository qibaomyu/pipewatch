"""Anomaly detection command: flag pipelines whose recent error rate
deviates significantly from their historical baseline."""
from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_anomaly(
    entries: List[HistoryEntry],
    pipeline: str,
    baseline_hours: int,
    recent_hours: int,
    threshold: float,
) -> Optional[dict]:
    now = datetime.now(tz=timezone.utc)
    baseline_cutoff = now - timedelta(hours=baseline_hours)
    recent_cutoff = now - timedelta(hours=recent_hours)

    pipeline_entries = [e for e in entries if e.pipeline == pipeline]
    baseline = [e for e in pipeline_entries if baseline_cutoff <= e.timestamp < recent_cutoff]
    recent = [e for e in pipeline_entries if e.timestamp >= recent_cutoff]

    if not baseline or not recent:
        return None

    baseline_mean = statistics.mean(e.error_rate for e in baseline)
    recent_mean = statistics.mean(e.error_rate for e in recent)

    deviation = recent_mean - baseline_mean
    is_anomaly = deviation > threshold

    return {
        "pipeline": pipeline,
        "baseline_mean": round(baseline_mean, 4),
        "recent_mean": round(recent_mean, 4),
        "deviation": round(deviation, 4),
        "anomaly": is_anomaly,
    }


def run_anomaly_cmd(args: argparse.Namespace) -> int:
    history = RunHistory(path=args.history_file)
    entries = history.all()

    if not entries:
        print("No history available.")
        return 0

    pipelines = (
        [args.pipeline] if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    results = []
    for name in pipelines:
        result = _pipeline_anomaly(
            entries, name,
            baseline_hours=args.baseline_hours,
            recent_hours=args.recent_hours,
            threshold=args.threshold,
        )
        if result:
            results.append(result)

    if args.format == "json":
        print(json.dumps(results, indent=2))
    else:
        if not results:
            print("No anomaly data available (insufficient history).")
            return 0
        for r in results:
            flag = " [ANOMALY]" if r["anomaly"] else ""
            print(
                f"{r['pipeline']}: baseline={r['baseline_mean']:.4f} "
                f"recent={r['recent_mean']:.4f} "
                f"deviation={r['deviation']:+.4f}{flag}"
            )

    if args.exit_code and any(r["anomaly"] for r in results):
        return 1
    return 0
