"""SLA compliance command — reports whether pipelines meet error-rate and latency SLAs."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Optional

from pipewatch.history import RunHistory


def _pipeline_sla(
    history: RunHistory,
    pipeline: Optional[str],
    hours: int,
    max_error_rate: float,
    max_latency: float,
) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    results = []
    names = {e.pipeline for e in history.entries if e.timestamp >= cutoff}
    if pipeline:
        names = {n for n in names if n == pipeline}
    for name in sorted(names):
        entries = [
            e for e in history.entries
            if e.pipeline == name and e.timestamp >= cutoff
        ]
        if not entries:
            continue
        avg_error = sum(e.error_rate for e in entries) / len(entries)
        avg_latency = sum(e.latency for e in entries) / len(entries)
        breaches = [
            e for e in entries
            if e.error_rate > max_error_rate or e.latency > max_latency
        ]
        compliance = round(100.0 * (1 - len(breaches) / len(entries)), 2)
        results.append({
            "pipeline": name,
            "total_runs": len(entries),
            "breaches": len(breaches),
            "compliance_pct": compliance,
            "avg_error_rate": round(avg_error, 4),
            "avg_latency": round(avg_latency, 4),
            "meets_sla": compliance >= 100.0 * (1 - max(max_error_rate, 0)),
        })
    return results


def run_sla_cmd(args, history: Optional[RunHistory] = None) -> int:
    if history is None:
        history = RunHistory(path=args.history_file)
    data = _pipeline_sla(
        history,
        pipeline=args.pipeline,
        hours=args.hours,
        max_error_rate=args.max_error_rate,
        max_latency=args.max_latency,
    )
    if not data:
        print("No SLA data available.")
        return 0
    if args.json:
        print(json.dumps(data, indent=2))
    else:
        print(f"{'Pipeline':<30} {'Runs':>6} {'Breaches':>9} {'Compliance':>11} {'Meets SLA':>10}")
        print("-" * 70)
        for row in data:
            flag = "YES" if row["meets_sla"] else "NO"
            print(
                f"{row['pipeline']:<30} {row['total_runs']:>6} {row['breaches']:>9}"
                f" {row['compliance_pct']:>10.2f}% {flag:>10}"
            )
    if args.exit_code:
        return 0 if all(r["meets_sla"] for r in data) else 1
    return 0


def register_sla_subcommand(subparsers) -> None:
    p = subparsers.add_parser("sla", help="Report SLA compliance for pipelines")
    p.add_argument("--hours", type=int, default=24)
    p.add_argument("--pipeline", default=None)
    p.add_argument("--max-error-rate", type=float, default=0.05, dest="max_error_rate")
    p.add_argument("--max-latency", type=float, default=5.0, dest="max_latency")
    p.add_argument("--json", action="store_true", default=False)
    p.add_argument("--exit-code", action="store_true", default=False, dest="exit_code")
    p.add_argument("--history-file", default=".pipewatch_history.json", dest="history_file")
    p.set_defaults(func=run_sla_cmd)
