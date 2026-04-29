"""breach_cmd: report pipelines that have exceeded their error-rate threshold
more than N times within a rolling window."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Optional

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_breach(
    entries: list[HistoryEntry],
    pipeline: str,
    hours: int,
    min_breaches: int,
) -> Optional[dict]:
    """Return breach stats for a single pipeline or None if no entries."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    relevant = [
        e for e in entries
        if e.pipeline == pipeline and e.timestamp >= cutoff
    ]
    if not relevant:
        return None

    breach_count = sum(1 for e in relevant if e.alerts)
    total = len(relevant)
    breached = breach_count >= min_breaches
    return {
        "pipeline": pipeline,
        "total_runs": total,
        "breach_count": breach_count,
        "breached": breached,
    }


def run_breach_cmd(args) -> int:
    history = RunHistory(path=args.history_file)
    entries = history.all()

    pipelines = (
        [args.pipeline]
        if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    results = []
    for name in pipelines:
        stat = _pipeline_breach(entries, name, args.hours, args.min_breaches)
        if stat is not None:
            results.append(stat)

    if not results:
        print("No history entries found.")
        return 0

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print(f"{'Pipeline':<30} {'Runs':>6} {'Breaches':>9} {'Status':<10}")
        print("-" * 60)
        for r in results:
            status = "BREACHED" if r["breached"] else "ok"
            print(
                f"{r['pipeline']:<30} {r['total_runs']:>6} "
                f"{r['breach_count']:>9} {status:<10}"
            )

    if args.exit_code:
        return 1 if any(r["breached"] for r in results) else 0
    return 0


def register_breach_subcommand(subparsers) -> None:
    p = subparsers.add_parser(
        "breach",
        help="Report pipelines that breached error thresholds repeatedly.",
    )
    p.add_argument("--hours", type=int, default=24, help="Rolling window in hours.")
    p.add_argument("--min-breaches", type=int, default=3,
                   dest="min_breaches",
                   help="Minimum breach count to flag a pipeline.")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline.")
    p.add_argument("--json", action="store_true", help="Output as JSON.")
    p.add_argument("--exit-code", action="store_true", dest="exit_code",
                   help="Exit 1 if any pipeline is breached.")
    p.add_argument("--history-file", default=".pipewatch_history.json",
                   dest="history_file")
    p.set_defaults(func=run_breach_cmd)
