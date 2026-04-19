"""latency_cmd: report average, min, and max latency per pipeline."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import Dict, List, Optional

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_latency(
    entries: List[HistoryEntry],
    pipeline: Optional[str],
    hours: int,
) -> List[Dict]:
    from datetime import datetime, timezone, timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    filtered = [
        e for e in entries
        if e.timestamp >= cutoff and (pipeline is None or e.pipeline == pipeline)
    ]

    grouped: Dict[str, List[float]] = {}
    for e in filtered:
        if e.latency_ms is not None:
            grouped.setdefault(e.pipeline, []).append(e.latency_ms)

    results = []
    for name, values in sorted(grouped.items()):
        results.append({
            "pipeline": name,
            "count": len(values),
            "avg_ms": round(sum(values) / len(values), 2),
            "min_ms": round(min(values), 2),
            "max_ms": round(max(values), 2),
        })
    return results


def _format_text(rows: List[Dict]) -> str:
    if not rows:
        return "No latency data found."
    lines = [f"{'Pipeline':<30} {'Count':>6} {'Avg ms':>10} {'Min ms':>10} {'Max ms':>10}"]
    lines.append("-" * 70)
    for r in rows:
        lines.append(
            f"{r['pipeline']:<30} {r['count']:>6} {r['avg_ms']:>10} {r['min_ms']:>10} {r['max_ms']:>10}"
        )
    return "\n".join(lines)


def run_latency_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    entries = history.all()
    rows = _pipeline_latency(entries, getattr(args, "pipeline", None), args.hours)

    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        print(_format_text(rows))
    return 0


def register_latency_subcommand(sub) -> None:
    p: ArgumentParser = sub.add_parser("latency", help="Show latency stats per pipeline")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.set_defaults(func=run_latency_cmd)
