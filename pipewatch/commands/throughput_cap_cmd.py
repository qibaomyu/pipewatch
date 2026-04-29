"""throughput_cap_cmd: flag pipelines exceeding a maximum run-count ceiling."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from pipewatch.history import RunHistory


def _pipeline_throughput_cap(
    history: RunHistory,
    pipeline: Optional[str],
    hours: int,
    limit: int,
) -> list[dict]:
    """Return per-pipeline run counts and whether they exceed *limit*."""
    cutoff = datetime.now(tz=timezone.utc).timestamp() - hours * 3600
    entries = [
        e for e in history.all()
        if e.timestamp >= cutoff
        and (pipeline is None or e.pipeline == pipeline)
    ]

    counts: dict[str, int] = {}
    for e in entries:
        counts[e.pipeline] = counts.get(e.pipeline, 0) + 1

    results = []
    for name, count in sorted(counts.items()):
        results.append({
            "pipeline": name,
            "runs": count,
            "limit": limit,
            "exceeded": count > limit,
        })
    return results


def run_throughput_cap_cmd(args) -> int:
    history = RunHistory(path=args.history_file)
    rows = _pipeline_throughput_cap(
        history,
        pipeline=getattr(args, "pipeline", None),
        hours=args.hours,
        limit=args.limit,
    )

    if not rows:
        print("No pipeline runs found.")
        return 0

    if getattr(args, "json", False):
        print(json.dumps(rows, indent=2))
        return 1 if any(r["exceeded"] for r in rows) and getattr(args, "exit_code", False) else 0

    header = f"{'Pipeline':<30} {'Runs':>6} {'Limit':>6} {'Status'}"
    print(header)
    print("-" * len(header))
    for r in rows:
        status = "EXCEEDED" if r["exceeded"] else "ok"
        print(f"{r['pipeline']:<30} {r['runs']:>6} {r['limit']:>6}  {status}")

    if getattr(args, "exit_code", False) and any(r["exceeded"] for r in rows):
        return 1
    return 0


def register_throughput_cap_subcommand(subparsers) -> None:
    p = subparsers.add_parser(
        "throughput-cap",
        help="Flag pipelines that exceed a maximum run-count ceiling.",
    )
    p.add_argument("--hours", type=int, default=24, help="Lookback window in hours (default: 24).")
    p.add_argument("--limit", type=int, default=100, help="Maximum allowed runs (default: 100).")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline.")
    p.add_argument("--json", action="store_true", help="Output as JSON.")
    p.add_argument("--exit-code", action="store_true", help="Exit 1 if any pipeline exceeded.")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.set_defaults(func=run_throughput_cap_cmd)
