"""coverage_cmd: report what fraction of configured pipelines have run recently."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from pipewatch.config import load_config
from pipewatch.history import RunHistory


def _pipeline_coverage(
    pipeline_names: List[str],
    history: RunHistory,
    hours: int,
    pipeline_filter: Optional[str],
) -> List[dict]:
    """Return coverage info for each configured pipeline."""
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    results = []
    for name in pipeline_names:
        if pipeline_filter and name != pipeline_filter:
            continue
        entries = [
            e for e in history.get(name)
            if e.timestamp >= cutoff
        ]
        covered = len(entries) > 0
        last_run = max((e.timestamp for e in entries), default=None)
        results.append({
            "pipeline": name,
            "covered": covered,
            "run_count": len(entries),
            "last_run": last_run.isoformat() if last_run else None,
        })
    return results


def run_coverage_cmd(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("ERROR: config file not found")
        return 2

    history = RunHistory(args.history_file)
    pipeline_names = list(cfg.pipelines.keys())
    rows = _pipeline_coverage(
        pipeline_names, history, args.hours, getattr(args, "pipeline", None)
    )

    if not rows:
        print("No pipelines found.")
        return 0

    covered = sum(1 for r in rows if r["covered"])
    total = len(rows)
    pct = 100.0 * covered / total if total else 0.0

    if args.json:
        print(json.dumps({"coverage_pct": round(pct, 1), "pipelines": rows}, indent=2))
        return 0

    print(f"Pipeline coverage ({args.hours}h): {covered}/{total}  ({pct:.1f}%)")
    print()
    for r in rows:
        symbol = "✔" if r["covered"] else "✘"
        last = r["last_run"] or "never"
        print(f"  {symbol}  {r['pipeline']:<30}  runs={r['run_count']}  last={last}")

    if args.exit_code and covered < total:
        return 1
    return 0


def register_coverage_subcommand(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = sub.add_parser("coverage", help="Show which pipelines have run recently")
    p.add_argument("--config", default="pipewatch.yaml")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--exit-code",
        action="store_true",
        help="Exit 1 if any pipeline has no coverage",
    )
    p.set_defaults(func=run_coverage_cmd)
