"""dormant_cmd: identify pipelines that have never run or have no history entries."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.config import AppConfig, load_config
from pipewatch.history import RunHistory


def _pipeline_dormant(
    pipeline_name: str,
    history: RunHistory,
    hours: float,
) -> dict:
    """Return dormancy info for a single pipeline."""
    cutoff = datetime.now(tz=timezone.utc).timestamp() - hours * 3600
    entries = [
        e for e in history.get(pipeline_name)
        if e.timestamp >= cutoff
    ]
    all_entries = history.get(pipeline_name)
    last_run: Optional[float] = max((e.timestamp for e in all_entries), default=None)
    return {
        "pipeline": pipeline_name,
        "dormant": len(entries) == 0,
        "last_run": (
            datetime.fromtimestamp(last_run, tz=timezone.utc).isoformat()
            if last_run is not None
            else None
        ),
        "runs_in_window": len(entries),
    }


def run_dormant_cmd(args: argparse.Namespace) -> int:
    cfg: AppConfig = load_config(args.config)
    if cfg is None:
        print("error: config file not found")
        return 2

    history = RunHistory(args.history_file)
    pipelines: List[str] = (
        [args.pipeline]
        if getattr(args, "pipeline", None)
        else [p.name for p in cfg.pipelines]
    )

    results = [_pipeline_dormant(p, history, args.hours) for p in pipelines]
    dormant_results = [r for r in results if r["dormant"]] if getattr(args, "only_dormant", False) else results

    if args.json:
        print(json.dumps(dormant_results, indent=2))
    else:
        if not dormant_results:
            print("No pipelines found.")
            return 0
        for r in dormant_results:
            status = "DORMANT" if r["dormant"] else "active"
            last = r["last_run"] or "never"
            print(f"{r['pipeline']:<30} {status:<8}  last_run={last}  runs={r['runs_in_window']}")

    if getattr(args, "exit_code", False):
        return 1 if any(r["dormant"] for r in results) else 0
    return 0


def register_dormant_subcommand(subparsers) -> None:
    p = subparsers.add_parser("dormant", help="List pipelines with no recent runs")
    p.add_argument("--config", default="pipewatch.yaml")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.add_argument("--hours", type=float, default=24.0, help="Lookback window in hours")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--only-dormant", action="store_true", help="Show only dormant pipelines")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--exit-code", action="store_true", help="Exit 1 if any pipeline is dormant")
    p.set_defaults(func=run_dormant_cmd)
