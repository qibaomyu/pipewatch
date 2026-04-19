"""stale_cmd: detect pipelines that have not reported recently."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import List, Dict, Any

from pipewatch.history import RunHistory


def _pipeline_stale(
    history: RunHistory,
    pipeline: str,
    threshold_minutes: int,
) -> Dict[str, Any]:
    entries = [e for e in history.get(pipeline) if e.pipeline == pipeline]
    if not entries:
        return {"pipeline": pipeline, "stale": True, "last_seen": None, "minutes_since": None}

    latest = max(entries, key=lambda e: e.timestamp)
    now = datetime.now(timezone.utc)
    delta = (now - latest.timestamp).total_seconds() / 60
    return {
        "pipeline": pipeline,
        "stale": delta > threshold_minutes,
        "last_seen": latest.timestamp.isoformat(),
        "minutes_since": round(delta, 1),
    }


def run_stale_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    pipelines: List[str] = (
        [args.pipeline] if args.pipeline else history.pipelines()
    )

    if not pipelines:
        print("No history found.")
        return 0

    results = [_pipeline_stale(history, p, args.threshold) for p in pipelines]

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for r in results:
            symbol = "!" if r["stale"] else "✓"
            since = f"{r['minutes_since']} min ago" if r["minutes_since"] is not None else "never"
            print(f"[{symbol}] {r['pipeline']:<30} last seen: {since}")

    if args.exit_code and any(r["stale"] for r in results):
        return 1
    return 0


def register_stale_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "stale", help="Detect pipelines that have not reported recently"
    )
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--threshold", type=int, default=60, help="Minutes before a pipeline is considered stale (default: 60)")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--exit-code", action="store_true", help="Return exit code 1 if any pipeline is stale")
    p.set_defaults(func=run_stale_cmd)
