"""cascade_cmd: detect pipelines whose failures tend to follow another pipeline's failure."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from pipewatch.history import RunHistory, HistoryEntry


def _failure_times(entries: List[HistoryEntry], pipeline: str) -> List[datetime]:
    """Return timestamps of failed runs for a given pipeline."""
    return [
        e.timestamp
        for e in entries
        if e.pipeline == pipeline and not e.healthy
    ]


def _pipeline_cascade(
    entries: List[HistoryEntry],
    lead: str,
    window_minutes: int = 10,
) -> List[Dict]:
    """For each pipeline other than *lead*, count how many of its failures
    occurred within *window_minutes* after a *lead* failure."""
    lead_times = _failure_times(entries, lead)
    if not lead_times:
        return []

    pipelines = sorted({e.pipeline for e in entries} - {lead})
    results = []
    for pipeline in pipelines:
        follower_times = _failure_times(entries, pipeline)
        cascade_count = 0
        for ft in follower_times:
            for lt in lead_times:
                if timedelta(0) <= (ft - lt) <= timedelta(minutes=window_minutes):
                    cascade_count += 1
                    break
        results.append(
            {
                "pipeline": pipeline,
                "cascade_failures": cascade_count,
                "total_failures": len(follower_times),
                "cascade_rate": round(cascade_count / len(follower_times), 3)
                if follower_times
                else 0.0,
            }
        )
    results.sort(key=lambda r: r["cascade_rate"], reverse=True)
    return results


def run_cascade_cmd(args: Namespace) -> int:
    history = RunHistory(path=args.history_file)
    entries = history.all()

    rows = _pipeline_cascade(entries, lead=args.lead, window_minutes=args.window)

    if not rows:
        print(f"No cascade data found for lead pipeline '{args.lead}'.")
        return 0

    if args.json:
        print(json.dumps({"lead": args.lead, "cascades": rows}, indent=2))
        return 0

    print(f"Cascade analysis  (lead: {args.lead}, window: {args.window}m)")
    print(f"{'Pipeline':<30} {'Cascade':>8} {'Total':>7} {'Rate':>7}")
    print("-" * 58)
    for r in rows:
        print(
            f"{r['pipeline']:<30} {r['cascade_failures']:>8} "
            f"{r['total_failures']:>7} {r['cascade_rate']:>7.1%}"
        )
    return 0


def register_cascade_subcommand(sub) -> None:
    p: ArgumentParser = sub.add_parser(
        "cascade", help="Detect pipelines that fail in the wake of a lead pipeline."
    )
    p.add_argument("lead", help="Name of the lead pipeline to analyse.")
    p.add_argument(
        "--window",
        type=int,
        default=10,
        metavar="MINUTES",
        help="Time window in minutes after a lead failure (default: 10).",
    )
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        metavar="FILE",
    )
    p.add_argument("--json", action="store_true", help="Output as JSON.")
    p.set_defaults(func=run_cascade_cmd)
