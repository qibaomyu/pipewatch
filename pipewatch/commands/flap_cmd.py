"""Flap detection: pipelines that alternate between healthy and failing."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import List

from pipewatch.history import RunHistory, HistoryEntry


def _flap_score(entries: List[HistoryEntry]) -> int:
    """Count state transitions (healthy<->failing) in chronological order."""
    if len(entries) < 2:
        return 0
    transitions = 0
    prev = entries[0].healthy
    for e in entries[1:]:
        if e.healthy != prev:
            transitions += 1
        prev = e.healthy
    return transitions


def _pipeline_flap(pipeline: str, entries: List[HistoryEntry], threshold: int):
    ordered = sorted(entries, key=lambda e: e.timestamp)
    score = _flap_score(ordered)
    return {
        "pipeline": pipeline,
        "transitions": score,
        "flapping": score >= threshold,
        "entries_checked": len(ordered),
    }


def run_flap_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    all_entries = history.all()

    pipelines = {e.pipeline for e in all_entries}
    if args.pipeline:
        pipelines = {p for p in pipelines if p == args.pipeline}

    if not pipelines:
        print("No history found.")
        return 0

    results = [
        _pipeline_flap(
            p,
            [e for e in all_entries if e.pipeline == p],
            args.threshold,
        )
        for p in sorted(pipelines)
    ]

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    for r in results:
        symbol = "⚡" if r["flapping"] else "✓"
        print(f"{symbol} {r['pipeline']:30s}  transitions={r['transitions']}  flapping={r['flapping']}")

    if args.exit_code and any(r["flapping"] for r in results):
        return 1
    return 0


def register_flap_subcommand(sub) -> None:
    p: ArgumentParser = sub.add_parser("flap", help="Detect flapping pipelines")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--threshold", type=int, default=3, help="Min transitions to flag as flapping (default: 3)")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--exit-code", action="store_true", help="Exit 1 if any pipeline is flapping")
    p.set_defaults(func=run_flap_cmd)
