"""Pipeline health score command — composite 0-100 score per pipeline."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import Any

from pipewatch.history import RunHistory


def _pipeline_score(entries: list) -> dict[str, Any]:
    """Compute a 0-100 health score from recent history entries."""
    if not entries:
        return {"score": None, "grade": "N/A", "samples": 0}

    total = len(entries)
    healthy = sum(1 for e in entries if e.healthy)
    avg_error_rate = sum(e.error_rate for e in entries) / total
    avg_latency = sum(e.latency_p99 for e in entries) / total

    uptime_score = (healthy / total) * 60
    error_score = max(0.0, 1.0 - avg_error_rate) * 25
    latency_score = max(0.0, 1.0 - min(avg_latency / 60.0, 1.0)) * 15

    score = round(uptime_score + error_score + latency_score, 1)

    if score >= 90:
        grade = "A"
    elif score >= 75:
        grade = "B"
    elif score >= 60:
        grade = "C"
    elif score >= 40:
        grade = "D"
    else:
        grade = "F"

    return {"score": score, "grade": grade, "samples": total}


def run_score_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    pipelines = args.pipeline or history.pipelines()

    if not pipelines:
        print("No history found.")
        return 0

    results: dict[str, Any] = {}
    for name in pipelines:
        entries = history.get(name, hours=args.hours)
        results[name] = _pipeline_score(entries)

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    print(f"{'Pipeline':<30} {'Score':>7} {'Grade':>6} {'Samples':>8}")
    print("-" * 55)
    for name, r in results.items():
        score_str = f"{r['score']:.1f}" if r["score"] is not None else "N/A"
        print(f"{name:<30} {score_str:>7} {r['grade']:>6} {r['samples']:>8}")

    if args.exit_code:
        failing = [n for n, r in results.items() if r["score"] is not None and r["score"] < 60]
        return 1 if failing else 0
    return 0


def register_score_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser("score", help="Show composite health score per pipeline")
    p.add_argument("--pipeline", nargs="+", metavar="NAME")
    p.add_argument("--hours", type=int, default=24)
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.add_argument("--json", action="store_true")
    p.add_argument("--exit-code", action="store_true", help="Exit 1 if any pipeline scores below 60")
    p.set_defaults(func=run_score_cmd)
