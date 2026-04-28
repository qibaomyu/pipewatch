"""rollup_cmd.py – aggregate pipeline health into a single rollup summary."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import Any

from pipewatch.history import RunHistory


def _pipeline_rollup(
    history: RunHistory,
    pipeline: str | None,
    hours: float,
) -> list[dict[str, Any]]:
    """Return a rollup record per pipeline within the last *hours*."""
    now = datetime.now(tz=timezone.utc).timestamp()
    cutoff = now - hours * 3600

    entries = [
        e for e in history.all()
        if e.timestamp >= cutoff
        and (pipeline is None or e.pipeline == pipeline)
    ]

    pipelines: dict[str, list] = {}
    for e in entries:
        pipelines.setdefault(e.pipeline, []).append(e)

    results: list[dict[str, Any]] = []
    for name, runs in sorted(pipelines.items()):
        total = len(runs)
        failed = sum(1 for r in runs if not r.healthy)
        error_rates = [r.error_rate for r in runs if r.error_rate is not None]
        latencies = [r.latency for r in runs if r.latency is not None]
        results.append({
            "pipeline": name,
            "total_runs": total,
            "failed_runs": failed,
            "success_rate": round((total - failed) / total * 100, 1) if total else 0.0,
            "avg_error_rate": round(sum(error_rates) / len(error_rates), 4) if error_rates else None,
            "avg_latency": round(sum(latencies) / len(latencies), 3) if latencies else None,
        })
    return results


def run_rollup_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    rows = _pipeline_rollup(history, args.pipeline, args.hours)

    if not rows:
        print("No history entries found for the specified window.")
        return 0

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Runs':>6} {'Failed':>7} {'Success%':>9} {'AvgErrRate':>11} {'AvgLatency':>11}"
    print(header)
    print("-" * len(header))
    for r in rows:
        err = f"{r['avg_error_rate']:.4f}" if r["avg_error_rate"] is not None else "  n/a"
        lat = f"{r['avg_latency']:.3f}s" if r["avg_latency"] is not None else "   n/a"
        print(
            f"{r['pipeline']:<30} {r['total_runs']:>6} {r['failed_runs']:>7}"
            f" {r['success_rate']:>8.1f}% {err:>11} {lat:>11}"
        )
    return 0


def register_rollup_subcommand(subparsers: Any) -> None:  # pragma: no cover
    p: ArgumentParser = subparsers.add_parser(
        "rollup", help="Aggregate pipeline health into a rollup summary"
    )
    p.add_argument("--hours", type=float, default=24.0, help="Look-back window in hours (default: 24)")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.set_defaults(func=run_rollup_cmd)
