"""rerun_cmd: track and report pipeline re-run attempts and their outcomes."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pipewatch.history import RunHistory


def _pipeline_rerun(
    entries: list[Any],
    pipeline: str,
    hours: float,
) -> dict[str, Any] | None:
    """Return rerun statistics for a single pipeline within the time window."""
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    relevant = [
        e for e in entries
        if e.pipeline == pipeline and e.timestamp >= cutoff
    ]
    if not relevant:
        return None

    total = len(relevant)
    reruns = [e for e in relevant if getattr(e, "rerun", False)]
    rerun_count = len(reruns)
    rerun_success = sum(1 for e in reruns if e.healthy)
    success_rate = round(rerun_success / rerun_count * 100, 1) if rerun_count else 0.0

    return {
        "pipeline": pipeline,
        "total_runs": total,
        "rerun_count": rerun_count,
        "rerun_success": rerun_success,
        "success_rate_pct": success_rate,
    }


def run_rerun_cmd(args: Any) -> int:  # noqa: C901
    history = RunHistory(path=args.history_file)
    entries = history.all()

    pipelines = (
        [args.pipeline]
        if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    results = [
        r
        for p in pipelines
        if (r := _pipeline_rerun(entries, p, args.hours)) is not None
    ]

    if not results:
        print("No rerun data found.")
        return 0

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Total':>7} {'Reruns':>7} {'Success':>8} {'Rate%':>7}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['pipeline']:<30} {r['total_runs']:>7} {r['rerun_count']:>7} "
            f"{r['rerun_success']:>8} {r['success_rate_pct']:>7}"
        )
    return 0


def register_rerun_subcommand(subparsers: Any) -> None:
    p = subparsers.add_parser("rerun", help="Show pipeline re-run statistics")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--hours", type=float, default=24.0, help="Lookback window in hours")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.set_defaults(func=run_rerun_cmd)
