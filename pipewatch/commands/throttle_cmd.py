"""throttle_cmd: detect pipelines exceeding a max run frequency."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from pipewatch.history import RunHistory


def _pipeline_throttle(
    history: RunHistory,
    pipeline: str,
    hours: float,
    max_runs: int,
) -> Optional[dict]:
    """Return throttle info for a single pipeline or None if no entries."""
    cutoff = datetime.now(tz=timezone.utc).timestamp() - hours * 3600
    entries = [
        e for e in history.get(pipeline)
        if e.timestamp >= cutoff
    ]
    if not entries:
        return None
    run_count = len(entries)
    exceeded = run_count > max_runs
    return {
        "pipeline": pipeline,
        "run_count": run_count,
        "max_runs": max_runs,
        "hours": hours,
        "exceeded": exceeded,
    }


def run_throttle_cmd(args) -> int:
    history = RunHistory(path=args.history_file)
    pipelines = (
        [args.pipeline] if args.pipeline
        else history.pipelines()
    )

    results = []
    for name in pipelines:
        info = _pipeline_throttle(history, name, args.hours, args.max_runs)
        if info is not None:
            results.append(info)

    if not results:
        print("No pipeline run data found.")
        return 0

    if args.json:
        print(json.dumps(results, indent=2))
        return 1 if args.exit_code and any(r["exceeded"] for r in results) else 0

    for r in results:
        flag = "  [THROTTLED]" if r["exceeded"] else ""
        print(
            f"{r['pipeline']}: {r['run_count']} runs in last {r['hours']}h "
            f"(max {r['max_runs']}){flag}"
        )

    if args.exit_code and any(r["exceeded"] for r in results):
        return 1
    return 0


def register_throttle_subcommand(subparsers) -> None:
    p = subparsers.add_parser(
        "throttle",
        help="Detect pipelines exceeding a maximum run frequency.",
    )
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline.")
    p.add_argument("--hours", type=float, default=1.0, help="Lookback window in hours.")
    p.add_argument("--max-runs", type=int, default=10, dest="max_runs",
                   help="Maximum allowed runs within the window.")
    p.add_argument("--json", action="store_true", help="Output as JSON.")
    p.add_argument("--exit-code", action="store_true", dest="exit_code",
                   help="Return exit code 1 if any pipeline is throttled.")
    p.add_argument("--history-file", default=".pipewatch_history.json",
                   dest="history_file")
    p.set_defaults(func=run_throttle_cmd)
