"""throughput_floor_cmd: alert when pipeline throughput drops below a minimum floor."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from pipewatch.history import RunHistory


def _pipeline_throughput_floor(
    history: RunHistory,
    pipeline: Optional[str],
    hours: int,
    min_runs: int,
) -> list[dict]:
    """Return pipelines whose run count in the window is below *min_runs*."""
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    entries = [
        e for e in history.all()
        if datetime.fromisoformat(e.timestamp) >= cutoff
        and (pipeline is None or e.pipeline == pipeline)
    ]

    by_pipeline: dict[str, int] = {}
    for entry in entries:
        by_pipeline[entry.pipeline] = by_pipeline.get(entry.pipeline, 0) + 1

    results = []
    for name, count in sorted(by_pipeline.items()):
        results.append({
            "pipeline": name,
            "runs": count,
            "min_runs": min_runs,
            "below_floor": count < min_runs,
        })
    return results


def run_throughput_floor_cmd(args) -> int:  # noqa: ANN001
    history = RunHistory(path=args.history_file)
    rows = _pipeline_throughput_floor(
        history,
        pipeline=getattr(args, "pipeline", None),
        hours=args.hours,
        min_runs=args.min_runs,
    )

    if not rows:
        print("No pipeline data found.")
        return 0

    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        print(f"{'Pipeline':<30} {'Runs':>6} {'Min':>6} {'Status':<10}")
        print("-" * 56)
        for row in rows:
            status = "BELOW FLOOR" if row["below_floor"] else "OK"
            print(f"{row['pipeline']:<30} {row['runs']:>6} {row['min_runs']:>6} {status:<10}")

    if args.exit_code:
        return 1 if any(r["below_floor"] for r in rows) else 0
    return 0


def register_throughput_floor_subcommand(subparsers) -> None:  # noqa: ANN001
    parser = subparsers.add_parser(
        "throughput-floor",
        help="Alert when pipeline run count drops below a minimum floor.",
    )
    parser.add_argument("--history-file", default=".pipewatch_history.json")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--min-runs", type=int, default=1, dest="min_runs")
    parser.add_argument("--pipeline", default=None)
    parser.add_argument("--json", action="store_true", default=False)
    parser.add_argument("--exit-code", action="store_true", default=False, dest="exit_code")
    parser.set_defaults(func=run_throughput_floor_cmd)
