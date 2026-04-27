"""throughput_rate_cmd: report runs-per-hour for each pipeline over a window."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import Dict, List, Optional

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_throughput_rate(
    entries: List[HistoryEntry],
    pipeline: Optional[str],
    hours: int,
) -> List[Dict]:
    """Compute runs-per-hour for each pipeline within *hours* window."""
    import datetime

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=hours)
    filtered = [
        e for e in entries
        if e.timestamp >= cutoff and (pipeline is None or e.pipeline == pipeline)
    ]

    counts: Dict[str, int] = {}
    for e in filtered:
        counts[e.pipeline] = counts.get(e.pipeline, 0) + 1

    results = []
    for name, total in sorted(counts.items()):
        rate = round(total / hours, 4)
        results.append({"pipeline": name, "runs": total, "hours": hours, "rate": rate})
    return results


def run_throughput_rate_cmd(args: Namespace) -> int:
    history = RunHistory(path=args.history_file)
    entries = history.all()

    rows = _pipeline_throughput_rate(entries, args.pipeline, args.hours)

    if not rows:
        print("No data available.")
        return 0

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    header = f"{'Pipeline':<30} {'Runs':>6} {'Hours':>6} {'Rate/hr':>10}"
    print(header)
    print("-" * len(header))
    for row in rows:
        print(
            f"{row['pipeline']:<30} {row['runs']:>6} {row['hours']:>6} {row['rate']:>10.4f}"
        )
    return 0


def register_throughput_rate_subcommand(subparsers) -> None:
    parser: ArgumentParser = subparsers.add_parser(
        "throughput-rate",
        help="Show runs-per-hour rate for pipelines over a time window.",
    )
    parser.add_argument(
        "--hours", type=int, default=24, help="Window size in hours (default: 24)."
    )
    parser.add_argument(
        "--pipeline", default=None, help="Filter to a single pipeline."
    )
    parser.add_argument(
        "--json", action="store_true", help="Output as JSON."
    )
    parser.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file.",
    )
    parser.set_defaults(func=run_throughput_rate_cmd)
