"""Heatmap command: show error-rate intensity per pipeline per hour bucket."""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from pipewatch.history import RunHistory, HistoryEntry

_SYMBOLS = [" ", "░", "▒", "▓", "█"]


def _symbol(rate: float) -> str:
    if rate == 0:
        return _SYMBOLS[0]
    elif rate < 0.1:
        return _SYMBOLS[1]
    elif rate < 0.3:
        return _SYMBOLS[2]
    elif rate < 0.6:
        return _SYMBOLS[3]
    return _SYMBOLS[4]


def _build_heatmap(
    entries: List[HistoryEntry],
    hours: int,
    pipeline: Optional[str],
) -> dict:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    buckets: dict = defaultdict(lambda: defaultdict(list))

    for e in entries:
        ts = datetime.fromisoformat(e.timestamp)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts < cutoff:
            continue
        if pipeline and e.pipeline != pipeline:
            continue
        hour_label = ts.strftime("%Y-%m-%dT%H")
        buckets[e.pipeline][hour_label].append(e.error_rate)

    result = {}
    for pipe, hours_data in buckets.items():
        result[pipe] = {
            h: sum(rates) / len(rates) for h, rates in sorted(hours_data.items())
        }
    return result


def run_heatmap_cmd(args) -> int:
    history = RunHistory(path=args.history_file)
    entries = history.all()
    heatmap = _build_heatmap(entries, args.hours, getattr(args, "pipeline", None))

    if not heatmap:
        print("No data available.")
        return 0

    if args.json:
        print(json.dumps(heatmap, indent=2))
        return 0

    for pipe, hours_data in sorted(heatmap.items()):
        print(f"\nPipeline: {pipe}")
        print("  " + "".join(f"{h[-2:]}" for h in hours_data))
        print("  " + "".join(_symbol(r) for r in hours_data.values()))
    return 0


def register_heatmap_subcommand(subparsers):
    p = subparsers.add_parser("heatmap", help="Show error-rate heatmap by hour")
    p.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.set_defaults(func=run_heatmap_cmd)
