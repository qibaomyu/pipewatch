"""noise_cmd: report pipelines generating the most alerts (noisy pipelines)."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from collections import Counter
from typing import List, Tuple

from pipewatch.history import RunHistory, HistoryEntry


def _collect_noise(
    entries: List[HistoryEntry],
    pipeline: str | None = None,
    hours: int = 24,
) -> List[Tuple[str, int]]:
    """Return (pipeline, alert_count) sorted descending by alert count."""
    import time

    cutoff = time.time() - hours * 3600
    counter: Counter = Counter()
    for e in entries:
        if e.timestamp < cutoff:
            continue
        if pipeline and e.pipeline != pipeline:
            continue
        counter[e.pipeline] += len(e.alerts)
    return counter.most_common()


def run_noise_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    entries = history.all()
    results = _collect_noise(entries, pipeline=getattr(args, "pipeline", None), hours=args.hours)

    if not results:
        print("No alert data found.")
        return 0

    if args.json:
        payload = [{"pipeline": p, "alert_count": c} for p, c in results]
        print(json.dumps(payload, indent=2))
        return 0

    print(f"{'Pipeline':<30} {'Alerts':>8}")
    print("-" * 40)
    for pipeline_name, count in results:
        print(f"{pipeline_name:<30} {count:>8}")
    return 0


def register_noise_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "noise", help="Show pipelines ranked by alert frequency"
    )
    p.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.set_defaults(func=run_noise_cmd)
