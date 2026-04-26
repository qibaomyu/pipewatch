"""congestion_cmd.py — Detect pipeline congestion by analysing queue depth trends.

A pipeline is considered *congested* when its average latency in the most
recent window exceeds a user-supplied multiplier times the baseline average
latency computed from an earlier reference window.
"""

from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.history import HistoryEntry, RunHistory


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def _window_avg_latency(
    entries: List[HistoryEntry],
    pipeline: str,
    start: datetime,
    end: datetime,
) -> Optional[float]:
    """Return the mean latency (seconds) for *pipeline* in [start, end)."""
    samples = [
        e.latency_seconds
        for e in entries
        if e.pipeline == pipeline
        and start <= e.timestamp < end
        and e.latency_seconds is not None
    ]
    if not samples:
        return None
    return sum(samples) / len(samples)


def _pipeline_congestion(
    entries: List[HistoryEntry],
    pipeline: str,
    recent_hours: int,
    baseline_hours: int,
    multiplier: float,
    now: datetime,
) -> Dict:
    """Analyse congestion for a single pipeline.

    Returns a dict with keys: pipeline, baseline_avg, recent_avg,
    ratio, congested, sample_count.
    """
    from datetime import timedelta

    recent_start = now - timedelta(hours=recent_hours)
    baseline_start = now - timedelta(hours=baseline_hours)
    # Baseline window is the older slice that does NOT overlap the recent window
    baseline_end = recent_start

    baseline_avg = _window_avg_latency(entries, pipeline, baseline_start, baseline_end)
    recent_avg = _window_avg_latency(entries, pipeline, recent_start, now)

    recent_count = sum(
        1 for e in entries
        if e.pipeline == pipeline and recent_start <= e.timestamp < now
    )

    ratio: Optional[float] = None
    congested = False
    if baseline_avg and recent_avg:
        ratio = recent_avg / baseline_avg
        congested = ratio >= multiplier

    return {
        "pipeline": pipeline,
        "baseline_avg": round(baseline_avg, 3) if baseline_avg is not None else None,
        "recent_avg": round(recent_avg, 3) if recent_avg is not None else None,
        "ratio": round(ratio, 3) if ratio is not None else None,
        "congested": congested,
        "sample_count": recent_count,
    }


# ---------------------------------------------------------------------------
# Command entry-point
# ---------------------------------------------------------------------------

def run_congestion_cmd(args: Namespace) -> int:
    """Execute the *congestion* sub-command."""
    history = RunHistory(path=args.history_file)
    entries = history.all()
    now = datetime.now(tz=timezone.utc)

    pipelines: List[str] = (
        [args.pipeline] if args.pipeline
        else sorted({e.pipeline for e in entries})
    )

    if not pipelines:
        print("No history entries found.")
        return 0

    results = [
        _pipeline_congestion(
            entries,
            p,
            recent_hours=args.recent_hours,
            baseline_hours=args.baseline_hours,
            multiplier=args.multiplier,
            now=now,
        )
        for p in pipelines
    ]

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    # Text output
    header = f"{'PIPELINE':<30} {'BASELINE':>10} {'RECENT':>10} {'RATIO':>7}  STATUS"
    print(header)
    print("-" * len(header))
    any_congested = False
    for r in results:
        status = "CONGESTED" if r["congested"] else "ok"
        if r["congested"]:
            any_congested = True
        baseline_str = f"{r['baseline_avg']:.3f}s" if r["baseline_avg"] is not None else "n/a"
        recent_str = f"{r['recent_avg']:.3f}s" if r["recent_avg"] is not None else "n/a"
        ratio_str = f"{r['ratio']:.2f}x" if r["ratio"] is not None else "n/a"
        print(f"{r['pipeline']:<30} {baseline_str:>10} {recent_str:>10} {ratio_str:>7}  {status}")

    if args.exit_code and any_congested:
        return 1
    return 0


# ---------------------------------------------------------------------------
# Sub-command registration
# ---------------------------------------------------------------------------

def register_congestion_subcommand(subparsers) -> None:  # type: ignore[type-arg]
    """Attach the *congestion* sub-command to *subparsers*."""
    parser: ArgumentParser = subparsers.add_parser(
        "congestion",
        help="Detect pipelines whose recent latency significantly exceeds their baseline.",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Limit analysis to a single pipeline name.",
    )
    parser.add_argument(
        "--recent-hours",
        type=int,
        default=1,
        dest="recent_hours",
        help="Size of the recent observation window in hours (default: 1).",
    )
    parser.add_argument(
        "--baseline-hours",
        type=int,
        default=24,
        dest="baseline_hours",
        help="How many hours of history to use as the baseline window (default: 24).",
    )
    parser.add_argument(
        "--multiplier",
        type=float,
        default=2.0,
        help="Ratio threshold above which a pipeline is flagged as congested (default: 2.0).",
    )
    parser.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        dest="history_file",
        help="Path to the run-history file.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Emit results as JSON.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        dest="exit_code",
        help="Return exit code 1 when at least one pipeline is congested.",
    )
    parser.set_defaults(func=run_congestion_cmd)
