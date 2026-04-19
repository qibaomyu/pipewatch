"""correlation_cmd: find pipelines whose error rates move together."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import Dict, List, Tuple

from pipewatch.history import RunHistory, HistoryEntry


def _error_series(entries: List[HistoryEntry]) -> List[float]:
    return [e.error_rate for e in sorted(entries, key=lambda e: e.timestamp)]


def _pearson(a: List[float], b: List[float]) -> float:
    n = min(len(a), len(b))
    if n < 2:
        return 0.0
    a, b = a[:n], b[:n]
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    num = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b))
    den_a = sum((x - mean_a) ** 2 for x in a) ** 0.5
    den_b = sum((y - mean_b) ** 2 for y in b) ** 0.5
    if den_a == 0 or den_b == 0:
        return 0.0
    return num / (den_a * den_b)


def _pipeline_correlations(
    history: RunHistory, min_entries: int = 3
) -> List[Tuple[str, str, float]]:
    by_pipeline: Dict[str, List[HistoryEntry]] = {}
    for entry in history.all():
        by_pipeline.setdefault(entry.pipeline, []).append(entry)

    pipelines = [p for p, es in by_pipeline.items() if len(es) >= min_entries]
    results = []
    for i, p1 in enumerate(pipelines):
        for p2 in pipelines[i + 1 :]:
            r = _pearson(_error_series(by_pipeline[p1]), _error_series(by_pipeline[p2]))
            results.append((p1, p2, round(r, 4)))
    results.sort(key=lambda t: abs(t[2]), reverse=True)
    return results


def run_correlation_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    pairs = _pipeline_correlations(history, min_entries=args.min_entries)

    if not pairs:
        print("No correlation data available (insufficient history).")
        return 0

    if args.json:
        print(json.dumps([{"pipeline_a": a, "pipeline_b": b, "r": r} for a, b, r in pairs], indent=2))
        return 0

    print(f"{'Pipeline A':<25} {'Pipeline B':<25} {'r':>8}")
    print("-" * 62)
    for a, b, r in pairs:
        print(f"{a:<25} {b:<25} {r:>8.4f}")
    return 0


def register_correlation_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "correlation", help="Show error-rate correlations between pipelines"
    )
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.add_argument("--min-entries", type=int, default=3, help="Minimum runs required per pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.set_defaults(func=run_correlation_cmd)
