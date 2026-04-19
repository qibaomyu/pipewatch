"""forecast_cmd: predict future error rate based on recent trend."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import List

from pipewatch.history import RunHistory, HistoryEntry


def _linear_forecast(values: List[float], steps: int = 1) -> float:
    """Simple least-squares linear extrapolation."""
    n = len(values)
    if n == 0:
        return 0.0
    if n == 1:
        return values[0]
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(values) / n
    num = sum((xs[i] - x_mean) * (values[i] - y_mean) for i in range(n))
    den = sum((xs[i] - x_mean) ** 2 for i in range(n))
    slope = num / den if den else 0.0
    return y_mean + slope * (n - 1 + steps)


def _pipeline_forecast(entries: List[HistoryEntry], steps: int) -> dict:
    error_rates = [e.error_rate for e in entries]
    latencies = [e.latency_p99 for e in entries]
    return {
        "samples": len(entries),
        "forecast_error_rate": round(_linear_forecast(error_rates, steps), 4),
        "forecast_latency_p99": round(_linear_forecast(latencies, steps), 4),
    }


def run_forecast_cmd(args: Namespace) -> int:
    history = RunHistory(args.history_file)
    entries = history.get_all()
    if args.pipeline:
        entries = [e for e in entries if e.pipeline == args.pipeline]

    pipelines = sorted({e.pipeline for e in entries}) if not args.pipeline else [args.pipeline]

    if not pipelines:
        print("No history available.")
        return 0

    results = []
    for name in pipelines:
        pipe_entries = [e for e in entries if e.pipeline == name]
        pipe_entries.sort(key=lambda e: e.timestamp)
        row = {"pipeline": name, **_pipeline_forecast(pipe_entries, args.steps)}
        results.append(row)

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print(f"{'Pipeline':<25} {'Samples':>8} {'Err Rate (forecast)':>20} {'Latency p99 (forecast)':>23}")
        print("-" * 80)
        for r in results:
            print(f"{r['pipeline']:<25} {r['samples']:>8} {r['forecast_error_rate']:>20.4f} {r['forecast_latency_p99']:>23.4f}")
    return 0


def register_forecast_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser("forecast", help="Forecast future error rate and latency")
    p.add_argument("--history-file", default=".pipewatch_history.json")
    p.add_argument("--pipeline", default=None)
    p.add_argument("--steps", type=int, default=1, help="Periods ahead to forecast")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=run_forecast_cmd)
