"""Command to display effective thresholds for configured pipelines."""
from __future__ import annotations

import json
from argparse import Namespace
from typing import List

from pipewatch.config import AppConfig, PipelineConfig, load_config


def _pipeline_thresholds(pipeline: PipelineConfig) -> dict:
    return {
        "pipeline": pipeline.name,
        "max_error_rate": pipeline.max_error_rate,
        "max_latency_seconds": pipeline.max_latency_seconds,
        "min_throughput": pipeline.min_throughput,
    }


def _format_text(rows: List[dict]) -> str:
    if not rows:
        return "No pipelines configured."
    lines = []
    header = f"{'PIPELINE':<30} {'MAX_ERROR_RATE':>15} {'MAX_LATENCY_S':>14} {'MIN_THROUGHPUT':>15}"
    lines.append(header)
    lines.append("-" * len(header))
    for r in rows:
        lines.append(
            f"{r['pipeline']:<30} {str(r['max_error_rate']):>15} "
            f"{str(r['max_latency_seconds']):>14} {str(r['min_throughput']):>15}"
        )
    return "\n".join(lines)


def run_threshold_cmd(args: Namespace) -> int:
    try:
        cfg: AppConfig = load_config(args.config)
    except FileNotFoundError:
        print(f"Config file not found: {args.config}")
        return 2

    pipelines = cfg.pipelines
    if getattr(args, "pipeline", None):
        pipelines = [p for p in pipelines if p.name == args.pipeline]
        if not pipelines:
            print(f"Unknown pipeline: {args.pipeline}")
            return 2

    rows = [_pipeline_thresholds(p) for p in pipelines]

    if getattr(args, "json", False):
        print(json.dumps(rows, indent=2))
    else:
        print(_format_text(rows))

    return 0
