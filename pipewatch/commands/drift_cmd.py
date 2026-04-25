"""drift_cmd: detect configuration drift between current thresholds and baseline."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import Any

from pipewatch.commands.baseline_cmd import get_baseline
from pipewatch.config import AppConfig, load_config


def _pipeline_drift(pipeline_name: str, cfg: AppConfig, baseline_file: str) -> dict[str, Any]:
    """Compare current pipeline thresholds against the stored baseline."""
    pipeline = next((p for p in cfg.pipelines if p.name == pipeline_name), None)
    if pipeline is None:
        return {"pipeline": pipeline_name, "error": "not found in config"}

    baseline = get_baseline(pipeline_name, baseline_file)
    if baseline is None:
        return {"pipeline": pipeline_name, "error": "no baseline recorded"}

    fields = ["max_error_rate", "max_latency_seconds", "min_throughput"]
    drifts: list[dict[str, Any]] = []
    for field in fields:
        current = getattr(pipeline, field, None)
        stored = baseline.get(field)
        if stored is not None and current != stored:
            drifts.append({"field": field, "baseline": stored, "current": current})

    return {"pipeline": pipeline_name, "drifts": drifts}


def _format_text(results: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for r in results:
        if "error" in r:
            lines.append(f"  {r['pipeline']}: {r['error']}")
            continue
        drifts = r.get("drifts", [])
        if not drifts:
            lines.append(f"  {r['pipeline']}: no drift")
        else:
            lines.append(f"  {r['pipeline']}:")
            for d in drifts:
                lines.append(f"    {d['field']}: baseline={d['baseline']}  current={d['current']}")
    return "\n".join(lines)


def run_drift_cmd(args: Namespace) -> int:
    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"Config file not found: {args.config}")
        return 2

    pipelines = (
        [args.pipeline]
        if args.pipeline
        else [p.name for p in cfg.pipelines]
    )

    results = [_pipeline_drift(p, cfg, args.baseline_file) for p in pipelines]

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print(_format_text(results))

    has_drift = any(r.get("drifts") for r in results)
    return 1 if (args.exit_code and has_drift) else 0


def register_drift_subcommand(subparsers: Any) -> None:
    p: ArgumentParser = subparsers.add_parser("drift", help="Detect threshold configuration drift")
    p.add_argument("--config", default="pipewatch.yaml")
    p.add_argument("--baseline-file", default=".pipewatch_baselines.json")
    p.add_argument("--pipeline", default=None)
    p.add_argument("--json", action="store_true")
    p.add_argument("--exit-code", action="store_true")
    p.set_defaults(func=run_drift_cmd)
