"""Baseline command: save or show a performance baseline for pipelines."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

DEFAULT_BASELINE_FILE = ".pipewatch_baseline.json"


def _load_baselines(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def _save_baselines(path: str, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def set_baseline(pipeline: str, error_rate: float, latency: float, path: str) -> dict:
    baselines = _load_baselines(path)
    baselines[pipeline] = {
        "error_rate": error_rate,
        "latency": latency,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_baselines(path, baselines)
    return baselines[pipeline]


def get_baseline(pipeline: str, path: str) -> dict | None:
    return _load_baselines(path).get(pipeline)


def _format_text(pipeline: str, entry: dict) -> str:
    return (
        f"  pipeline : {pipeline}\n"
        f"  error_rate: {entry['error_rate']:.2%}\n"
        f"  latency  : {entry['latency']:.2f}s\n"
        f"  recorded : {entry['recorded_at']}"
    )


def run_baseline_cmd(args: Any) -> int:
    path = getattr(args, "baseline_file", DEFAULT_BASELINE_FILE)

    if args.baseline_action == "set":
        entry = set_baseline(args.pipeline, args.error_rate, args.latency, path)
        print(f"Baseline saved for '{args.pipeline}'.")
        print(_format_text(args.pipeline, entry))
        return 0

    # show
    baselines = _load_baselines(path)
    pipeline_filter = getattr(args, "pipeline", None)
    items = (
        {pipeline_filter: baselines[pipeline_filter]}
        if pipeline_filter and pipeline_filter in baselines
        else baselines
    )
    if not items:
        print("No baselines recorded.")
        return 0
    for name, entry in items.items():
        print(f"[{name}]")
        print(_format_text(name, entry))
    return 0
