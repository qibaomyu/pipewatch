"""Snapshot command: capture and display current pipeline status summary."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import List

from pipewatch.config import load_config
from pipewatch.monitor import PipelineMonitor
from pipewatch.runner import PipelineRunner, RunResult


def _collect_results(config_path: str, pipelines: List[str] | None) -> List[RunResult]:
    app_cfg = load_config(config_path)
    targets = (
        [p for p in app_cfg.pipelines if p.name in pipelines]
        if pipelines
        else app_cfg.pipelines
    )
    results = []
    for pipeline_cfg in targets:
        monitor = PipelineMonitor(pipeline_cfg)
        runner = PipelineRunner(pipeline_cfg, monitor)
        results.append(runner.run())
    return results


def run_snapshot_cmd(
    config_path: str,
    pipelines: List[str] | None = None,
    output_format: str = "text",
    out=None,
) -> int:
    import sys

    out = out or sys.stdout

    try:
        results = _collect_results(config_path, pipelines)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=out)
        return 2

    timestamp = datetime.now(timezone.utc).isoformat()

    if output_format == "json":
        payload = {
            "snapshot_at": timestamp,
            "pipelines": [
                {
                    "pipeline": r.pipeline_name,
                    "healthy": r.status.healthy,
                    "alerts": [str(a) for a in r.alerts],
                }
                for r in results
            ],
        }
        print(json.dumps(payload, indent=2), file=out)
    else:
        print(f"Snapshot @ {timestamp}", file=out)
        print("-" * 40, file=out)
        for r in results:
            symbol = "OK" if r.status.healthy else "FAIL"
            alert_str = ", ".join(str(a) for a in r.alerts) if r.alerts else "none"
            print(f"[{symbol}] {r.pipeline_name}  alerts={alert_str}", file=out)

    return 0
