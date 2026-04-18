"""health_cmd: show current health status for all or selected pipelines."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import List

from pipewatch.config import load_config
from pipewatch.history import RunHistory
from pipewatch.monitor import evaluate_pipeline, PipelineStatus


def _pipeline_health(pipeline_name: str, history: RunHistory, window: int) -> dict:
    entries = [
        e for e in history.get(pipeline_name)
        if e is not None
    ]
    if not entries:
        return {"pipeline": pipeline_name, "status": "unknown", "entries": 0}

    recent = entries[-window:]
    failed = sum(1 for e in recent if not e.healthy)
    total = len(recent)
    error_rate = failed / total if total else 0.0
    avg_latency = sum(e.latency for e in recent) / total if total else 0.0

    return {
        "pipeline": pipeline_name,
        "status": "healthy" if failed == 0 else "failing",
        "error_rate": round(error_rate, 4),
        "avg_latency": round(avg_latency, 4),
        "entries": total,
    }


def _format_text(rows: List[dict]) -> str:
    lines = []
    for r in rows:
        if r["status"] == "unknown":
            lines.append(f"  [{r['pipeline']}]  status=unknown  (no history)")
        else:
            symbol = "✓" if r["status"] == "healthy" else "✗"
            lines.append(
                f"  {symbol} [{r['pipeline']}]  status={r['status']}  "
                f"error_rate={r['error_rate']:.2%}  avg_latency={r['avg_latency']:.3f}s  "
                f"samples={r['entries']}"
            )
    return "\n".join(lines)


def run_health_cmd(args: Namespace) -> int:
    cfg = load_config(args.config)
    history = RunHistory(args.history_file)

    pipelines = (
        [args.pipeline]
        if getattr(args, "pipeline", None)
        else [p.name for p in cfg.pipelines]
    )

    rows = [_pipeline_health(p, history, args.window) for p in pipelines]

    if args.format == "json":
        print(json.dumps(rows, indent=2))
    else:
        print("Pipeline Health")
        print(_format_text(rows))

    any_failing = any(r["status"] == "failing" for r in rows)
    return 1 if (args.exit_code and any_failing) else 0
