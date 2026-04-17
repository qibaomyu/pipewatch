"""Output formatters for pipeline run results."""
from __future__ import annotations

import json
from typing import List

from pipewatch.runner import RunResult
from pipewatch.alerts import AlertLevel


DEFAULT_FORMAT = "text"


def _level_symbol(level: AlertLevel) -> str:
    return {AlertLevel.INFO: "ℹ", AlertLevel.WARNING: "⚠", AlertLevel.CRITICAL: "✖"}.get(level, "?")


def format_text(results: List[RunResult]) -> str:
    lines = []
    for r in results:
        status = "OK" if r.healthy else "FAIL"
        lines.append(f"[{status}] {r.pipeline_name}")
        for alert in r.alerts:
            symbol = _level_symbol(alert.level)
            lines.append(f"  {symbol} {alert}")
    if not lines:
        lines.append("No pipelines evaluated.")
    return "\n".join(lines)


def format_json(results: List[RunResult]) -> str:
    data = [
        {
            "pipeline": r.pipeline_name,
            "healthy": r.healthy,
            "alerts": [
                {"level": a.level.value, "message": a.message}
                for a in r.alerts
            ],
        }
        for r in results
    ]
    return json.dumps(data, indent=2)


def format_results(results: List[RunResult], fmt: str = DEFAULT_FORMAT) -> str:
    if fmt == "json":
        return format_json(results)
    return format_text(results)
