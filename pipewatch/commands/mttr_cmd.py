"""Mean Time To Recovery (MTTR) command for pipewatch."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from pipewatch.history import RunHistory, HistoryEntry


def _pipeline_mttr(entries: list[HistoryEntry]) -> Optional[float]:
    """Return mean recovery time in seconds, or None if no recoveries found."""
    recovery_times: list[float] = []
    prev: Optional[HistoryEntry] = None

    for entry in sorted(entries, key=lambda e: e.timestamp):
        if prev is not None and not prev.healthy and entry.healthy:
            delta = (entry.timestamp - prev.timestamp).total_seconds()
            recovery_times.append(delta)
        prev = entry

    if not recovery_times:
        return None
    return sum(recovery_times) / len(recovery_times)


def _format_text(results: dict[str, Optional[float]]) -> str:
    lines = []
    for pipeline, mttr in results.items():
        if mttr is None:
            lines.append(f"  {pipeline}: no recovery data")
        else:
            lines.append(f"  {pipeline}: {mttr:.1f}s avg recovery")
    return "\n".join(lines) if lines else "  No data available."


def run_mttr_cmd(args) -> int:
    history = RunHistory(args.history_file)
    all_entries = history.all()

    pipelines = (
        [args.pipeline] if getattr(args, "pipeline", None)
        else sorted({e.pipeline for e in all_entries})
    )

    results: dict[str, Optional[float]] = {}
    for name in pipelines:
        entries = [e for e in all_entries if e.pipeline == name]
        results[name] = _pipeline_mttr(entries)

    if getattr(args, "json", False):
        payload = {
            k: round(v, 3) if v is not None else None
            for k, v in results.items()
        }
        print(json.dumps(payload, indent=2))
    else:
        print("Mean Time To Recovery:")
        print(_format_text(results))

    return 0


def register_mttr_subcommand(subparsers) -> None:
    p = subparsers.add_parser("mttr", help="Show mean time to recovery per pipeline")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file",
    )
    p.set_defaults(func=run_mttr_cmd)
