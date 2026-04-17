"""CLI subcommand: pipewatch history — show past run history."""

from __future__ import annotations

from typing import List, Optional

from pipewatch.history import RunHistory, HistoryEntry


STATUS_OK = "\u2713"
STATUS_FAIL = "\u2717"


def _row(entry: HistoryEntry) -> str:
    symbol = STATUS_OK if entry.healthy else STATUS_FAIL
    return (
        f"  [{symbol}] {entry.timestamp}  "
        f"pipeline={entry.pipeline}  "
        f"error_rate={entry.error_rate:.2%}  "
        f"latency={entry.latency:.2f}s  "
        f"alerts={entry.alert_count}"
    )


def run_history_cmd(
    history_path: str,
    pipeline: Optional[str] = None,
    limit: int = 20,
    clear: bool = False,
) -> int:
    """Display or clear run history. Returns exit code."""
    history = RunHistory(path=history_path)

    if clear:
        history.clear(pipeline)
        label = pipeline if pipeline else "all pipelines"
        print(f"Cleared history for {label}.")
        return 0

    entries = history.get(pipeline)
    if not entries:
        label = pipeline if pipeline else "any pipeline"
        print(f"No history found for {label}.")
        return 0

    entries = entries[-limit:]
    header = f"Run history ({len(entries)} entries)"
    if pipeline:
        header += f" for pipeline '{pipeline}'"
    print(header)
    print("-" * 60)
    for entry in entries:
        print(_row(entry))
    return 0
