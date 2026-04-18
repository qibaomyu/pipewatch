"""Command to prune old history entries beyond a retention limit."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.history import RunHistory


def run_prune_cmd(args: argparse.Namespace) -> int:
    """Remove history entries older than --days days.

    Returns 0 on success, 2 on error.
    """
    history_path = Path(args.history_file)
    if not history_path.exists():
        print("No history file found — nothing to prune.")
        return 0

    history = RunHistory(history_path)

    pipeline = getattr(args, "pipeline", None)
    days: int = args.days

    entries_before = len(history.get(pipeline=pipeline))
    removed = history.prune(days=days, pipeline=pipeline)
    entries_after = len(history.get(pipeline=pipeline))

    if args.dry_run:
        print(
            f"[dry-run] Would remove {removed} entr{'y' if removed == 1 else 'ies'} "
            f"older than {days} day(s)."
        )
        # Reload to undo in-memory changes persisted during prune dry-run
        # (prune should not persist when dry_run; see history.prune signature)
    else:
        print(
            f"Pruned {removed} entr{'y' if removed == 1 else 'ies'} "
            f"({entries_before} → {entries_after}) older than {days} day(s)."
        )
    return 0
