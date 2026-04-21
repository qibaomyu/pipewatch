"""pause_cmd: pause and resume pipeline monitoring."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Optional

DEFAULT_PAUSE_FILE = ".pipewatch_pauses.json"


def _load_pauses(pause_file: str) -> dict:
    if not os.path.exists(pause_file):
        return {}
    with open(pause_file) as fh:
        return json.load(fh)


def _save_pauses(data: dict, pause_file: str) -> None:
    with open(pause_file, "w") as fh:
        json.dump(data, fh, indent=2)


def pause_pipeline(name: str, reason: str = "", pause_file: str = DEFAULT_PAUSE_FILE) -> None:
    """Mark a pipeline as paused with an optional reason."""
    data = _load_pauses(pause_file)
    data[name] = {
        "paused_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
    }
    _save_pauses(data, pause_file)


def resume_pipeline(name: str, pause_file: str = DEFAULT_PAUSE_FILE) -> bool:
    """Remove a pipeline from the paused set. Returns True if it was paused."""
    data = _load_pauses(pause_file)
    if name not in data:
        return False
    del data[name]
    _save_pauses(data, pause_file)
    return True


def is_paused(name: str, pause_file: str = DEFAULT_PAUSE_FILE) -> bool:
    """Return True if the pipeline is currently paused."""
    data = _load_pauses(pause_file)
    return name in data


def get_pause_info(name: str, pause_file: str = DEFAULT_PAUSE_FILE) -> Optional[dict]:
    """Return pause metadata for a pipeline, or None if not paused."""
    data = _load_pauses(pause_file)
    return data.get(name)


def run_pause_cmd(args) -> int:
    pause_file = getattr(args, "pause_file", DEFAULT_PAUSE_FILE)

    if args.pause_action == "pause":
        pause_pipeline(args.pipeline, reason=getattr(args, "reason", ""), pause_file=pause_file)
        print(f"Pipeline '{args.pipeline}' paused.")
        return 0

    if args.pause_action == "resume":
        removed = resume_pipeline(args.pipeline, pause_file=pause_file)
        if removed:
            print(f"Pipeline '{args.pipeline}' resumed.")
        else:
            print(f"Pipeline '{args.pipeline}' was not paused.")
        return 0

    if args.pause_action == "list":
        data = _load_pauses(pause_file)
        if not data:
            print("No pipelines are currently paused.")
            return 0
        for name, meta in sorted(data.items()):
            reason = meta.get("reason") or "(no reason)"
            print(f"  {name}  paused_at={meta['paused_at']}  reason={reason}")
        return 0

    print(f"Unknown pause action: {args.pause_action}")
    return 2


def register_pause_subcommand(subparsers) -> None:
    parser = subparsers.add_parser("pause", help="Pause or resume pipeline monitoring")
    parser.add_argument("pause_action", choices=["pause", "resume", "list"])
    parser.add_argument("pipeline", nargs="?", default="", help="Pipeline name")
    parser.add_argument("--reason", default="", help="Reason for pausing")
    parser.add_argument("--pause-file", default=DEFAULT_PAUSE_FILE, dest="pause_file")
    parser.set_defaults(func=run_pause_cmd)
