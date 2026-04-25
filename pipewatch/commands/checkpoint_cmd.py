"""checkpoint_cmd: record and retrieve named checkpoints for pipeline runs."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Optional

DEFAULT_CHECKPOINT_FILE = ".pipewatch_checkpoints.json"


def _load_checkpoints(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path) as fh:
        return json.load(fh)


def _save_checkpoints(data: dict, path: str) -> None:
    with open(path, "w") as fh:
        json.dump(data, fh, indent=2)


def set_checkpoint(pipeline: str, label: str, path: str = DEFAULT_CHECKPOINT_FILE) -> dict:
    """Record a named checkpoint for *pipeline* with the current UTC timestamp."""
    data = _load_checkpoints(path)
    entry = {
        "pipeline": pipeline,
        "label": label,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    data.setdefault(pipeline, {})[label] = entry
    _save_checkpoints(data, path)
    return entry


def get_checkpoint(pipeline: str, label: str, path: str = DEFAULT_CHECKPOINT_FILE) -> Optional[dict]:
    """Return the checkpoint entry or *None* if it does not exist."""
    data = _load_checkpoints(path)
    return data.get(pipeline, {}).get(label)


def list_checkpoints(pipeline: Optional[str], path: str = DEFAULT_CHECKPOINT_FILE) -> list[dict]:
    """Return all checkpoints, optionally filtered to *pipeline*."""
    data = _load_checkpoints(path)
    results: list[dict] = []
    for pipe_name, labels in data.items():
        if pipeline and pipe_name != pipeline:
            continue
        results.extend(labels.values())
    results.sort(key=lambda e: e["timestamp"])
    return results


def run_checkpoint_cmd(args) -> int:
    path = getattr(args, "checkpoint_file", DEFAULT_CHECKPOINT_FILE)
    if args.checkpoint_action == "set":
        entry = set_checkpoint(args.pipeline, args.label, path)
        print(f"Checkpoint set: [{entry['pipeline']}] {entry['label']} @ {entry['timestamp']}")
        return 0
    if args.checkpoint_action == "get":
        entry = get_checkpoint(args.pipeline, args.label, path)
        if entry is None:
            print(f"No checkpoint '{args.label}' for pipeline '{args.pipeline}'.")
            return 1
        print(f"[{entry['pipeline']}] {entry['label']} @ {entry['timestamp']}")
        return 0
    # list
    entries = list_checkpoints(getattr(args, "pipeline", None), path)
    if not entries:
        print("No checkpoints recorded.")
        return 0
    for e in entries:
        print(f"  {e['pipeline']:<20} {e['label']:<20} {e['timestamp']}")
    return 0


def register_checkpoint_subcommand(subparsers) -> None:
    p = subparsers.add_parser("checkpoint", help="Record or retrieve named pipeline checkpoints")
    p.add_argument("checkpoint_action", choices=["set", "get", "list"], help="Action to perform")
    p.add_argument("--pipeline", default=None, help="Pipeline name")
    p.add_argument("--label", default=None, help="Checkpoint label")
    p.add_argument("--checkpoint-file", default=DEFAULT_CHECKPOINT_FILE, dest="checkpoint_file")
    p.set_defaults(func=run_checkpoint_cmd)
