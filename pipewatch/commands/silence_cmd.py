"""Command to silence alerts for a pipeline for a given duration."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

SILENCE_FILE = Path(".pipewatch_silences.json")


def _load_silences(path: Path) -> dict:
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def _save_silences(silences: dict, path: Path) -> None:
    with open(path, "w") as f:
        json.dump(silences, f, indent=2)


def silence_pipeline(pipeline: str, minutes: int, path: Path = SILENCE_FILE) -> datetime:
    """Silence alerts for *pipeline* for *minutes* minutes. Returns expiry time."""
    silences = _load_silences(path)
    expiry = datetime.utcnow() + timedelta(minutes=minutes)
    silences[pipeline] = expiry.isoformat()
    _save_silences(silences, path)
    return expiry


def is_silenced(pipeline: str, path: Path = SILENCE_FILE) -> bool:
    """Return True if *pipeline* is currently silenced."""
    silences = _load_silences(path)
    if pipeline not in silences:
        return False
    expiry = datetime.fromisoformat(silences[pipeline])
    if datetime.utcnow() < expiry:
        return True
    # expired — clean up
    del silences[pipeline]
    _save_silences(silences, path)
    return False


def run_silence_cmd(args, path: Path = SILENCE_FILE) -> int:
    """Entry point called from CLI."""
    if args.silence_subcommand == "add":
        expiry = silence_pipeline(args.pipeline, args.minutes, path)
        print(f"Silenced '{args.pipeline}' until {expiry.strftime('%Y-%m-%d %H:%M')} UTC")
        return 0
    if args.silence_subcommand == "check":
        silenced = is_silenced(args.pipeline, path)
        print(f"'{args.pipeline}' is {'silenced' if silenced else 'not silenced'}")
        return 0
    print("Unknown silence subcommand")
    return 2
