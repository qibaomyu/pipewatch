"""Ownership command: assign and query pipeline owners."""
from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Dict, List, Optional

DEFAULT_OWNERSHIP_FILE = "pipewatch_ownership.json"


def _load_owners(path: str) -> Dict[str, Dict]:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open() as fh:
        return json.load(fh)


def _save_owners(path: str, data: Dict[str, Dict]) -> None:
    with Path(path).open("w") as fh:
        json.dump(data, fh, indent=2)


def set_owner(pipeline: str, owner: str, team: Optional[str], path: str) -> None:
    data = _load_owners(path)
    data[pipeline] = {"owner": owner, "team": team or ""}
    _save_owners(path, data)


def get_owner(pipeline: str, path: str) -> Optional[Dict]:
    data = _load_owners(path)
    return data.get(pipeline)


def list_owners(path: str) -> List[Dict]:
    data = _load_owners(path)
    return [
        {"pipeline": p, "owner": v["owner"], "team": v["team"]}
        for p, v in sorted(data.items())
    ]


def run_ownership_cmd(args: Namespace) -> int:
    if args.ownership_action == "set":
        set_owner(args.pipeline, args.owner, getattr(args, "team", None), args.ownership_file)
        print(f"Owner '{args.owner}' assigned to pipeline '{args.pipeline}'.")
        return 0

    if args.ownership_action == "get":
        entry = get_owner(args.pipeline, args.ownership_file)
        if entry is None:
            print(f"No owner found for pipeline '{args.pipeline}'.")
            return 1
        team_part = f"  team: {entry['team']}" if entry["team"] else ""
        print(f"{args.pipeline}: {entry['owner']}{team_part}")
        return 0

    if args.ownership_action == "list":
        rows = list_owners(args.ownership_file)
        if not rows:
            print("No ownership entries found.")
            return 0
        for row in rows:
            team_part = f"  [{row['team']}]" if row["team"] else ""
            print(f"{row['pipeline']}: {row['owner']}{team_part}")
        return 0

    return 2


def register_ownership_subcommand(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser("ownership", help="Manage pipeline owners")
    p.add_argument("--ownership-file", default=DEFAULT_OWNERSHIP_FILE)
    sub = p.add_subparsers(dest="ownership_action")

    s = sub.add_parser("set", help="Assign an owner to a pipeline")
    s.add_argument("pipeline")
    s.add_argument("owner")
    s.add_argument("--team", default="")

    g = sub.add_parser("get", help="Get owner for a pipeline")
    g.add_argument("pipeline")

    sub.add_parser("list", help="List all pipeline owners")
    p.set_defaults(func=run_ownership_cmd)
