"""incident_cmd: track and summarise open incidents per pipeline."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_INCIDENT_FILE = ".pipewatch_incidents.json"


def _load_incidents(path: str) -> dict[str, list[dict[str, Any]]]:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open() as fh:
        return json.load(fh)


def _save_incidents(path: str, data: dict[str, list[dict[str, Any]]]) -> None:
    with open(path, "w") as fh:
        json.dump(data, fh, indent=2)


def open_incident(
    pipeline: str, message: str, path: str = DEFAULT_INCIDENT_FILE
) -> dict[str, Any]:
    data = _load_incidents(path)
    data.setdefault(pipeline, [])
    entry: dict[str, Any] = {
        "id": len(data[pipeline]) + 1,
        "message": message,
        "opened_at": datetime.now(timezone.utc).isoformat(),
        "resolved_at": None,
    }
    data[pipeline].append(entry)
    _save_incidents(path, data)
    return entry


def resolve_incident(
    pipeline: str, incident_id: int, path: str = DEFAULT_INCIDENT_FILE
) -> bool:
    data = _load_incidents(path)
    for entry in data.get(pipeline, []):
        if entry["id"] == incident_id and entry["resolved_at"] is None:
            entry["resolved_at"] = datetime.now(timezone.utc).isoformat()
            _save_incidents(path, data)
            return True
    return False


def get_open_incidents(
    pipeline: str | None, path: str = DEFAULT_INCIDENT_FILE
) -> list[dict[str, Any]]:
    data = _load_incidents(path)
    results = []
    for name, entries in data.items():
        if pipeline and name != pipeline:
            continue
        for e in entries:
            if e["resolved_at"] is None:
                results.append({"pipeline": name, **e})
    return results


def run_incident_cmd(args: Any) -> int:
    if args.incident_action == "open":
        entry = open_incident(args.pipeline, args.message, path=args.incident_file)
        print(f"Opened incident #{entry['id']} for '{args.pipeline}': {args.message}")
        return 0

    if args.incident_action == "resolve":
        ok = resolve_incident(args.pipeline, args.incident_id, path=args.incident_file)
        if ok:
            print(f"Resolved incident #{args.incident_id} for '{args.pipeline}'")
            return 0
        print(
            f"No open incident #{args.incident_id} found for '{args.pipeline}'",
            file=sys.stderr,
        )
        return 2

    if args.incident_action == "list":
        open_inc = get_open_incidents(
            getattr(args, "pipeline", None), path=args.incident_file
        )
        if not open_inc:
            print("No open incidents.")
            return 0
        if getattr(args, "json", False):
            print(json.dumps(open_inc, indent=2))
        else:
            for inc in open_inc:
                print(
                    f"[{inc['pipeline']}] #{inc['id']} "
                    f"(opened {inc['opened_at']}): {inc['message']}"
                )
        return 0

    print(f"Unknown action: {args.incident_action}", file=sys.stderr)
    return 2


def register_incident_subcommand(subparsers: Any) -> None:
    p = subparsers.add_parser("incident", help="Manage pipeline incidents")
    p.add_argument(
        "--incident-file", default=DEFAULT_INCIDENT_FILE, help="Incidents store path"
    )
    sub = p.add_subparsers(dest="incident_action", required=True)

    op = sub.add_parser("open", help="Open a new incident")
    op.add_argument("pipeline", help="Pipeline name")
    op.add_argument("message", help="Incident description")

    rp = sub.add_parser("resolve", help="Resolve an open incident")
    rp.add_argument("pipeline", help="Pipeline name")
    rp.add_argument("incident_id", type=int, help="Incident ID to resolve")

    lp = sub.add_parser("list", help="List open incidents")
    lp.add_argument("--pipeline", default=None, help="Filter by pipeline")
    lp.add_argument("--json", action="store_true", help="Output as JSON")
