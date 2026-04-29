"""Escalation command: track and report repeated alert escalations per pipeline."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_ESCALATION_FILE = "pipewatch_escalations.json"
DEFAULT_THRESHOLD = 3
DEFAULT_HOURS = 24


def _load_escalations(path: str) -> dict[str, list[dict[str, Any]]]:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open() as fh:
        return json.load(fh)


def _save_escalations(path: str, data: dict[str, list[dict[str, Any]]]) -> None:
    with open(path, "w") as fh:
        json.dump(data, fh, indent=2)


def record_escalation(pipeline: str, level: str, path: str = DEFAULT_ESCALATION_FILE) -> None:
    """Append an escalation event for *pipeline*."""
    data = _load_escalations(path)
    data.setdefault(pipeline, []).append(
        {"ts": datetime.now(timezone.utc).isoformat(), "level": level}
    )
    _save_escalations(path, data)


def _pipeline_escalation(
    pipeline: str,
    events: list[dict[str, Any]],
    hours: int,
    threshold: int,
) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    recent = [
        e for e in events
        if datetime.fromisoformat(e["ts"]).timestamp() >= cutoff
    ]
    count = len(recent)
    escalated = count >= threshold
    return {"pipeline": pipeline, "count": count, "escalated": escalated, "threshold": threshold}


def run_escalation_cmd(args: Any) -> int:
    data = _load_escalations(args.escalation_file)
    pipelines = [args.pipeline] if args.pipeline else list(data.keys())

    if not pipelines:
        print("No escalation data found.")
        return 0

    results = [
        _pipeline_escalation(p, data.get(p, []), args.hours, args.threshold)
        for p in pipelines
    ]

    if args.json:
        import json as _json
        print(_json.dumps(results, indent=2))
    else:
        for r in results:
            flag = " [ESCALATED]" if r["escalated"] else ""
            print(f"{r['pipeline']}: {r['count']} escalation(s) in {args.hours}h (threshold={r['threshold']}){flag}")

    if args.exit_code and any(r["escalated"] for r in results):
        return 1
    return 0


def register_escalation_subcommand(subparsers: Any) -> None:
    p = subparsers.add_parser("escalation", help="Report repeated alert escalations per pipeline")
    p.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="Lookback window in hours")
    p.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD, help="Escalation count threshold")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--exit-code", action="store_true", help="Exit 1 if any pipeline is escalated")
    p.add_argument("--escalation-file", default=DEFAULT_ESCALATION_FILE, help="Path to escalations file")
    p.set_defaults(func=run_escalation_cmd)
