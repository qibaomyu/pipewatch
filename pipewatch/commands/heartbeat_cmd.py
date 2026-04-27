"""heartbeat_cmd: report time since last successful run per pipeline."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from pipewatch.history import RunHistory


def _pipeline_heartbeat(
    history: RunHistory,
    pipeline: Optional[str],
    now: Optional[datetime] = None,
) -> list[dict]:
    """Return heartbeat info (seconds since last healthy run) for each pipeline."""
    if now is None:
        now = datetime.now(tz=timezone.utc)

    entries = history.all()
    if pipeline:
        entries = [e for e in entries if e.pipeline == pipeline]

    pipelines: dict[str, Optional[datetime]] = {}
    for entry in entries:
        if entry.healthy:
            ts = entry.timestamp
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            prev = pipelines.get(entry.pipeline)
            if prev is None or ts > prev:
                pipelines[entry.pipeline] = ts
        else:
            if entry.pipeline not in pipelines:
                pipelines[entry.pipeline] = None

    results = []
    for name, last_ok in sorted(pipelines.items()):
        if last_ok is None:
            age_seconds = None
            status = "never_healthy"
        else:
            age_seconds = (now - last_ok).total_seconds()
            status = "ok"
        results.append(
            {"pipeline": name, "last_healthy": last_ok.isoformat() if last_ok else None,
             "age_seconds": age_seconds, "status": status}
        )
    return results


def run_heartbeat_cmd(args) -> int:
    history = RunHistory(path=args.history_file)
    rows = _pipeline_heartbeat(history, getattr(args, "pipeline", None))

    if not rows:
        print("No pipeline data found.")
        return 0

    if getattr(args, "json", False):
        print(json.dumps(rows, indent=2))
        return 0

    print(f"{'Pipeline':<30} {'Last Healthy':<26} {'Age (s)':>10}  Status")
    print("-" * 75)
    for r in rows:
        last = r["last_healthy"] or "—"
        age = f"{r['age_seconds']:.0f}" if r["age_seconds"] is not None else "—"
        print(f"{r['pipeline']:<30} {last:<26} {age:>10}  {r['status']}")

    if getattr(args, "exit_code", False):
        if any(r["status"] != "ok" for r in rows):
            return 1
    return 0


def register_heartbeat_subcommand(subparsers) -> None:
    p = subparsers.add_parser("heartbeat", help="Show time since last healthy run per pipeline")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--exit-code", action="store_true", dest="exit_code",
                   help="Return exit code 1 if any pipeline has never been healthy")
    p.add_argument("--history-file", default=".pipewatch_history.json",
                   dest="history_file", help="Path to history file")
    p.set_defaults(func=run_heartbeat_cmd)
