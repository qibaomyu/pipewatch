"""budget_cmd: track cumulative error counts against a rolling budget."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pipewatch.history import RunHistory


def _pipeline_budget(
    entries: list,
    pipeline: str,
    hours: int,
    limit: int,
) -> dict[str, Any]:
    """Return budget consumption stats for *pipeline* over the last *hours*."""
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    relevant = [
        e for e in entries
        if e.pipeline == pipeline and e.timestamp >= cutoff
    ]
    if not relevant:
        return {"pipeline": pipeline, "runs": 0, "errors": 0, "limit": limit,
                "remaining": limit, "pct_used": 0.0, "breached": False}

    total_errors = sum(getattr(e, "error_count", 0) for e in relevant)
    remaining = max(0, limit - total_errors)
    pct_used = round(total_errors / limit * 100, 1) if limit > 0 else 0.0
    return {
        "pipeline": pipeline,
        "runs": len(relevant),
        "errors": total_errors,
        "limit": limit,
        "remaining": remaining,
        "pct_used": pct_used,
        "breached": total_errors > limit,
    }


def _format_text(rows: list[dict[str, Any]]) -> str:
    lines = []
    for r in rows:
        status = "BREACHED" if r["breached"] else "ok"
        lines.append(
            f"{r['pipeline']}: {r['errors']}/{r['limit']} errors "
            f"({r['pct_used']}% used, {r['remaining']} remaining) [{status}]"
        )
    return "\n".join(lines) if lines else "No data."


def run_budget_cmd(args) -> int:  # noqa: ANN001
    history = RunHistory(args.history_file)
    entries = history.all()
    pipelines = (
        [args.pipeline] if args.pipeline
        else sorted({e.pipeline for e in entries})
    )
    rows = [
        _pipeline_budget(entries, p, args.hours, args.limit)
        for p in pipelines
    ]
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        print(_format_text(rows))
    if args.exit_code and any(r["breached"] for r in rows):
        return 1
    return 0


def register_budget_subcommand(subparsers) -> None:  # noqa: ANN001
    p = subparsers.add_parser("budget", help="Track error budget consumption")
    p.add_argument("--hours", type=int, default=24,
                   help="Rolling window in hours (default: 24)")
    p.add_argument("--limit", type=int, default=100,
                   help="Error budget limit (default: 100)")
    p.add_argument("--pipeline", default=None, help="Filter to one pipeline")
    p.add_argument("--json", action="store_true", help="JSON output")
    p.add_argument("--exit-code", action="store_true",
                   help="Exit 1 if any pipeline has breached its budget")
    p.add_argument("--history-file", default="pipewatch_history.json")
    p.set_defaults(func=run_budget_cmd)
