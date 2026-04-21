"""dependency_cmd.py — Show pipeline dependency relationships and flag chains with failures."""

from __future__ import annotations

import json
from argparse import ArgumentParser, Namespace
from typing import Dict, List, Optional

from pipewatch.config import load_config
from pipewatch.history import RunHistory


def _pipeline_dependency(
    pipeline_name: str,
    depends_on: List[str],
    failed_pipelines: set,
) -> Dict:
    """Build a dependency summary for a single pipeline."""
    blocking = [dep for dep in depends_on if dep in failed_pipelines]
    return {
        "pipeline": pipeline_name,
        "depends_on": depends_on,
        "blocking_failures": blocking,
        "at_risk": len(blocking) > 0,
    }


def _format_text(rows: List[Dict]) -> str:
    """Render dependency rows as a human-readable table."""
    if not rows:
        return "No dependency information available."

    lines = [f"{'PIPELINE':<28} {'DEPENDS ON':<30} {'BLOCKING':<28} RISK"]
    lines.append("-" * 95)
    for row in rows:
        deps = ", ".join(row["depends_on"]) if row["depends_on"] else "-"
        blocking = ", ".join(row["blocking_failures"]) if row["blocking_failures"] else "-"
        risk = "⚠ AT RISK" if row["at_risk"] else "OK"
        lines.append(f"{row['pipeline']:<28} {deps:<30} {blocking:<28} {risk}")
    return "\n".join(lines)


def run_dependency_cmd(args: Namespace) -> int:
    """Entry point for the ``dependency`` subcommand.

    Loads pipeline config to discover declared dependencies, then checks
    recent history to determine which upstream pipelines are currently
    failing.  Pipelines that depend on a failing upstream are flagged as
    *at risk*.

    Returns exit code 1 when any pipeline is at risk (useful for CI),
    otherwise 0.
    """
    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"Config file not found: {args.config}")
        return 2

    history = RunHistory(path=args.history_file)

    # Determine which pipelines have a recent failure.
    failed_pipelines: set = set()
    for pipeline_cfg in cfg.pipelines:
        entries = history.get(pipeline_cfg.name, limit=1)
        if entries and not entries[0].healthy:
            failed_pipelines.add(pipeline_cfg.name)

    # Build dependency rows.
    rows: List[Dict] = []
    for pipeline_cfg in cfg.pipelines:
        # Dependencies are declared as an optional list in the pipeline config.
        depends_on: List[str] = getattr(pipeline_cfg, "depends_on", []) or []

        # Optional: filter to a single pipeline.
        if args.pipeline and pipeline_cfg.name != args.pipeline:
            continue

        rows.append(
            _pipeline_dependency(
                pipeline_name=pipeline_cfg.name,
                depends_on=depends_on,
                failed_pipelines=failed_pipelines,
            )
        )

    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        print(_format_text(rows))

    at_risk = any(r["at_risk"] for r in rows)
    if args.exit_code and at_risk:
        return 1
    return 0


def register_dependency_subcommand(subparsers) -> None:
    """Attach the ``dependency`` subcommand to the CLI argument parser."""
    parser: ArgumentParser = subparsers.add_parser(
        "dependency",
        help="Show pipeline dependency graph and highlight chains blocked by failures.",
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to pipewatch config file (default: pipewatch.yaml).",
    )
    parser.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to run-history file.",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Restrict output to a single pipeline.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Emit results as JSON.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if any pipeline is at risk.",
    )
    parser.set_defaults(func=run_dependency_cmd)
