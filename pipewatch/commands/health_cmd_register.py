"""Register the 'health' subcommand with the CLI parser."""
from __future__ import annotations

from argparse import _SubParsersAction


def register_health_subcommand(subparsers: _SubParsersAction) -> None:
    p = subparsers.add_parser(
        "health",
        help="Show current health status for monitored pipelines.",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        help="Limit output to a single pipeline.",
    )
    p.add_argument(
        "--window",
        type=int,
        default=10,
        help="Number of recent history entries to consider (default: 10).",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    p.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Return exit code 1 if any pipeline is failing.",
    )
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        help="Path to history file.",
    )
