"""Register the alert-log subcommand with the CLI parser."""
from __future__ import annotations

from argparse import _SubParsersAction

from pipewatch.commands.alert_log_cmd import run_alert_log_cmd


def register_alert_log_subcommand(subparsers: _SubParsersAction, default_history: str) -> None:
    p = subparsers.add_parser(
        "alert-log",
        help="Show alert history across pipeline runs.",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        help="Filter alerts to a specific pipeline.",
    )
    p.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output as JSON.",
    )
    p.add_argument(
        "--history-file",
        default=default_history,
        help="Path to history file.",
    )
    p.set_defaults(func=run_alert_log_cmd)
