"""Registration helper for the digest subcommand — imported by cli.py."""
from __future__ import annotations

import argparse


def register_digest_subcommand(subparsers: argparse._SubParsersAction) -> None:
    """Attach the 'digest' subcommand to *subparsers*."""
    p = subparsers.add_parser(
        "digest",
        help="Show a digest summary of pipeline health over a time window.",
    )
    p.add_argument(
        "--hours",
        type=int,
        default=24,
        metavar="N",
        help="Look-back window in hours (default: 24).",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        metavar="NAME",
        help="Restrict digest to a single pipeline.",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    p.add_argument(
        "--history-file",
        default=".pipewatch_history.json",
        metavar="PATH",
        help="Path to the run-history file.",
    )
