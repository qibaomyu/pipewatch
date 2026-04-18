"""Register the 'tag' subcommand with the CLI argument parser."""
from __future__ import annotations


def register_tag_subcommand(subparsers) -> None:
    parser = subparsers.add_parser("tag", help="Manage pipeline tags")
    parser.add_argument(
        "tag_action",
        choices=["add", "remove", "list", "find"],
        help="Action to perform",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Pipeline name (required for add/remove/list)",
    )
    parser.add_argument(
        "--tag",
        default=None,
        help="Tag label (required for add/remove/find)",
    )
    parser.add_argument(
        "--tags-file",
        default="pipewatch_tags.json",
        dest="tags_file",
        help="Path to tags JSON file",
    )
