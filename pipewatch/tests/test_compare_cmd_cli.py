"""CLI integration tests for the compare subcommand."""
from __future__ import annotations

from pipewatch.cli import build_parser


def test_compare_subcommand_defaults():
    parser = build_parser()
    args = parser.parse_args(["compare"])
    assert args.command == "compare"
    assert args.window == 24
    assert args.pipeline is None
    assert args.format == "text"


def test_compare_subcommand_custom_window():
    parser = build_parser()
    args = parser.parse_args(["compare", "--window", "48"])
    assert args.window == 48


def test_compare_subcommand_pipeline_filter():
    parser = build_parser()
    args = parser.parse_args(["compare", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_compare_subcommand_json_flag():
    parser = build_parser()
    args = parser.parse_args(["compare", "--format", "json"])
    assert args.format == "json"
