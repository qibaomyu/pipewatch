"""CLI integration tests for the trend subcommand."""
from __future__ import annotations

from pipewatch.cli import build_parser


def test_trend_subcommand_defaults():
    parser = build_parser()
    args = parser.parse_args(["trend"])
    assert args.subcommand == "trend"
    assert args.pipeline is None
    assert args.buckets == 5


def test_trend_subcommand_pipeline_filter():
    parser = build_parser()
    args = parser.parse_args(["trend", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_trend_subcommand_custom_buckets():
    parser = build_parser()
    args = parser.parse_args(["trend", "--buckets", "10"])
    assert args.buckets == 10


def test_trend_subcommand_history_file():
    parser = build_parser()
    args = parser.parse_args(["--history-file", "/tmp/h.json", "trend"])
    assert args.history_file == "/tmp/h.json"
