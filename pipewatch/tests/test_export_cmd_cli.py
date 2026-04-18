"""Integration-level tests: export subcommand wired into CLI parser."""

import pytest
from pipewatch.cli import build_parser


def test_export_subcommand_defaults():
    parser = build_parser()
    args = parser.parse_args(["export", "--config", "pipewatch.yaml"])
    assert args.format == "csv"
    assert args.output is None
    assert args.pipeline is None


def test_export_subcommand_json_flag():
    parser = build_parser()
    args = parser.parse_args(["export", "--config", "pipewatch.yaml", "--format", "json"])
    assert args.format == "json"


def test_export_subcommand_pipeline_filter():
    parser = build_parser()
    args = parser.parse_args(["export", "--config", "pipewatch.yaml", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_export_subcommand_output_path():
    parser = build_parser()
    args = parser.parse_args(["export", "--config", "pipewatch.yaml", "--output", "/tmp/out.csv"])
    assert args.output == "/tmp/out.csv"
