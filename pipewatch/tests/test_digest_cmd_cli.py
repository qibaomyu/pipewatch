"""CLI integration tests for the digest subcommand."""
from __future__ import annotations

from pipewatch.cli import build_parser


def test_digest_subcommand_defaults():
    parser = build_parser()
    args = parser.parse_args(["digest", "--config", "pipewatch.yaml"])
    assert args.subcommand == "digest"
    assert args.hours == 24
    assert args.pipeline is None
    assert args.format == "text"


def test_digest_subcommand_custom_hours():
    parser = build_parser()
    args = parser.parse_args(["digest", "--config", "pipewatch.yaml", "--hours", "48"])
    assert args.hours == 48


def test_digest_subcommand_pipeline_filter():
    parser = build_parser()
    args = parser.parse_args(["digest", "--config", "pipewatch.yaml", "--pipeline", "etl"])
    assert args.pipeline == "etl"


def test_digest_subcommand_json_flag():
    parser = build_parser()
    args = parser.parse_args(["digest", "--config", "pipewatch.yaml", "--format", "json"])
    assert args.format == "json"
