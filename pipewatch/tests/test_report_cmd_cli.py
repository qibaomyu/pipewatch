"""Integration tests: report subcommand wired into CLI parser."""
from __future__ import annotations

from pathlib import Path

import pytest

from pipewatch.cli import build_parser


def test_report_subcommand_defaults(tmp_path: Path):
    parser = build_parser()
    args = parser.parse_args(["report", "--history-file", str(tmp_path / "h.json")])
    assert args.subcommand == "report"
    assert args.hours == 24
    assert args.format == "text"
    assert args.exit_code is False


def test_report_subcommand_custom_hours(tmp_path: Path):
    parser = build_parser()
    args = parser.parse_args(["report", "--history-file", str(tmp_path / "h.json"), "--hours", "48"])
    assert args.hours == 48


def test_report_subcommand_json_flag(tmp_path: Path):
    parser = build_parser()
    args = parser.parse_args(["report", "--history-file", str(tmp_path / "h.json"), "--format", "json"])
    assert args.format == "json"


def test_report_subcommand_exit_code_flag(tmp_path: Path):
    parser = build_parser()
    args = parser.parse_args(["report", "--history-file", str(tmp_path / "h.json"), "--exit-code"])
    assert args.exit_code is True
