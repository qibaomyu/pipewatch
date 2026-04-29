"""CLI-level tests for the breach subcommand parser."""
from __future__ import annotations

import argparse
import pytest

from pipewatch.commands.breach_cmd import register_breach_subcommand


@pytest.fixture()
def _parser():
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_breach_subcommand(sub)
    return root


def test_breach_subcommand_defaults(_parser):
    args = _parser.parse_args(["breach"])
    assert args.hours == 24
    assert args.min_breaches == 3
    assert args.pipeline is None
    assert args.json is False
    assert args.exit_code is False
    assert args.history_file == ".pipewatch_history.json"


def test_breach_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["breach", "--hours", "48"])
    assert args.hours == 48


def test_breach_subcommand_custom_min_breaches(_parser):
    args = _parser.parse_args(["breach", "--min-breaches", "5"])
    assert args.min_breaches == 5


def test_breach_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["breach", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_breach_subcommand_json_flag(_parser):
    args = _parser.parse_args(["breach", "--json"])
    assert args.json is True


def test_breach_subcommand_exit_code_flag(_parser):
    args = _parser.parse_args(["breach", "--exit-code"])
    assert args.exit_code is True


def test_breach_subcommand_custom_history(_parser):
    args = _parser.parse_args(["breach", "--history-file", "/tmp/h.json"])
    assert args.history_file == "/tmp/h.json"
