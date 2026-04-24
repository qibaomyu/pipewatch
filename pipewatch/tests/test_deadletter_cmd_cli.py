"""CLI-level tests for the dead-letter subcommand registration."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.deadletter_cmd import register_deadletter_subcommand


@pytest.fixture()
def _parser():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    register_deadletter_subcommand(sub)
    return parser


def test_deadletter_subcommand_defaults(_parser):
    args = _parser.parse_args(["deadletter"])
    assert args.threshold == 5
    assert args.pipeline is None
    assert args.history_file == ".pipewatch_history.json"
    assert args.json is False
    assert args.exit_code is False


def test_deadletter_subcommand_custom_threshold(_parser):
    args = _parser.parse_args(["deadletter", "--threshold", "10"])
    assert args.threshold == 10


def test_deadletter_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["deadletter", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_deadletter_subcommand_json_flag(_parser):
    args = _parser.parse_args(["deadletter", "--json"])
    assert args.json is True


def test_deadletter_subcommand_exit_code_flag(_parser):
    args = _parser.parse_args(["deadletter", "--exit-code"])
    assert args.exit_code is True


def test_deadletter_subcommand_custom_history(_parser):
    args = _parser.parse_args(["deadletter", "--history-file", "/tmp/hist.json"])
    assert args.history_file == "/tmp/hist.json"
