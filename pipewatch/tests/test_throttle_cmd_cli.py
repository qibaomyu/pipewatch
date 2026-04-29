"""CLI-level tests for the throttle subcommand registration."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.throttle_cmd import register_throttle_subcommand


@pytest.fixture()
def _parser():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    register_throttle_subcommand(sub)
    return parser


def test_throttle_subcommand_defaults(_parser):
    args = _parser.parse_args(["throttle"])
    assert args.hours == 1.0
    assert args.max_runs == 10
    assert args.json is False
    assert args.exit_code is False
    assert args.pipeline is None
    assert args.history_file == ".pipewatch_history.json"


def test_throttle_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["throttle", "--hours", "6"])
    assert args.hours == 6.0


def test_throttle_subcommand_custom_max_runs(_parser):
    args = _parser.parse_args(["throttle", "--max-runs", "20"])
    assert args.max_runs == 20


def test_throttle_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["throttle", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_throttle_subcommand_json_flag(_parser):
    args = _parser.parse_args(["throttle", "--json"])
    assert args.json is True


def test_throttle_subcommand_exit_code_flag(_parser):
    args = _parser.parse_args(["throttle", "--exit-code"])
    assert args.exit_code is True
