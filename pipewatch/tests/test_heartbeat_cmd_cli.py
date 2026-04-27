"""CLI-level tests for the heartbeat subcommand registration."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.heartbeat_cmd import register_heartbeat_subcommand


@pytest.fixture()
def _parser():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    register_heartbeat_subcommand(sub)
    return parser


def test_heartbeat_subcommand_defaults(_parser):
    args = _parser.parse_args(["heartbeat"])
    assert args.pipeline is None
    assert args.json is False
    assert args.exit_code is False
    assert args.history_file == ".pipewatch_history.json"


def test_heartbeat_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["heartbeat", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_heartbeat_subcommand_json_flag(_parser):
    args = _parser.parse_args(["heartbeat", "--json"])
    assert args.json is True


def test_heartbeat_subcommand_exit_code_flag(_parser):
    args = _parser.parse_args(["heartbeat", "--exit-code"])
    assert args.exit_code is True


def test_heartbeat_subcommand_custom_history(_parser):
    args = _parser.parse_args(["heartbeat", "--history-file", "/tmp/h.json"])
    assert args.history_file == "/tmp/h.json"


def test_heartbeat_subcommand_has_func(_parser):
    args = _parser.parse_args(["heartbeat"])
    assert callable(args.func)
