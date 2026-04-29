"""CLI registration tests for throughput-floor subcommand."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.throughput_floor_cmd import register_throughput_floor_subcommand


@pytest.fixture()
def _parser():
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_throughput_floor_subcommand(sub)
    return root


def test_throughput_floor_subcommand_defaults(_parser):
    args = _parser.parse_args(["throughput-floor"])
    assert args.hours == 24
    assert args.min_runs == 1
    assert args.pipeline is None
    assert args.json is False
    assert args.exit_code is False
    assert args.history_file == ".pipewatch_history.json"


def test_throughput_floor_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["throughput-floor", "--hours", "48"])
    assert args.hours == 48


def test_throughput_floor_subcommand_custom_min_runs(_parser):
    args = _parser.parse_args(["throughput-floor", "--min-runs", "10"])
    assert args.min_runs == 10


def test_throughput_floor_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["throughput-floor", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_throughput_floor_subcommand_json_flag(_parser):
    args = _parser.parse_args(["throughput-floor", "--json"])
    assert args.json is True


def test_throughput_floor_subcommand_exit_code_flag(_parser):
    args = _parser.parse_args(["throughput-floor", "--exit-code"])
    assert args.exit_code is True


def test_throughput_floor_subcommand_custom_history(_parser):
    args = _parser.parse_args(["throughput-floor", "--history-file", "/tmp/h.json"])
    assert args.history_file == "/tmp/h.json"
