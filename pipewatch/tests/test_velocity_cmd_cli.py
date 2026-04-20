"""CLI-level registration tests for the velocity subcommand."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.velocity_cmd import register_velocity_subcommand


@pytest.fixture()
def _parser():
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_velocity_subcommand(sub)
    return root


def test_velocity_subcommand_defaults(_parser):
    args = _parser.parse_args(["velocity"])
    assert args.hours == 24
    assert args.pipeline is None
    assert args.json is False
    assert args.history_file == ".pipewatch_history.json"


def test_velocity_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["velocity", "--hours", "48"])
    assert args.hours == 48


def test_velocity_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["velocity", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_velocity_subcommand_json_flag(_parser):
    args = _parser.parse_args(["velocity", "--json"])
    assert args.json is True


def test_velocity_subcommand_custom_history(_parser):
    args = _parser.parse_args(["velocity", "--history-file", "/tmp/hist.json"])
    assert args.history_file == "/tmp/hist.json"


def test_velocity_subcommand_func_set(_parser):
    from pipewatch.commands.velocity_cmd import run_velocity_cmd
    args = _parser.parse_args(["velocity"])
    assert args.func is run_velocity_cmd
