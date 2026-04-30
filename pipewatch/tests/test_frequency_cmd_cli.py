"""CLI registration tests for frequency subcommand."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.frequency_cmd import register_frequency_subcommand


@pytest.fixture()
def _parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    register_frequency_subcommand(subparsers)
    return parser


def test_frequency_subcommand_defaults(_parser):
    args = _parser.parse_args(["frequency"])
    assert args.hours == 24
    assert args.pipeline is None
    assert args.json is False
    assert args.history_file == ".pipewatch_history.json"


def test_frequency_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["frequency", "--hours", "48"])
    assert args.hours == 48


def test_frequency_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["frequency", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_frequency_subcommand_json_flag(_parser):
    args = _parser.parse_args(["frequency", "--json"])
    assert args.json is True


def test_frequency_subcommand_custom_history_file(_parser):
    args = _parser.parse_args(["frequency", "--history-file", "/tmp/hist.json"])
    assert args.history_file == "/tmp/hist.json"


def test_register_frequency_subcommand_importable():
    from pipewatch.commands.frequency_cmd_register import register_frequency_subcommand as fn
    assert callable(fn)
