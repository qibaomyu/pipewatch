"""CLI registration tests for the recovery subcommand."""
from argparse import ArgumentParser

import pytest

from pipewatch.commands.recovery_cmd import register_recovery_subcommand


@pytest.fixture()
def _parser():
    p = ArgumentParser()
    sub = p.add_subparsers(dest="command")
    register_recovery_subcommand(sub)
    return p


def test_recovery_subcommand_defaults(_parser):
    args = _parser.parse_args(["recovery"])
    assert args.history_file == ".pipewatch_history.json"
    assert args.pipeline is None
    assert args.json is False


def test_recovery_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["recovery", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_recovery_subcommand_json_flag(_parser):
    args = _parser.parse_args(["recovery", "--json"])
    assert args.json is True


def test_recovery_subcommand_custom_history(_parser):
    args = _parser.parse_args(["recovery", "--history-file", "custom.json"])
    assert args.history_file == "custom.json"
