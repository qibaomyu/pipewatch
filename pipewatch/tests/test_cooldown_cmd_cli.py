"""CLI-level parser tests for the cooldown subcommand."""
from __future__ import annotations

import pytest

from pipewatch.commands.cooldown_cmd import register_cooldown_subcommand
import argparse


@pytest.fixture()
def _parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    register_cooldown_subcommand(sub)
    return p


def test_cooldown_subcommand_defaults(_parser):
    args = _parser.parse_args(["cooldown"])
    assert args.history_file == ".pipewatch_history.json"
    assert args.cooldown_minutes == 30
    assert args.pipeline is None
    assert args.json is False
    assert args.exit_code is False


def test_cooldown_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["cooldown", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_cooldown_subcommand_custom_minutes(_parser):
    args = _parser.parse_args(["cooldown", "--cooldown-minutes", "60"])
    assert args.cooldown_minutes == 60


def test_cooldown_subcommand_json_flag(_parser):
    args = _parser.parse_args(["cooldown", "--json"])
    assert args.json is True


def test_cooldown_subcommand_exit_code_flag(_parser):
    args = _parser.parse_args(["cooldown", "--exit-code"])
    assert args.exit_code is True


def test_cooldown_subcommand_custom_history_file(_parser):
    args = _parser.parse_args(["cooldown", "--history-file", "/tmp/hist.json"])
    assert args.history_file == "/tmp/hist.json"
