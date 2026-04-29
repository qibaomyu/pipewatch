"""CLI registration tests for the rerun subcommand."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.rerun_cmd import register_rerun_subcommand


@pytest.fixture()
def _parser():
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_rerun_subcommand(sub)
    return root


def test_rerun_subcommand_defaults(_parser):
    args = _parser.parse_args(["rerun"])
    assert args.hours == 24.0
    assert args.pipeline is None
    assert args.json is False
    assert args.history_file == ".pipewatch_history.json"


def test_rerun_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["rerun", "--hours", "48"])
    assert args.hours == 48.0


def test_rerun_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["rerun", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_rerun_subcommand_json_flag(_parser):
    args = _parser.parse_args(["rerun", "--json"])
    assert args.json is True


def test_rerun_subcommand_custom_history(_parser):
    args = _parser.parse_args(["rerun", "--history-file", "/tmp/hist.json"])
    assert args.history_file == "/tmp/hist.json"


def test_rerun_subcommand_func_set(_parser):
    from pipewatch.commands.rerun_cmd import run_rerun_cmd
    args = _parser.parse_args(["rerun"])
    assert args.func is run_rerun_cmd
