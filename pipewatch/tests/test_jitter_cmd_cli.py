"""CLI-level tests for the jitter subcommand registration."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.jitter_cmd import register_jitter_subcommand


@pytest.fixture()
def _parser():
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_jitter_subcommand(sub)
    return root


def test_jitter_subcommand_defaults(_parser):
    args = _parser.parse_args(["jitter"])
    assert args.hours == 24
    assert args.pipeline is None
    assert args.json is False
    assert args.history_file == ".pipewatch_history.json"


def test_jitter_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["jitter", "--hours", "48"])
    assert args.hours == 48


def test_jitter_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["jitter", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_jitter_subcommand_json_flag(_parser):
    args = _parser.parse_args(["jitter", "--json"])
    assert args.json is True


def test_jitter_subcommand_custom_history(_parser):
    args = _parser.parse_args(["jitter", "--history-file", "/tmp/hist.json"])
    assert args.history_file == "/tmp/hist.json"


def test_jitter_subcommand_func_set(_parser):
    from pipewatch.commands.jitter_cmd import run_jitter_cmd
    args = _parser.parse_args(["jitter"])
    assert args.func is run_jitter_cmd
