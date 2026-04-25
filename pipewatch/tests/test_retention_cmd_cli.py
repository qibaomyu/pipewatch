"""CLI-level tests for the retention subcommand registration."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.retention_cmd import register_retention_subcommand


@pytest.fixture()
def _parser():
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_retention_subcommand(sub)
    return root


def test_retention_subcommand_defaults(_parser):
    args = _parser.parse_args(["retention"])
    assert args.history_file == "pipewatch_history.json"
    assert args.pipeline is None
    assert args.top == 20
    assert args.json is False


def test_retention_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["retention", "--pipeline", "etl"])
    assert args.pipeline == "etl"


def test_retention_subcommand_json_flag(_parser):
    args = _parser.parse_args(["retention", "--json"])
    assert args.json is True


def test_retention_subcommand_custom_top(_parser):
    args = _parser.parse_args(["retention", "--top", "5"])
    assert args.top == 5


def test_retention_subcommand_custom_history(_parser):
    args = _parser.parse_args(["retention", "--history-file", "custom.json"])
    assert args.history_file == "custom.json"


def test_retention_subcommand_sets_func(_parser):
    from pipewatch.commands.retention_cmd import run_retention_cmd
    args = _parser.parse_args(["retention"])
    assert args.func is run_retention_cmd
