"""CLI-level tests for the quota subcommand."""
from __future__ import annotations

import pytest
from argparse import ArgumentParser

from pipewatch.commands.quota_cmd import register_quota_subcommand


@pytest.fixture()
def _parser():
    root = ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_quota_subcommand(sub)
    return root


def test_quota_subcommand_defaults(_parser):
    args = _parser.parse_args(["quota"])
    assert args.hours == 24
    assert args.limit == 100
    assert args.pipeline is None
    assert args.json is False
    assert args.exit_code is False
    assert args.history_file == ".pipewatch_history.json"


def test_quota_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["quota", "--hours", "6"])
    assert args.hours == 6


def test_quota_subcommand_custom_limit(_parser):
    args = _parser.parse_args(["quota", "--limit", "50"])
    assert args.limit == 50


def test_quota_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["quota", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_quota_subcommand_json_flag(_parser):
    args = _parser.parse_args(["quota", "--json"])
    assert args.json is True


def test_quota_subcommand_exit_code_flag(_parser):
    args = _parser.parse_args(["quota", "--exit-code"])
    assert args.exit_code is True
