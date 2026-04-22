"""CLI-level tests for the budget subcommand."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.budget_cmd import register_budget_subcommand


@pytest.fixture()
def _parser():
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_budget_subcommand(sub)
    return root


def test_budget_subcommand_defaults(_parser):
    args = _parser.parse_args(["budget"])
    assert args.hours == 24
    assert args.limit == 100
    assert args.pipeline is None
    assert args.json is False
    assert args.exit_code is False
    assert args.history_file == "pipewatch_history.json"


def test_budget_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["budget", "--hours", "48"])
    assert args.hours == 48


def test_budget_subcommand_custom_limit(_parser):
    args = _parser.parse_args(["budget", "--limit", "250"])
    assert args.limit == 250


def test_budget_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["budget", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_budget_subcommand_json_flag(_parser):
    args = _parser.parse_args(["budget", "--json"])
    assert args.json is True


def test_budget_subcommand_exit_code_flag(_parser):
    args = _parser.parse_args(["budget", "--exit-code"])
    assert args.exit_code is True


def test_budget_subcommand_custom_history(_parser):
    args = _parser.parse_args(["budget", "--history-file", "/tmp/h.json"])
    assert args.history_file == "/tmp/h.json"
