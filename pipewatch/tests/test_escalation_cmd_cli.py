"""CLI-level tests for the escalation subcommand registration."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.escalation_cmd import (
    DEFAULT_ESCALATION_FILE,
    DEFAULT_HOURS,
    DEFAULT_THRESHOLD,
    register_escalation_subcommand,
)


@pytest.fixture()
def _parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    sub = root.add_subparsers()
    register_escalation_subcommand(sub)
    return root


def test_escalation_subcommand_defaults(_parser):
    args = _parser.parse_args(["escalation"])
    assert args.hours == DEFAULT_HOURS
    assert args.threshold == DEFAULT_THRESHOLD
    assert args.pipeline is None
    assert args.json is False
    assert args.exit_code is False
    assert args.escalation_file == DEFAULT_ESCALATION_FILE


def test_escalation_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["escalation", "--hours", "48"])
    assert args.hours == 48


def test_escalation_subcommand_custom_threshold(_parser):
    args = _parser.parse_args(["escalation", "--threshold", "5"])
    assert args.threshold == 5


def test_escalation_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["escalation", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_escalation_subcommand_json_flag(_parser):
    args = _parser.parse_args(["escalation", "--json"])
    assert args.json is True


def test_escalation_subcommand_exit_code_flag(_parser):
    args = _parser.parse_args(["escalation", "--exit-code"])
    assert args.exit_code is True


def test_escalation_subcommand_custom_file(_parser, tmp_path):
    f = str(tmp_path / "custom.json")
    args = _parser.parse_args(["escalation", "--escalation-file", f])
    assert args.escalation_file == f
