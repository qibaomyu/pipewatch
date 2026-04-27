"""CLI-level registration tests for saturation subcommand."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.commands.saturation_cmd import register_saturation_subcommand


@pytest.fixture()
def _parser():
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_saturation_subcommand(sub)
    return root


def test_saturation_subcommand_defaults(_parser):
    args = _parser.parse_args(["saturation"])
    assert args.hours == 24.0
    assert args.limit == 100
    assert args.pipeline is None
    assert args.json is False
    assert args.exit_code is False
    assert args.history_file == ".pipewatch_history.json"


def test_saturation_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["saturation", "--hours", "48"])
    assert args.hours == 48.0


def test_saturation_subcommand_custom_limit(_parser):
    args = _parser.parse_args(["saturation", "--limit", "200"])
    assert args.limit == 200


def test_saturation_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["saturation", "--pipeline", "etl_daily"])
    assert args.pipeline == "etl_daily"


def test_saturation_subcommand_json_flag(_parser):
    args = _parser.parse_args(["saturation", "--json"])
    assert args.json is True


def test_saturation_subcommand_exit_code_flag(_parser):
    args = _parser.parse_args(["saturation", "--exit-code"])
    assert args.exit_code is True
