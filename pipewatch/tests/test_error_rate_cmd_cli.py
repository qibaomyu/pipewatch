"""CLI registration tests for error-rate subcommand."""
from __future__ import annotations

import argparse
from pipewatch.commands.error_rate_cmd import register_error_rate_subcommand


def _parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    register_error_rate_subcommand(sub)
    return p


def test_error_rate_subcommand_defaults():
    args = _parser().parse_args(["error-rate"])
    assert args.hours == 24
    assert args.pipeline is None
    assert args.json is False
    assert args.history_file == ".pipewatch_history.json"


def test_error_rate_subcommand_custom_hours():
    args = _parser().parse_args(["error-rate", "--hours", "48"])
    assert args.hours == 48


def test_error_rate_subcommand_pipeline_filter():
    args = _parser().parse_args(["error-rate", "--pipeline", "my-pipe"])
    assert args.pipeline == "my-pipe"


def test_error_rate_subcommand_json_flag():
    args = _parser().parse_args(["error-rate", "--json"])
    assert args.json is True


def test_error_rate_subcommand_history_file():
    args = _parser().parse_args(["error-rate", "--history-file", "custom.json"])
    assert args.history_file == "custom.json"
