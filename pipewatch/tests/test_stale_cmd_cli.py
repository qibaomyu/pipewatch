"""CLI-level tests for the stale subcommand registration."""
from __future__ import annotations

import argparse

from pipewatch.commands.stale_cmd import register_stale_subcommand


def _parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    register_stale_subcommand(sub)
    return p


def test_stale_subcommand_defaults():
    args = _parser().parse_args(["stale"])
    assert args.threshold == 60
    assert args.pipeline is None
    assert args.json is False
    assert args.exit_code is False
    assert args.history_file == ".pipewatch_history.json"


def test_stale_subcommand_custom_threshold():
    args = _parser().parse_args(["stale", "--threshold", "120"])
    assert args.threshold == 120


def test_stale_subcommand_pipeline_filter():
    args = _parser().parse_args(["stale", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_stale_subcommand_flags():
    args = _parser().parse_args(["stale", "--json", "--exit-code"])
    assert args.json is True
    assert args.exit_code is True
