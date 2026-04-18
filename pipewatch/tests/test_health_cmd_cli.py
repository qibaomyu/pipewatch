"""CLI-level tests for the 'health' subcommand registration."""
from __future__ import annotations

from pipewatch.commands.health_cmd_register import register_health_subcommand
import argparse


def _parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    register_health_subcommand(sub)
    return p


def test_health_subcommand_defaults():
    args = _parser().parse_args(["health"])
    assert args.command == "health"
    assert args.pipeline is None
    assert args.window == 10
    assert args.format == "text"
    assert args.exit_code is False
    assert args.history_file == ".pipewatch_history.json"


def test_health_subcommand_pipeline_filter():
    args = _parser().parse_args(["health", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_health_subcommand_json_flag():
    args = _parser().parse_args(["health", "--format", "json"])
    assert args.format == "json"


def test_health_subcommand_exit_code_flag():
    args = _parser().parse_args(["health", "--exit-code"])
    assert args.exit_code is True


def test_health_subcommand_custom_window():
    args = _parser().parse_args(["health", "--window", "20"])
    assert args.window == 20
