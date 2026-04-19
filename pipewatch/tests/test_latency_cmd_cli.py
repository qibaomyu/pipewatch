"""CLI-level tests for the latency subcommand registration."""
from __future__ import annotations

from argparse import ArgumentParser

from pipewatch.commands.latency_cmd import register_latency_subcommand


def _parser():
    p = ArgumentParser()
    sub = p.add_subparsers(dest="command")
    register_latency_subcommand(sub)
    return p


def test_latency_subcommand_defaults():
    args = _parser().parse_args(["latency"])
    assert args.hours == 24
    assert args.pipeline is None
    assert args.json is False
    assert args.history_file == ".pipewatch_history.json"


def test_latency_subcommand_custom_hours():
    args = _parser().parse_args(["latency", "--hours", "48"])
    assert args.hours == 48


def test_latency_subcommand_pipeline_filter():
    args = _parser().parse_args(["latency", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_latency_subcommand_json_flag():
    args = _parser().parse_args(["latency", "--json"])
    assert args.json is True


def test_latency_subcommand_history_file():
    args = _parser().parse_args(["latency", "--history-file", "custom.json"])
    assert args.history_file == "custom.json"
