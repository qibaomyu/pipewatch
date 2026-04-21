"""CLI-level tests for the throughput subcommand registration."""
from __future__ import annotations

import argparse

from pipewatch.commands.throughput_cmd import register_throughput_subcommand


def _parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="pipewatch")
    sub = root.add_subparsers(dest="command")
    register_throughput_subcommand(sub)
    return root


def test_throughput_subcommand_defaults():
    args = _parser().parse_args(["throughput"])
    assert args.hours == 24
    assert args.pipeline is None
    assert args.json is False
    assert args.history_file == ".pipewatch_history.json"


def test_throughput_subcommand_custom_hours():
    args = _parser().parse_args(["throughput", "--hours", "48"])
    assert args.hours == 48


def test_throughput_subcommand_pipeline_filter():
    args = _parser().parse_args(["throughput", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_throughput_subcommand_json_flag():
    args = _parser().parse_args(["throughput", "--json"])
    assert args.json is True


def test_throughput_subcommand_custom_history_file():
    args = _parser().parse_args(["throughput", "--history-file", "/tmp/hist.json"])
    assert args.history_file == "/tmp/hist.json"
