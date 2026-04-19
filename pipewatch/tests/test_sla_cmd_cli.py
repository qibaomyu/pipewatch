"""CLI-level tests for the sla subcommand registration."""
from __future__ import annotations

import argparse

from pipewatch.commands.sla_cmd import register_sla_subcommand


def _parser():
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_sla_subcommand(sub)
    return root


def test_sla_subcommand_defaults():
    args = _parser().parse_args(["sla"])
    assert args.hours == 24
    assert args.max_error_rate == 0.05
    assert args.max_latency == 5.0
    assert args.json is False
    assert args.exit_code is False
    assert args.pipeline is None


def test_sla_subcommand_custom_hours():
    args = _parser().parse_args(["sla", "--hours", "48"])
    assert args.hours == 48


def test_sla_subcommand_pipeline_filter():
    args = _parser().parse_args(["sla", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_sla_subcommand_json_flag():
    args = _parser().parse_args(["sla", "--json"])
    assert args.json is True


def test_sla_subcommand_exit_code_flag():
    args = _parser().parse_args(["sla", "--exit-code"])
    assert args.exit_code is True


def test_sla_subcommand_custom_thresholds():
    args = _parser().parse_args(["sla", "--max-error-rate", "0.10", "--max-latency", "3.0"])
    assert args.max_error_rate == 0.10
    assert args.max_latency == 3.0
