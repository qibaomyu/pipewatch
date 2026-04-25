"""CLI-level tests for the drift subcommand."""
from __future__ import annotations

import argparse

from pipewatch.commands.drift_cmd import register_drift_subcommand


def _parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_drift_subcommand(sub)
    return root


def test_drift_subcommand_defaults():
    args = _parser().parse_args(["drift"])
    assert args.config == "pipewatch.yaml"
    assert args.baseline_file == ".pipewatch_baselines.json"
    assert args.pipeline is None
    assert args.json is False
    assert args.exit_code is False


def test_drift_subcommand_pipeline_filter():
    args = _parser().parse_args(["drift", "--pipeline", "my-pipe"])
    assert args.pipeline == "my-pipe"


def test_drift_subcommand_json_flag():
    args = _parser().parse_args(["drift", "--json"])
    assert args.json is True


def test_drift_subcommand_exit_code_flag():
    args = _parser().parse_args(["drift", "--exit-code"])
    assert args.exit_code is True


def test_drift_subcommand_custom_baseline_file():
    args = _parser().parse_args(["drift", "--baseline-file", "/tmp/bl.json"])
    assert args.baseline_file == "/tmp/bl.json"


def test_drift_subcommand_func_set():
    from pipewatch.commands.drift_cmd import run_drift_cmd
    args = _parser().parse_args(["drift"])
    assert args.func is run_drift_cmd
