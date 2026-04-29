"""Tests for throttle_cmd_register module."""
from __future__ import annotations

import argparse


def test_register_throttle_subcommand_importable():
    from pipewatch.commands.throttle_cmd_register import register_throttle_subcommand
    assert callable(register_throttle_subcommand)


def test_register_throttle_subcommand_attaches_parser():
    from pipewatch.commands.throttle_cmd_register import register_throttle_subcommand

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    register_throttle_subcommand(sub)

    args = parser.parse_args(["throttle", "--max-runs", "3"])
    assert args.max_runs == 3
    assert hasattr(args, "func")
