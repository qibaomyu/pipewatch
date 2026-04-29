"""Import and registration smoke-tests for throughput_floor_cmd_register."""
from __future__ import annotations

import argparse

from pipewatch.commands.throughput_floor_cmd_register import register_throughput_floor_subcommand


def test_register_throughput_floor_subcommand_importable():
    assert callable(register_throughput_floor_subcommand)


def test_register_throughput_floor_subcommand_attaches_parser():
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_throughput_floor_subcommand(sub)
    args = root.parse_args(["throughput-floor"])
    assert args.command == "throughput-floor"
    assert hasattr(args, "func")
