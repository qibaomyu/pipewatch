"""Smoke test for drift_cmd_register."""
from __future__ import annotations

from pipewatch.commands.drift_cmd_register import register_drift_subcommand


def test_register_drift_subcommand_importable():
    assert callable(register_drift_subcommand)


def test_register_drift_subcommand_attaches_parser():
    import argparse
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_drift_subcommand(sub)
    args = root.parse_args(["drift"])
    assert args.command == "drift"
