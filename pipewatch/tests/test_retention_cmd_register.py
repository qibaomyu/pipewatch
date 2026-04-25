"""Smoke tests for retention_cmd_register."""
from __future__ import annotations

import argparse


def test_register_retention_subcommand_importable():
    from pipewatch.commands.retention_cmd_register import register_retention_subcommand
    assert callable(register_retention_subcommand)


def test_register_retention_subcommand_attaches_parser():
    from pipewatch.commands.retention_cmd_register import register_retention_subcommand

    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_retention_subcommand(sub)
    args = root.parse_args(["retention"])
    assert args.command == "retention"
