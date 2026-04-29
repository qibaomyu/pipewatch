"""Importability and wiring tests for breach_cmd_register."""
from __future__ import annotations

import argparse


def test_register_breach_subcommand_importable():
    from pipewatch.commands.breach_cmd_register import register_breach_subcommand
    assert callable(register_breach_subcommand)


def test_register_breach_subcommand_attaches_parser():
    from pipewatch.commands.breach_cmd_register import register_breach_subcommand

    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command")
    register_breach_subcommand(sub)

    args = root.parse_args(["breach"])
    assert args.command == "breach"
    assert hasattr(args, "func")
