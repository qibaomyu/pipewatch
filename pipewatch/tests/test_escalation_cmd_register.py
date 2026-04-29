"""Smoke tests for the escalation_cmd_register shim."""
from __future__ import annotations

import argparse


def test_register_escalation_subcommand_importable():
    from pipewatch.commands.escalation_cmd_register import register_escalation_subcommand
    assert callable(register_escalation_subcommand)


def test_register_escalation_subcommand_attaches_parser():
    from pipewatch.commands.escalation_cmd_register import register_escalation_subcommand

    root = argparse.ArgumentParser()
    sub = root.add_subparsers()
    register_escalation_subcommand(sub)
    args = root.parse_args(["escalation"])
    assert hasattr(args, "func")
