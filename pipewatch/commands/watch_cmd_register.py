"""Thin registration shim so cli.py stays uniform."""
from pipewatch.commands.watch_cmd import register_watch_subcommand

__all__ = ["register_watch_subcommand"]
