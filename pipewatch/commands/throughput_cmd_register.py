"""Registry module for throughput subcommand.

This module exposes the registration function for the throughput subcommand,
allowing it to be imported and wired into the CLI entry point.
"""

from pipewatch.commands.throughput_cmd import register_throughput_subcommand

__all__ = ["register_throughput_subcommand"]
