"""CLI integration tests for the spike subcommand."""
import argparse

import pytest

from pipewatch.commands.spike_cmd import register_spike_subcommand


@pytest.fixture
def _parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    register_spike_subcommand(sub)
    return p


def test_spike_subcommand_defaults(_parser):
    args = _parser.parse_args(["spike"])
    assert args.window == 5
    assert args.multiplier == 2.0
    assert args.json is False
    assert args.exit_code is False
    assert args.pipeline is None
    assert args.history_file == ".pipewatch_history.json"


def test_spike_subcommand_custom_window(_parser):
    args = _parser.parse_args(["spike", "--window", "10"])
    assert args.window == 10


def test_spike_subcommand_multiplier(_parser):
    args = _parser.parse_args(["spike", "--multiplier", "3.5"])
    assert args.multiplier == 3.5


def test_spike_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["spike", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_spike_subcommand_flags(_parser):
    args = _parser.parse_args(["spike", "--json", "--exit-code"])
    assert args.json is True
    assert args.exit_code is True


def test_spike_subcommand_custom_history_file(_parser):
    """Ensure --history-file overrides the default path."""
    args = _parser.parse_args(["spike", "--history-file", "/tmp/custom_history.json"])
    assert args.history_file == "/tmp/custom_history.json"
