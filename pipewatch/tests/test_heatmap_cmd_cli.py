"""CLI-level parser tests for the heatmap subcommand."""
import pytest
from argparse import ArgumentParser

from pipewatch.commands.heatmap_cmd import register_heatmap_subcommand


@pytest.fixture
def _parser():
    p = ArgumentParser()
    sub = p.add_subparsers(dest="command")
    register_heatmap_subcommand(sub)
    return p


def test_heatmap_subcommand_defaults(_parser):
    args = _parser.parse_args(["heatmap"])
    assert args.hours == 24
    assert args.pipeline is None
    assert args.json is False
    assert args.history_file == ".pipewatch_history.json"


def test_heatmap_subcommand_custom_hours(_parser):
    args = _parser.parse_args(["heatmap", "--hours", "48"])
    assert args.hours == 48


def test_heatmap_subcommand_pipeline_filter(_parser):
    args = _parser.parse_args(["heatmap", "--pipeline", "etl"])
    assert args.pipeline == "etl"


def test_heatmap_subcommand_json_flag(_parser):
    args = _parser.parse_args(["heatmap", "--json"])
    assert args.json is True


def test_heatmap_subcommand_history_file(_parser):
    args = _parser.parse_args(["heatmap", "--history-file", "/tmp/h.json"])
    assert args.history_file == "/tmp/h.json"
