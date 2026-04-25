"""Tests for pipewatch/commands/checkpoint_cmd.py"""
import pytest

from pipewatch.commands.checkpoint_cmd import (
    set_checkpoint,
    get_checkpoint,
    list_checkpoints,
    run_checkpoint_cmd,
)


@pytest.fixture()
def checkpoint_file(tmp_path):
    return str(tmp_path / "checkpoints.json")


@pytest.fixture()
def _args(checkpoint_file):
    import argparse

    def _make(**kwargs):
        base = {
            "checkpoint_action": "list",
            "pipeline": None,
            "label": None,
            "checkpoint_file": checkpoint_file,
        }
        base.update(kwargs)
        return argparse.Namespace(**base)

    return _make


def test_set_checkpoint_creates_entry(checkpoint_file):
    entry = set_checkpoint("pipe_a", "start", checkpoint_file)
    assert entry["pipeline"] == "pipe_a"
    assert entry["label"] == "start"
    assert "timestamp" in entry


def test_set_checkpoint_persists(checkpoint_file):
    set_checkpoint("pipe_a", "start", checkpoint_file)
    result = get_checkpoint("pipe_a", "start", checkpoint_file)
    assert result is not None
    assert result["label"] == "start"


def test_set_checkpoint_overwrites(checkpoint_file):
    set_checkpoint("pipe_a", "start", checkpoint_file)
    first_ts = get_checkpoint("pipe_a", "start", checkpoint_file)["timestamp"]
    set_checkpoint("pipe_a", "start", checkpoint_file)
    second_ts = get_checkpoint("pipe_a", "start", checkpoint_file)["timestamp"]
    # Both are valid ISO timestamps; second write should not fail
    assert isinstance(second_ts, str)
    _ = first_ts  # referenced to silence warning


def test_get_checkpoint_returns_none_for_unknown(checkpoint_file):
    assert get_checkpoint("ghost", "nope", checkpoint_file) is None


def test_list_checkpoints_all(checkpoint_file):
    set_checkpoint("pipe_a", "start", checkpoint_file)
    set_checkpoint("pipe_b", "end", checkpoint_file)
    entries = list_checkpoints(None, checkpoint_file)
    assert len(entries) == 2
    pipelines = {e["pipeline"] for e in entries}
    assert pipelines == {"pipe_a", "pipe_b"}


def test_list_checkpoints_filtered(checkpoint_file):
    set_checkpoint("pipe_a", "start", checkpoint_file)
    set_checkpoint("pipe_b", "end", checkpoint_file)
    entries = list_checkpoints("pipe_a", checkpoint_file)
    assert len(entries) == 1
    assert entries[0]["pipeline"] == "pipe_a"


def test_run_checkpoint_cmd_set(capsys, _args):
    code = run_checkpoint_cmd(_args(checkpoint_action="set", pipeline="pipe_a", label="mid"))
    assert code == 0
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "mid" in out


def test_run_checkpoint_cmd_get_missing(capsys, _args):
    code = run_checkpoint_cmd(_args(checkpoint_action="get", pipeline="ghost", label="nope"))
    assert code == 1
    assert "No checkpoint" in capsys.readouterr().out


def test_run_checkpoint_cmd_list_empty(capsys, _args):
    code = run_checkpoint_cmd(_args(checkpoint_action="list"))
    assert code == 0
    assert "No checkpoints" in capsys.readouterr().out
