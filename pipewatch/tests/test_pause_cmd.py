"""Tests for pipewatch/commands/pause_cmd.py"""
import argparse
import json
import os
import pytest

from pipewatch.commands.pause_cmd import (
    DEFAULT_PAUSE_FILE,
    get_pause_info,
    is_paused,
    pause_pipeline,
    resume_pipeline,
    run_pause_cmd,
)


@pytest.fixture()
def pause_file(tmp_path):
    return str(tmp_path / "pauses.json")


def _args(action, pipeline="", reason="", pause_file=DEFAULT_PAUSE_FILE):
    ns = argparse.Namespace(
        pause_action=action,
        pipeline=pipeline,
        reason=reason,
        pause_file=pause_file,
    )
    return ns


# --- unit tests ---

def test_pause_pipeline_creates_entry(pause_file):
    pause_pipeline("etl", reason="maintenance", pause_file=pause_file)
    assert is_paused("etl", pause_file=pause_file)


def test_pause_pipeline_persists_reason(pause_file):
    pause_pipeline("etl", reason="deploy freeze", pause_file=pause_file)
    info = get_pause_info("etl", pause_file=pause_file)
    assert info["reason"] == "deploy freeze"
    assert "paused_at" in info


def test_resume_pipeline_returns_true(pause_file):
    pause_pipeline("etl", pause_file=pause_file)
    result = resume_pipeline("etl", pause_file=pause_file)
    assert result is True
    assert not is_paused("etl", pause_file=pause_file)


def test_resume_unknown_returns_false(pause_file):
    result = resume_pipeline("unknown", pause_file=pause_file)
    assert result is False


def test_is_paused_returns_false_for_unknown(pause_file):
    assert not is_paused("ghost", pause_file=pause_file)


def test_get_pause_info_returns_none_for_unknown(pause_file):
    assert get_pause_info("ghost", pause_file=pause_file) is None


# --- run_pause_cmd tests ---

def test_run_pause_cmd_pause(pause_file, capsys):
    rc = run_pause_cmd(_args("pause", pipeline="etl", reason="test", pause_file=pause_file))
    assert rc == 0
    assert is_paused("etl", pause_file=pause_file)
    out = capsys.readouterr().out
    assert "paused" in out


def test_run_pause_cmd_resume(pause_file, capsys):
    pause_pipeline("etl", pause_file=pause_file)
    rc = run_pause_cmd(_args("resume", pipeline="etl", pause_file=pause_file))
    assert rc == 0
    assert not is_paused("etl", pause_file=pause_file)


def test_run_pause_cmd_resume_not_paused(pause_file, capsys):
    rc = run_pause_cmd(_args("resume", pipeline="nope", pause_file=pause_file))
    assert rc == 0
    out = capsys.readouterr().out
    assert "not paused" in out


def test_run_pause_cmd_list_empty(pause_file, capsys):
    rc = run_pause_cmd(_args("list", pause_file=pause_file))
    assert rc == 0
    assert "No pipelines" in capsys.readouterr().out


def test_run_pause_cmd_list_shows_entries(pause_file, capsys):
    pause_pipeline("alpha", reason="testing", pause_file=pause_file)
    pause_pipeline("beta", pause_file=pause_file)
    rc = run_pause_cmd(_args("list", pause_file=pause_file))
    assert rc == 0
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out
    assert "testing" in out
