"""Tests for pipewatch.commands.silence_cmd."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipewatch.commands.silence_cmd import (
    is_silenced,
    run_silence_cmd,
    silence_pipeline,
)


@pytest.fixture
def silence_file(tmp_path):
    return tmp_path / "silences.json"


def test_silence_pipeline_creates_entry(silence_file):
    expiry = silence_pipeline("etl", 30, silence_file)
    assert silence_file.exists()
    data = json.loads(silence_file.read_text())
    assert "etl" in data
    assert datetime.fromisoformat(data["etl"]) > datetime.utcnow()


def test_is_silenced_returns_true_within_window(silence_file):
    silence_pipeline("etl", 10, silence_file)
    assert is_silenced("etl", silence_file) is True


def test_is_silenced_returns_false_for_unknown(silence_file):
    assert is_silenced("unknown", silence_file) is False


def test_is_silenced_cleans_up_expired(silence_file):
    # Write an already-expired entry manually
    past = (datetime.utcnow() - timedelta(minutes=1)).isoformat()
    silence_file.write_text(json.dumps({"old": past}))
    assert is_silenced("old", silence_file) is False
    data = json.loads(silence_file.read_text())
    assert "old" not in data


def test_run_silence_cmd_add(silence_file, capsys):
    args = SimpleNamespace(silence_subcommand="add", pipeline="reports", minutes=60)
    rc = run_silence_cmd(args, silence_file)
    assert rc == 0
    captured = capsys.readouterr()
    assert "reports" in captured.out
    assert "silenced" in captured.out.lower()


def test_run_silence_cmd_check_silenced(silence_file, capsys):
    silence_pipeline("reports", 60, silence_file)
    args = SimpleNamespace(silence_subcommand="check", pipeline="reports")
    rc = run_silence_cmd(args, silence_file)
    assert rc == 0
    captured = capsys.readouterr()
    assert "silenced" in captured.out


def test_run_silence_cmd_unknown_subcommand(silence_file):
    args = SimpleNamespace(silence_subcommand="bogus", pipeline="x")
    rc = run_silence_cmd(args, silence_file)
    assert rc == 2
