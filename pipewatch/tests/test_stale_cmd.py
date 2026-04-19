"""Tests for stale_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.commands.stale_cmd import _pipeline_stale, run_stale_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, minutes_ago: float) -> HistoryEntry:
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return HistoryEntry(pipeline=pipeline, timestamp=ts, healthy=True, error_rate=0.0, latency=1.0, alerts=[])


def _args(**kwargs):
    defaults = {
        "pipeline": None,
        "threshold": 60,
        "history_file": ".pipewatch_history.json",
        "json": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries_by_pipeline: dict):
    mock_history = MagicMock()
    mock_history.pipelines.return_value = list(entries_by_pipeline.keys())
    mock_history.get.side_effect = lambda p: entries_by_pipeline.get(p, [])
    return patch("pipewatch.commands.stale_cmd.RunHistory", return_value=mock_history)


def test_pipeline_stale_no_entries():
    mock_history = MagicMock()
    mock_history.get.return_value = []
    result = _pipeline_stale(mock_history, "pipe_a", 60)
    assert result["stale"] is True
    assert result["last_seen"] is None


def test_pipeline_stale_recent():
    mock_history = MagicMock()
    mock_history.get.return_value = [_entry("pipe_a", 10)]
    result = _pipeline_stale(mock_history, "pipe_a", 60)
    assert result["stale"] is False
    assert result["minutes_since"] < 60


def test_pipeline_stale_old():
    mock_history = MagicMock()
    mock_history.get.return_value = [_entry("pipe_a", 120)]
    result = _pipeline_stale(mock_history, "pipe_a", 60)
    assert result["stale"] is True


def test_run_stale_cmd_no_history(capsys):
    with _patch({}):
        rc = run_stale_cmd(_args())
    assert rc == 0
    assert "No history" in capsys.readouterr().out


def test_run_stale_cmd_text_output(capsys):
    entries = {"pipe_a": [_entry("pipe_a", 10)]}
    with _patch(entries):
        rc = run_stale_cmd(_args())
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert rc == 0


def test_run_stale_cmd_json_output(capsys):
    entries = {"pipe_a": [_entry("pipe_a", 10)]}
    with _patch(entries):
        rc = run_stale_cmd(_args(json=True))
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"


def test_run_stale_exit_code_when_stale():
    entries = {"pipe_a": [_entry("pipe_a", 120)]}
    with _patch(entries):
        rc = run_stale_cmd(_args(exit_code=True))
    assert rc == 1


def test_run_stale_no_exit_code_flag():
    entries = {"pipe_a": [_entry("pipe_a", 120)]}
    with _patch(entries):
        rc = run_stale_cmd(_args(exit_code=False))
    assert rc == 0
