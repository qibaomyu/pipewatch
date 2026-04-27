"""Tests for heartbeat_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.heartbeat_cmd import _pipeline_heartbeat, run_heartbeat_cmd


def _entry(pipeline: str, healthy: bool, minutes_ago: float):
    ts = datetime.now(tz=timezone.utc) - timedelta(minutes=minutes_ago)
    e = MagicMock()
    e.pipeline = pipeline
    e.healthy = healthy
    e.timestamp = ts
    return e


def _args(**kwargs):
    defaults = {
        "pipeline": None,
        "json": False,
        "exit_code": False,
        "history_file": ".pipewatch_history.json",
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries):
    mock_history = MagicMock()
    mock_history.all.return_value = entries
    return patch("pipewatch.commands.heartbeat_cmd.RunHistory", return_value=mock_history)


def test_pipeline_heartbeat_no_entries():
    mock_history = MagicMock()
    mock_history.all.return_value = []
    result = _pipeline_heartbeat(mock_history, pipeline=None)
    assert result == []


def test_pipeline_heartbeat_healthy_pipeline():
    now = datetime.now(tz=timezone.utc)
    entries = [_entry("pipe_a", True, 30)]
    mock_history = MagicMock()
    mock_history.all.return_value = entries
    result = _pipeline_heartbeat(mock_history, pipeline=None, now=now)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"
    assert result[0]["status"] == "ok"
    assert abs(result[0]["age_seconds"] - 1800) < 5


def test_pipeline_heartbeat_never_healthy():
    entries = [_entry("pipe_b", False, 10)]
    mock_history = MagicMock()
    mock_history.all.return_value = entries
    result = _pipeline_heartbeat(mock_history, pipeline=None)
    assert result[0]["status"] == "never_healthy"
    assert result[0]["last_healthy"] is None
    assert result[0]["age_seconds"] is None


def test_pipeline_heartbeat_filters_by_pipeline():
    entries = [_entry("pipe_a", True, 5), _entry("pipe_b", True, 10)]
    mock_history = MagicMock()
    mock_history.all.return_value = entries
    result = _pipeline_heartbeat(mock_history, pipeline="pipe_a")
    assert all(r["pipeline"] == "pipe_a" for r in result)


def test_pipeline_heartbeat_picks_most_recent_healthy():
    now = datetime.now(tz=timezone.utc)
    entries = [_entry("pipe_a", True, 60), _entry("pipe_a", True, 10)]
    mock_history = MagicMock()
    mock_history.all.return_value = entries
    result = _pipeline_heartbeat(mock_history, pipeline=None, now=now)
    assert len(result) == 1
    assert abs(result[0]["age_seconds"] - 600) < 5


def test_run_heartbeat_cmd_no_data(capsys):
    with _patch([]) as _:
        code = run_heartbeat_cmd(_args())
    out = capsys.readouterr().out
    assert "No pipeline data" in out
    assert code == 0


def test_run_heartbeat_cmd_text_output(capsys):
    entries = [_entry("pipe_a", True, 5)]
    with _patch(entries):
        code = run_heartbeat_cmd(_args())
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert code == 0


def test_run_heartbeat_cmd_json_output(capsys):
    entries = [_entry("pipe_a", True, 5)]
    with _patch(entries):
        code = run_heartbeat_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"


def test_run_heartbeat_cmd_exit_code_never_healthy():
    entries = [_entry("pipe_b", False, 10)]
    with _patch(entries):
        code = run_heartbeat_cmd(_args(exit_code=True))
    assert code == 1


def test_run_heartbeat_cmd_exit_code_all_ok():
    entries = [_entry("pipe_a", True, 2)]
    with _patch(entries):
        code = run_heartbeat_cmd(_args(exit_code=True))
    assert code == 0
