"""Tests for backlog_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pipewatch.commands.backlog_cmd import _pipeline_backlog, run_backlog_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool, offset: int = 0) -> HistoryEntry:
    ts = datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc).isoformat()
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts,
        healthy=healthy,
        error_rate=0.0 if healthy else 0.5,
        latency_p99=100.0,
        alerts=[],
    )


def _args(**kwargs):
    defaults = {
        "history_file": ".pipewatch_history.json",
        "pipeline": None,
        "min_consecutive": 3,
        "json": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.backlog_cmd.RunHistory.all",
        return_value=entries,
    )


# ---------------------------------------------------------------------------
# unit: _pipeline_backlog
# ---------------------------------------------------------------------------

def test_pipeline_backlog_no_entries():
    result = _pipeline_backlog([], "pipe_a", min_consecutive=3)
    assert result["current_streak"] == 0
    assert result["in_backlog"] is False


def test_pipeline_backlog_all_healthy():
    entries = [_entry("pipe_a", True, i) for i in range(5)]
    result = _pipeline_backlog(entries, "pipe_a", min_consecutive=3)
    assert result["current_streak"] == 0
    assert result["in_backlog"] is False


def test_pipeline_backlog_detects_streak():
    entries = [
        _entry("pipe_a", True, 0),
        _entry("pipe_a", False, 1),
        _entry("pipe_a", False, 2),
        _entry("pipe_a", False, 3),
    ]
    result = _pipeline_backlog(entries, "pipe_a", min_consecutive=3)
    assert result["current_streak"] == 3
    assert result["in_backlog"] is True


def test_pipeline_backlog_streak_reset_by_healthy():
    entries = [
        _entry("pipe_a", False, 0),
        _entry("pipe_a", False, 1),
        _entry("pipe_a", True, 2),
        _entry("pipe_a", False, 3),
    ]
    result = _pipeline_backlog(entries, "pipe_a", min_consecutive=3)
    assert result["current_streak"] == 1
    assert result["in_backlog"] is False


def test_pipeline_backlog_max_streak_tracked():
    entries = [
        _entry("pipe_a", False, 0),
        _entry("pipe_a", False, 1),
        _entry("pipe_a", False, 2),
        _entry("pipe_a", True, 3),
        _entry("pipe_a", False, 4),
    ]
    result = _pipeline_backlog(entries, "pipe_a", min_consecutive=3)
    assert result["max_streak"] == 3
    assert result["current_streak"] == 1


# ---------------------------------------------------------------------------
# integration: run_backlog_cmd
# ---------------------------------------------------------------------------

def test_run_backlog_cmd_no_entries(capsys):
    with _patch([]):
        rc = run_backlog_cmd(_args())
    assert rc == 0
    assert "No history" in capsys.readouterr().out


def test_run_backlog_cmd_text_output(capsys):
    entries = [_entry("pipe_a", False, i) for i in range(4)]
    with _patch(entries):
        rc = run_backlog_cmd(_args())
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert rc == 0


def test_run_backlog_cmd_exit_code_on_backlog():
    entries = [_entry("pipe_a", False, i) for i in range(4)]
    with _patch(entries):
        rc = run_backlog_cmd(_args(exit_code=True))
    assert rc == 1


def test_run_backlog_cmd_json_output(capsys):
    entries = [_entry("pipe_a", False, i) for i in range(4)]
    with _patch(entries):
        rc = run_backlog_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"
    assert rc == 0


def test_run_backlog_cmd_pipeline_filter(capsys):
    entries = [
        _entry("pipe_a", False, i) for i in range(4)
    ] + [
        _entry("pipe_b", False, i) for i in range(4)
    ]
    with _patch(entries):
        rc = run_backlog_cmd(_args(pipeline="pipe_a", json=True))
    data = json.loads(capsys.readouterr().out)
    assert all(r["pipeline"] == "pipe_a" for r in data)
    assert rc == 0
