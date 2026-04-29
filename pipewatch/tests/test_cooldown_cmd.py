"""Tests for cooldown_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.cooldown_cmd import _pipeline_cooldown, run_cooldown_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool, minutes_ago: float) -> HistoryEntry:
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts,
        healthy=healthy,
        error_rate=0.0,
        latency_p99=0.0,
        alerts=[],
    )


def _args(**kwargs):
    defaults = {
        "history_file": ".pipewatch_history.json",
        "pipeline": None,
        "cooldown_minutes": 30,
        "json": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries):
    history = MagicMock()
    history.get_all.return_value = entries
    return patch("pipewatch.commands.cooldown_cmd.RunHistory", return_value=history)


# ---------------------------------------------------------------------------
# Unit tests for _pipeline_cooldown
# ---------------------------------------------------------------------------

def test_pipeline_cooldown_no_entries():
    history = MagicMock()
    history.get_all.return_value = []
    result = _pipeline_cooldown(history, "pipe_a", 30)
    assert result is None


def test_pipeline_cooldown_no_failures():
    history = MagicMock()
    history.get_all.return_value = [
        _entry("pipe_a", True, 10),
        _entry("pipe_a", True, 5),
    ]
    result = _pipeline_cooldown(history, "pipe_a", 30)
    assert result is not None
    assert result["in_cooldown"] is False
    assert result["last_failure"] is None
    assert result["cooldown_remaining_seconds"] == 0


def test_pipeline_cooldown_within_window():
    history = MagicMock()
    history.get_all.return_value = [
        _entry("pipe_a", False, 10),  # failed 10 min ago, cooldown=30
    ]
    result = _pipeline_cooldown(history, "pipe_a", 30)
    assert result["in_cooldown"] is True
    assert result["cooldown_remaining_seconds"] > 0


def test_pipeline_cooldown_outside_window():
    history = MagicMock()
    history.get_all.return_value = [
        _entry("pipe_a", False, 60),  # failed 60 min ago, cooldown=30
    ]
    result = _pipeline_cooldown(history, "pipe_a", 30)
    assert result["in_cooldown"] is False
    assert result["cooldown_remaining_seconds"] == 0


# ---------------------------------------------------------------------------
# Integration tests for run_cooldown_cmd
# ---------------------------------------------------------------------------

def test_run_cooldown_no_entries(capsys):
    with _patch([]) as _:
        rc = run_cooldown_cmd(_args())
    assert rc == 0
    assert "No pipeline history found" in capsys.readouterr().out


def test_run_cooldown_text_output(capsys):
    entries = [_entry("pipe_a", False, 5)]
    with _patch(entries):
        rc = run_cooldown_cmd(_args())
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert rc == 0


def test_run_cooldown_json_output(capsys):
    entries = [_entry("pipe_b", False, 10)]
    with _patch(entries):
        rc = run_cooldown_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_b"


def test_run_cooldown_exit_code_when_in_cooldown():
    entries = [_entry("pipe_a", False, 5)]
    with _patch(entries):
        rc = run_cooldown_cmd(_args(exit_code=True))
    assert rc == 1


def test_run_cooldown_exit_code_zero_when_not_in_cooldown():
    entries = [_entry("pipe_a", False, 60)]
    with _patch(entries):
        rc = run_cooldown_cmd(_args(exit_code=True))
    assert rc == 0


def test_run_cooldown_pipeline_filter(capsys):
    entries = [
        _entry("pipe_a", False, 5),
        _entry("pipe_b", False, 5),
    ]
    with _patch(entries):
        run_cooldown_cmd(_args(pipeline="pipe_a", json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert all(r["pipeline"] == "pipe_a" for r in data)
