"""Tests for aging_cmd."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.aging_cmd import _pipeline_aging, run_aging_cmd


def _entry(pipeline: str, healthy: bool, minutes_ago: float):
    e = MagicMock()
    e.pipeline = pipeline
    e.healthy = healthy
    e.timestamp = datetime.now(tz=timezone.utc) - timedelta(minutes=minutes_ago)
    return e


def _args(**kwargs):
    defaults = {
        "pipeline": None,
        "history_file": ".pipewatch_history.json",
        "json": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.aging_cmd.RunHistory", return_value=MagicMock(all=MagicMock(return_value=entries)))


# ---------------------------------------------------------------------------
# _pipeline_aging unit tests
# ---------------------------------------------------------------------------

def test_pipeline_aging_no_entries():
    history = MagicMock(all=MagicMock(return_value=[]))
    assert _pipeline_aging(history, None) == []


def test_pipeline_aging_single_healthy_pipeline():
    entries = [_entry("pipe_a", True, 30), _entry("pipe_a", True, 10)]
    history = MagicMock(all=MagicMock(return_value=entries))
    rows = _pipeline_aging(history, None)
    assert len(rows) == 1
    assert rows[0]["pipeline"] == "pipe_a"
    assert rows[0]["state"] == "healthy"
    assert rows[0]["duration_seconds"] >= 0


def test_pipeline_aging_detects_failing_state():
    entries = [_entry("pipe_b", True, 60), _entry("pipe_b", False, 5)]
    history = MagicMock(all=MagicMock(return_value=entries))
    rows = _pipeline_aging(history, None)
    assert rows[0]["state"] == "failing"
    # Duration should be ~5 minutes
    assert rows[0]["duration_seconds"] < 400


def test_pipeline_aging_filters_by_pipeline():
    entries = [
        _entry("pipe_a", True, 10),
        _entry("pipe_b", False, 10),
    ]
    history = MagicMock(all=MagicMock(return_value=entries))
    rows = _pipeline_aging(history, "pipe_a")
    assert len(rows) == 1
    assert rows[0]["pipeline"] == "pipe_a"


def test_pipeline_aging_streak_extends_across_entries():
    entries = [
        _entry("pipe_c", False, 90),
        _entry("pipe_c", False, 60),
        _entry("pipe_c", False, 30),
    ]
    history = MagicMock(all=MagicMock(return_value=entries))
    rows = _pipeline_aging(history, None)
    # Streak started 90 min ago; duration should be ~5400 s
    assert rows[0]["duration_seconds"] >= 5000


# ---------------------------------------------------------------------------
# run_aging_cmd integration tests
# ---------------------------------------------------------------------------

def test_run_aging_cmd_text_output(capsys):
    entries = [_entry("pipe_a", True, 20)]
    with _patch(entries):
        rc = run_aging_cmd(_args())
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "healthy" in out
    assert rc == 0


def test_run_aging_cmd_json_output(capsys):
    entries = [_entry("pipe_a", False, 15)]
    with _patch(entries):
        rc = run_aging_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["state"] == "failing"
    assert rc == 0


def test_run_aging_cmd_exit_code_failing():
    entries = [_entry("pipe_a", False, 5)]
    with _patch(entries):
        rc = run_aging_cmd(_args(exit_code=True))
    assert rc == 1


def test_run_aging_cmd_exit_code_healthy():
    entries = [_entry("pipe_a", True, 5)]
    with _patch(entries):
        rc = run_aging_cmd(_args(exit_code=True))
    assert rc == 0


def test_run_aging_cmd_no_entries(capsys):
    with _patch([]):
        rc = run_aging_cmd(_args())
    out = capsys.readouterr().out
    assert "No history" in out
    assert rc == 0
