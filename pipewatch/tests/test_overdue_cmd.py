"""Tests for pipewatch/commands/overdue_cmd.py"""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.overdue_cmd import _pipeline_overdue, run_overdue_cmd


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _entry(pipeline: str, hours_ago: float):
    e = MagicMock()
    e.pipeline = pipeline
    e.timestamp = NOW - timedelta(hours=hours_ago)
    return e


def _args(**kwargs):
    defaults = {
        "pipeline": None,
        "max_hours": 24.0,
        "json": False,
        "exit_code": False,
        "history_file": ".pipewatch_history.json",
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    history = MagicMock()
    history.all.return_value = entries
    return patch("pipewatch.commands.overdue_cmd.RunHistory", return_value=history)


# ---------------------------------------------------------------------------
# _pipeline_overdue unit tests
# ---------------------------------------------------------------------------

def test_pipeline_overdue_no_entries():
    history = MagicMock()
    history.all.return_value = []
    result = _pipeline_overdue(history, pipeline=None, max_hours=24.0, now=NOW)
    assert result == []


def test_pipeline_overdue_recent_run_not_reported():
    history = MagicMock()
    history.all.return_value = [_entry("pipe_a", hours_ago=10.0)]
    result = _pipeline_overdue(history, pipeline=None, max_hours=24.0, now=NOW)
    assert result == []


def test_pipeline_overdue_old_run_reported():
    history = MagicMock()
    history.all.return_value = [_entry("pipe_a", hours_ago=30.0)]
    result = _pipeline_overdue(history, pipeline=None, max_hours=24.0, now=NOW)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"
    assert result[0]["overdue"] is True
    assert result[0]["age_hours"] == pytest.approx(30.0, abs=0.01)


def test_pipeline_overdue_filters_by_pipeline():
    history = MagicMock()
    history.all.return_value = [
        _entry("pipe_a", hours_ago=50.0),
        _entry("pipe_b", hours_ago=50.0),
    ]
    result = _pipeline_overdue(history, pipeline="pipe_a", max_hours=24.0, now=NOW)
    names = [r["pipeline"] for r in result]
    assert "pipe_a" in names
    assert "pipe_b" not in names


def test_pipeline_overdue_uses_latest_entry():
    """Only the most-recent run per pipeline should be evaluated."""
    history = MagicMock()
    # One old entry and one recent entry for the same pipeline
    history.all.return_value = [
        _entry("pipe_a", hours_ago=100.0),
        _entry("pipe_a", hours_ago=5.0),
    ]
    result = _pipeline_overdue(history, pipeline=None, max_hours=24.0, now=NOW)
    assert result == []  # recent run means not overdue


# ---------------------------------------------------------------------------
# run_overdue_cmd integration tests
# ---------------------------------------------------------------------------

def test_run_overdue_cmd_no_results_prints_message(capsys):
    with _patch([_entry("pipe_a", hours_ago=2.0)]):
        rc = run_overdue_cmd(_args())
    out = capsys.readouterr().out
    assert "No overdue pipelines" in out
    assert rc == 0


def test_run_overdue_cmd_text_output(capsys):
    with _patch([_entry("pipe_a", hours_ago=48.0)]):
        rc = run_overdue_cmd(_args())
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert rc == 0


def test_run_overdue_cmd_json_output(capsys):
    with _patch([_entry("pipe_a", hours_ago=48.0)]):
        rc = run_overdue_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"


def test_run_overdue_cmd_exit_code_flag():
    with _patch([_entry("pipe_a", hours_ago=48.0)]):
        rc = run_overdue_cmd(_args(exit_code=True))
    assert rc == 1


def test_run_overdue_cmd_no_exit_code_when_healthy():
    with _patch([_entry("pipe_a", hours_ago=2.0)]):
        rc = run_overdue_cmd(_args(exit_code=True))
    assert rc == 0
