"""Tests for frequency_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.commands.frequency_cmd import _pipeline_frequency, run_frequency_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, minutes_ago: float, healthy: bool = True) -> HistoryEntry:
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts,
        healthy=healthy,
        error_rate=0.0,
        latency=1.0,
        alerts=[],
    )


def _args(**kwargs):
    defaults = {
        "hours": 24,
        "pipeline": None,
        "json": False,
        "history_file": ".pipewatch_history.json",
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    mock_history = MagicMock()
    mock_history.all.return_value = entries
    return patch("pipewatch.commands.frequency_cmd.RunHistory", return_value=mock_history)


def test_pipeline_frequency_no_entries():
    mock_history = MagicMock()
    mock_history.all.return_value = []
    result = _pipeline_frequency(mock_history, hours=24)
    assert result == []


def test_pipeline_frequency_single_run():
    mock_history = MagicMock()
    mock_history.all.return_value = [_entry("pipe_a", 30)]
    result = _pipeline_frequency(mock_history, hours=24)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"
    assert result[0]["run_count"] == 1
    assert result[0]["avg_interval_minutes"] is None


def test_pipeline_frequency_multiple_runs():
    mock_history = MagicMock()
    mock_history.all.return_value = [
        _entry("pipe_a", 120),
        _entry("pipe_a", 60),
        _entry("pipe_a", 0),
    ]
    result = _pipeline_frequency(mock_history, hours=24)
    assert result[0]["run_count"] == 3
    assert result[0]["avg_interval_minutes"] == pytest.approx(60.0, abs=1.0)


def test_pipeline_frequency_filters_by_pipeline():
    mock_history = MagicMock()
    mock_history.all.return_value = [
        _entry("pipe_a", 30),
        _entry("pipe_b", 30),
    ]
    result = _pipeline_frequency(mock_history, hours=24, pipeline="pipe_a")
    assert all(r["pipeline"] == "pipe_a" for r in result)


def test_run_frequency_cmd_no_data(capsys):
    with _patch([]):
        code = run_frequency_cmd(_args())
    assert code == 0
    assert "No data" in capsys.readouterr().out


def test_run_frequency_cmd_text_output(capsys):
    entries = [_entry("pipe_a", 60), _entry("pipe_a", 0)]
    with _patch(entries):
        code = run_frequency_cmd(_args())
    assert code == 0
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "2" in out


def test_run_frequency_cmd_json_output(capsys):
    entries = [_entry("pipe_a", 60), _entry("pipe_a", 0)]
    with _patch(entries):
        code = run_frequency_cmd(_args(json=True))
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"
