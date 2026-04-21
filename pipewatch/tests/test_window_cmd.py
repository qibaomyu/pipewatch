"""Tests for pipewatch/commands/window_cmd.py"""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.commands.window_cmd import _pipeline_window, run_window_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool, error_rate: float = 0.0,
           latency_ms: float = 100.0, hours_ago: float = 1.0) -> HistoryEntry:
    ts = datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
    e = MagicMock(spec=HistoryEntry)
    e.pipeline = pipeline
    e.healthy = healthy
    e.error_rate = error_rate
    e.latency_ms = latency_ms
    e.timestamp = ts
    return e


def _args(**kwargs) -> Namespace:
    defaults = dict(hours=24, pipeline=None, json=False,
                    history_file=".pipewatch_history.json")
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.window_cmd.RunHistory",
                 return_value=MagicMock(all=MagicMock(return_value=entries)))


def test_pipeline_window_no_entries():
    result = _pipeline_window([], "pipe_a", 24)
    assert result["runs"] == 0
    assert result["avg_error_rate"] is None
    assert result["avg_latency_ms"] is None


def test_pipeline_window_all_healthy():
    entries = [_entry("pipe_a", True, 0.01, 200.0) for _ in range(3)]
    result = _pipeline_window(entries, "pipe_a", 24)
    assert result["runs"] == 3
    assert result["healthy"] == 3
    assert result["failing"] == 0
    assert result["avg_error_rate"] == pytest.approx(0.01)
    assert result["avg_latency_ms"] == pytest.approx(200.0)


def test_pipeline_window_mixed():
    entries = [
        _entry("pipe_a", True, 0.0, 100.0),
        _entry("pipe_a", False, 0.5, 300.0),
    ]
    result = _pipeline_window(entries, "pipe_a", 24)
    assert result["healthy"] == 1
    assert result["failing"] == 1
    assert result["avg_error_rate"] == pytest.approx(0.25)
    assert result["avg_latency_ms"] == pytest.approx(200.0)


def test_pipeline_window_filters_outside_window():
    old = _entry("pipe_a", False, 1.0, 500.0, hours_ago=48)
    recent = _entry("pipe_a", True, 0.0, 100.0, hours_ago=1)
    result = _pipeline_window([old, recent], "pipe_a", 24)
    assert result["runs"] == 1
    assert result["healthy"] == 1


def test_run_window_cmd_no_entries(capsys):
    with _patch([]):
        code = run_window_cmd(_args())
    assert code == 0
    assert "No history" in capsys.readouterr().out


def test_run_window_cmd_text_output(capsys):
    entries = [_entry("alpha", True, 0.02, 150.0)]
    with _patch(entries):
        code = run_window_cmd(_args())
    assert code == 0
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "1" in out  # one run


def test_run_window_cmd_json_output(capsys):
    entries = [_entry("beta", False, 0.4, 800.0)]
    with _patch(entries):
        code = run_window_cmd(_args(json=True))
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "beta"
    assert data[0]["failing"] == 1


def test_run_window_cmd_pipeline_filter(capsys):
    entries = [
        _entry("alpha", True),
        _entry("beta", False),
    ]
    with _patch(entries):
        code = run_window_cmd(_args(pipeline="alpha", json=True))
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert len(data) == 1
    assert data[0]["pipeline"] == "alpha"
