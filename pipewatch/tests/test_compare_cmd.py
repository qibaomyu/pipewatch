"""Tests for compare_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pipewatch.commands.compare_cmd import (
    _window_entries,
    _window_stats,
    _pipeline_compare,
    run_compare_cmd,
)
from pipewatch.history import HistoryEntry


def _entry(pipeline="pipe", hours_ago=1, healthy=True, error_rate=0.01, latency=0.5):
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago),
        healthy=healthy,
        error_rate=error_rate,
        latency=latency,
        alerts=[],
    )


def _args(pipeline=None, window=24, fmt="text", history_file="/tmp/h.json"):
    return SimpleNamespace(
        pipeline=pipeline, window=window, format=fmt, history_file=history_file
    )


def test_window_entries_filters_correctly():
    entries = [_entry(hours_ago=1), _entry(hours_ago=30), _entry(hours_ago=50)]
    result = _window_entries(entries, 24, 0)
    assert len(result) == 1


def test_window_stats_empty():
    stats = _window_stats([])
    assert stats["count"] == 0
    assert stats["error_rate"] is None


def test_window_stats_computes_averages():
    entries = [_entry(error_rate=0.1, latency=1.0), _entry(error_rate=0.2, latency=2.0)]
    stats = _window_stats(entries)
    assert stats["count"] == 2
    assert stats["error_rate"] == pytest.approx(0.15, rel=1e-3)
    assert stats["avg_latency"] == pytest.approx(1.5, rel=1e-3)


def test_window_stats_counts_failures():
    entries = [_entry(healthy=True), _entry(healthy=False), _entry(healthy=False)]
    stats = _window_stats(entries)
    assert stats["failures"] == 2


def test_pipeline_compare_structure():
    entries = [_entry(hours_ago=1), _entry(hours_ago=25)]
    result = _pipeline_compare("pipe", entries, 24)
    assert result["pipeline"] == "pipe"
    assert "current" in result
    assert "previous" in result
    assert result["current"]["count"] == 1
    assert result["previous"]["count"] == 1


def test_run_compare_cmd_text(capsys):
    h = MagicMock()
    h.all.return_value = [_entry("alpha", hours_ago=1), _entry("alpha", hours_ago=25)]
    run_compare_cmd(_args(window=24), history=h)
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "Current" in out
    assert "Previous" in out


def test_run_compare_cmd_json(capsys):
    h = MagicMock()
    h.all.return_value = [_entry("beta", hours_ago=2)]
    run_compare_cmd(_args(fmt="json"), history=h)
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "beta"


def test_run_compare_cmd_empty_history(capsys):
    h = MagicMock()
    h.all.return_value = []
    code = run_compare_cmd(_args(), history=h)
    assert code == 0
    assert "No history" in capsys.readouterr().out


def test_run_compare_cmd_pipeline_filter(capsys):
    h = MagicMock()
    h.all.return_value = [_entry("x"), _entry("y")]
    run_compare_cmd(_args(pipeline="x"), history=h)
    out = capsys.readouterr().out
    assert "x" in out
    assert "y" not in out
