"""Tests for trend_cmd."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.commands.trend_cmd import (
    _bucket_entries,
    _trend_symbol,
    _pipeline_trend,
    run_trend_cmd,
)
from pipewatch.history import HistoryEntry


def _entry(pipeline="pipe", error_rate=0.0, latency=1.0, ts=None):
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts or datetime(2024, 1, 1, tzinfo=timezone.utc),
        error_rate=error_rate,
        latency=latency,
        healthy=error_rate < 0.1,
    )


def test_bucket_entries_empty():
    assert _bucket_entries([]) == []


def test_bucket_entries_single():
    result = _bucket_entries([_entry()], buckets=5)
    assert len(result) == 1
    assert result[0]["error_rate"] == 0.0


def test_trend_symbol_up():
    assert _trend_symbol([0.01, 0.05, 0.15]) == "↑"


def test_trend_symbol_down():
    assert _trend_symbol([0.20, 0.10, 0.02]) == "↓"


def test_trend_symbol_flat():
    assert _trend_symbol([0.05, 0.05]) == "→"


def test_trend_symbol_single():
    assert _trend_symbol([0.1]) == "~"


def test_pipeline_trend_output():
    entries = [_entry(error_rate=0.01 * i, latency=float(i)) for i in range(10)]
    out = _pipeline_trend("mypipe", entries, buckets=5)
    assert "mypipe" in out
    assert "error_rate" in out
    assert "latency" in out


@pytest.fixture()
def history_path(tmp_path):
    return tmp_path / "history.json"


def _args(history_path, pipeline=None, buckets=5):
    return argparse.Namespace(
        history_file=str(history_path),
        pipeline=pipeline,
        buckets=buckets,
    )


def test_empty_history_prints_message(history_path, capsys):
    rc = run_trend_cmd(_args(history_path))
    assert rc == 0
    assert "No history" in capsys.readouterr().out


def test_trend_shows_pipelines(history_path, capsys):
    data = [
        {
            "pipeline": "pipe",
            "timestamp": "2024-01-01T00:00:00+00:00",
            "error_rate": 0.05,
            "latency": 2.0,
            "healthy": True,
        }
    ]
    history_path.write_text(json.dumps(data))
    rc = run_trend_cmd(_args(history_path))
    assert rc == 0
    out = capsys.readouterr().out
    assert "pipe" in out


def test_filter_by_pipeline(history_path, capsys):
    data = [
        {"pipeline": "a", "timestamp": "2024-01-01T00:00:00+00:00", "error_rate": 0.0, "latency": 1.0, "healthy": True},
        {"pipeline": "b", "timestamp": "2024-01-01T00:00:00+00:00", "error_rate": 0.5, "latency": 9.0, "healthy": False},
    ]
    history_path.write_text(json.dumps(data))
    rc = run_trend_cmd(_args(history_path, pipeline="a"))
    assert rc == 0
    out = capsys.readouterr().out
    assert "a" in out
    assert "b" not in out
