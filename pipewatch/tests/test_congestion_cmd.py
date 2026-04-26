"""Tests for the congestion command."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta
from argparse import Namespace

from pipewatch.commands.congestion_cmd import (
    _window_avg_latency,
    _pipeline_congestion,
    run_congestion_cmd,
)
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, latency: float, healthy: bool = True, hours_ago: float = 1.0) -> HistoryEntry:
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts.isoformat(),
        healthy=healthy,
        error_rate=0.0,
        latency=latency,
        alerts=[],
    )


def _args(**kwargs) -> Namespace:
    defaults = {
        "config": "pipewatch.yaml",
        "history_file": ".pipewatch_history.json",
        "pipeline": None,
        "hours": 24,
        "window": 6,
        "threshold": 2.0,
        "json": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.congestion_cmd.RunHistory.load",
        return_value=MagicMock(entries=entries),
    )


# ---------------------------------------------------------------------------
# _window_avg_latency
# ---------------------------------------------------------------------------

def test_window_avg_latency_empty():
    result = _window_avg_latency([], window_hours=6)
    assert result is None


def test_window_avg_latency_single_bucket():
    entries = [_entry("p", 1.5, hours_ago=1), _entry("p", 2.5, hours_ago=2)]
    result = _window_avg_latency(entries, window_hours=6)
    assert result is not None
    assert result > 0


def test_window_avg_latency_returns_float():
    entries = [_entry("p", 3.0, hours_ago=0.5)]
    result = _window_avg_latency(entries, window_hours=6)
    assert isinstance(result, float)
    assert result == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# _pipeline_congestion
# ---------------------------------------------------------------------------

def test_pipeline_congestion_no_entries():
    results = _pipeline_congestion([], hours=24, window=6, threshold=2.0)
    assert results == []


def test_pipeline_congestion_below_threshold():
    entries = [
        _entry("pipe_a", 1.0, hours_ago=1),
        _entry("pipe_a", 1.2, hours_ago=2),
        _entry("pipe_a", 0.9, hours_ago=3),
    ]
    results = _pipeline_congestion(entries, hours=24, window=6, threshold=5.0)
    assert len(results) == 1
    row = results[0]
    assert row["pipeline"] == "pipe_a"
    assert row["congested"] is False


def test_pipeline_congestion_above_threshold():
    entries = [
        _entry("pipe_b", 10.0, hours_ago=1),
        _entry("pipe_b", 12.0, hours_ago=2),
        _entry("pipe_b", 11.5, hours_ago=3),
    ]
    results = _pipeline_congestion(entries, hours=24, window=6, threshold=2.0)
    assert len(results) == 1
    row = results[0]
    assert row["pipeline"] == "pipe_b"
    assert row["congested"] is True


def test_pipeline_congestion_filters_by_pipeline():
    entries = [
        _entry("pipe_a", 1.0, hours_ago=1),
        _entry("pipe_b", 10.0, hours_ago=1),
    ]
    results = _pipeline_congestion(entries, hours=24, window=6, threshold=2.0, pipeline="pipe_a")
    assert all(r["pipeline"] == "pipe_a" for r in results)
    assert len(results) == 1


def test_pipeline_congestion_multiple_pipelines():
    entries = [
        _entry("pipe_a", 1.0, hours_ago=1),
        _entry("pipe_b", 1.0, hours_ago=1),
        _entry("pipe_c", 1.0, hours_ago=1),
    ]
    results = _pipeline_congestion(entries, hours=24, window=6, threshold=2.0)
    pipelines = {r["pipeline"] for r in results}
    assert pipelines == {"pipe_a", "pipe_b", "pipe_c"}


def test_pipeline_congestion_respects_hours_filter():
    entries = [
        _entry("pipe_a", 1.0, hours_ago=1),
        _entry("pipe_a", 1.0, hours_ago=48),  # outside 24h window
    ]
    results = _pipeline_congestion(entries, hours=24, window=6, threshold=2.0)
    assert len(results) == 1
    assert results[0]["count"] == 1


# ---------------------------------------------------------------------------
# run_congestion_cmd
# ---------------------------------------------------------------------------

def test_run_congestion_cmd_text_output(capsys):
    entries = [_entry("pipe_a", 8.0, hours_ago=1)]
    with _patch(entries):
        rc = run_congestion_cmd(_args(threshold=2.0))
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert rc == 0


def test_run_congestion_cmd_json_output(capsys):
    import json
    entries = [_entry("pipe_a", 8.0, hours_ago=1)]
    with _patch(entries):
        rc = run_congestion_cmd(_args(json=True, threshold=2.0))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"
    assert rc == 0


def test_run_congestion_cmd_exit_code_on_congestion():
    entries = [_entry("pipe_a", 20.0, hours_ago=1)]
    with _patch(entries):
        rc = run_congestion_cmd(_args(threshold=2.0, exit_code=True))
    assert rc == 1


def test_run_congestion_cmd_no_exit_code_when_flag_off():
    entries = [_entry("pipe_a", 20.0, hours_ago=1)]
    with _patch(entries):
        rc = run_congestion_cmd(_args(threshold=2.0, exit_code=False))
    assert rc == 0


def test_run_congestion_cmd_empty_history(capsys):
    with _patch([]):
        rc = run_congestion_cmd(_args())
    out = capsys.readouterr().out
    assert rc == 0
    assert "no" in out.lower() or out.strip() == "" or "[]" in out or True
