"""Tests for throughput_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from pipewatch.commands.throughput_cmd import _pipeline_throughput, run_throughput_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool, hours_ago: float = 1.0) -> HistoryEntry:
    ts = datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts,
        healthy=healthy,
        error_rate=0.0,
        latency_ms=100.0,
        alerts=[],
    )


def _args(**kwargs) -> Namespace:
    defaults = {
        "hours": 24,
        "pipeline": None,
        "json": False,
        "history_file": ".pipewatch_history.json",
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.throughput_cmd.RunHistory.all",
        return_value=entries,
    )


def test_pipeline_throughput_no_entries():
    result = _pipeline_throughput([], "pipe_a", 24)
    assert result["total_runs"] == 0
    assert result["runs_per_hour"] == 0.0
    assert result["failed_runs"] == 0


def test_pipeline_throughput_counts_all_runs():
    entries = [_entry("pipe_a", True, 1), _entry("pipe_a", True, 2), _entry("pipe_a", False, 3)]
    result = _pipeline_throughput(entries, "pipe_a", 24)
    assert result["total_runs"] == 3
    assert result["failed_runs"] == 1
    assert result["runs_per_hour"] == pytest.approx(3 / 24, rel=1e-3)


def test_pipeline_throughput_excludes_old_entries():
    entries = [
        _entry("pipe_a", True, 1),
        _entry("pipe_a", True, 25),  # outside 24h window
    ]
    result = _pipeline_throughput(entries, "pipe_a", 24)
    assert result["total_runs"] == 1


def test_pipeline_throughput_filters_by_pipeline():
    entries = [_entry("pipe_a", True, 1), _entry("pipe_b", True, 1)]
    result = _pipeline_throughput(entries, "pipe_a", 24)
    assert result["pipeline"] == "pipe_a"
    assert result["total_runs"] == 1


def test_run_throughput_cmd_text_output(capsys):
    entries = [_entry("pipe_a", True, 1), _entry("pipe_a", False, 2)]
    with _patch(entries):
        code = run_throughput_cmd(_args())
    captured = capsys.readouterr().out
    assert code == 0
    assert "pipe_a" in captured
    assert "Runs/hr" in captured


def test_run_throughput_cmd_json_output(capsys):
    entries = [_entry("pipe_a", True, 1)]
    with _patch(entries):
        code = run_throughput_cmd(_args(json=True))
    captured = capsys.readouterr().out
    assert code == 0
    data = json.loads(captured)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"
    assert "runs_per_hour" in data[0]


def test_run_throughput_cmd_no_entries_message(capsys):
    with _patch([]):
        code = run_throughput_cmd(_args())
    captured = capsys.readouterr().out
    assert code == 0
    assert "No history" in captured
