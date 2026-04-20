"""Tests for pipewatch/commands/cascade_cmd.py"""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.commands.cascade_cmd import _failure_times, _pipeline_cascade, run_cascade_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool, minutes_ago: float = 0) -> HistoryEntry:
    ts = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc) - timedelta(minutes=minutes_ago)
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts,
        healthy=healthy,
        error_rate=0.0 if healthy else 0.5,
        latency_ms=100.0,
        alerts=[],
    )


def _args(**kwargs) -> Namespace:
    defaults = dict(
        lead="pipe_a",
        window=10,
        history_file=".pipewatch_history.json",
        json=False,
    )
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.cascade_cmd.RunHistory.all",
        return_value=entries,
    )


# ---------------------------------------------------------------------------
# unit tests for helpers
# ---------------------------------------------------------------------------

def test_failure_times_returns_only_failed():
    entries = [_entry("pipe_a", True, 5), _entry("pipe_a", False, 3)]
    times = _failure_times(entries, "pipe_a")
    assert len(times) == 1


def test_pipeline_cascade_no_lead_failures():
    entries = [_entry("pipe_a", True, 5), _entry("pipe_b", False, 3)]
    result = _pipeline_cascade(entries, lead="pipe_a", window_minutes=10)
    assert result == []


def test_pipeline_cascade_detects_follower():
    # pipe_a fails at t=0, pipe_b fails 5 minutes later -> within window
    entries = [
        _entry("pipe_a", False, 0),
        _entry("pipe_b", False, -5),  # 5 minutes AFTER pipe_a failure
    ]
    result = _pipeline_cascade(entries, lead="pipe_a", window_minutes=10)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_b"
    assert result[0]["cascade_failures"] == 1
    assert result[0]["cascade_rate"] == 1.0


def test_pipeline_cascade_outside_window():
    # pipe_b fails 20 minutes after pipe_a -> outside 10-minute window
    entries = [
        _entry("pipe_a", False, 0),
        _entry("pipe_b", False, -20),
    ]
    result = _pipeline_cascade(entries, lead="pipe_a", window_minutes=10)
    assert result[0]["cascade_failures"] == 0
    assert result[0]["cascade_rate"] == 0.0


def test_pipeline_cascade_excludes_lead_from_results():
    entries = [_entry("pipe_a", False, 0), _entry("pipe_a", False, -5)]
    result = _pipeline_cascade(entries, lead="pipe_a", window_minutes=10)
    assert all(r["pipeline"] != "pipe_a" for r in result)


# ---------------------------------------------------------------------------
# integration tests via run_cascade_cmd
# ---------------------------------------------------------------------------

def test_run_cascade_cmd_text_output(capsys):
    entries = [_entry("pipe_a", False, 0), _entry("pipe_b", False, -3)]
    with _patch(entries):
        rc = run_cascade_cmd(_args())
    out = capsys.readouterr().out
    assert rc == 0
    assert "pipe_b" in out
    assert "pipe_a" in out


def test_run_cascade_cmd_json_output(capsys):
    entries = [_entry("pipe_a", False, 0), _entry("pipe_b", False, -3)]
    with _patch(entries):
        rc = run_cascade_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["lead"] == "pipe_a"
    assert isinstance(data["cascades"], list)
    assert rc == 0


def test_run_cascade_cmd_no_data_message(capsys):
    entries = [_entry("pipe_a", True, 0)]  # lead has no failures
    with _patch(entries):
        rc = run_cascade_cmd(_args())
    out = capsys.readouterr().out
    assert "No cascade data" in out
    assert rc == 0
