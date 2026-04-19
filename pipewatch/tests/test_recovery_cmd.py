"""Tests for recovery_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from unittest.mock import patch

import pytest

from pipewatch.commands.recovery_cmd import _pipeline_recovery, run_recovery_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool, ts: str) -> HistoryEntry:
    return HistoryEntry(pipeline=pipeline, healthy=healthy, timestamp=ts,
                        error_rate=0.0, latency=0.0, alerts=[])


def _args(**kwargs):
    defaults = {"history_file": ".pipewatch_history.json", "pipeline": None, "json": False}
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.recovery_cmd.RunHistory.all", return_value=entries)


# --- unit tests for _pipeline_recovery ---

def test_pipeline_recovery_no_events():
    entries = [_entry("p", True, "2024-01-01T00:00:00"), _entry("p", True, "2024-01-01T01:00:00")]
    result = _pipeline_recovery(entries)
    assert result["recoveries"] == 0
    assert result["timestamps"] == []


def test_pipeline_recovery_single():
    entries = [
        _entry("p", True, "2024-01-01T00:00:00"),
        _entry("p", False, "2024-01-01T01:00:00"),
        _entry("p", True, "2024-01-01T02:00:00"),
    ]
    result = _pipeline_recovery(entries)
    assert result["recoveries"] == 1
    assert "2024-01-01T02:00:00" in result["timestamps"]


def test_pipeline_recovery_multiple():
    entries = [
        _entry("p", False, "2024-01-01T00:00:00"),
        _entry("p", True, "2024-01-01T01:00:00"),
        _entry("p", False, "2024-01-01T02:00:00"),
        _entry("p", True, "2024-01-01T03:00:00"),
    ]
    result = _pipeline_recovery(entries)
    assert result["recoveries"] == 2


# --- integration tests for run_recovery_cmd ---

def test_empty_history_prints_message(capsys):
    with _patch([]):
        code = run_recovery_cmd(_args())
    assert code == 0
    assert "No history" in capsys.readouterr().out


def test_text_output(capsys):
    entries = [
        _entry("pipe_a", False, "2024-01-01T00:00:00"),
        _entry("pipe_a", True, "2024-01-01T01:00:00"),
    ]
    with _patch(entries):
        code = run_recovery_cmd(_args())
    out = capsys.readouterr().out
    assert code == 0
    assert "pipe_a" in out
    assert "1" in out


def test_json_output(capsys):
    entries = [_entry("pipe_a", False, "2024-01-01T00:00:00"),
               _entry("pipe_a", True, "2024-01-01T01:00:00")]
    with _patch(entries):
        code = run_recovery_cmd(_args(json=True))
    data = json.loads(capsys.readouterr().out)
    assert code == 0
    assert data[0]["pipeline"] == "pipe_a"
    assert data[0]["recoveries"] == 1


def test_filter_by_pipeline(capsys):
    entries = [
        _entry("pipe_a", False, "2024-01-01T00:00:00"),
        _entry("pipe_a", True, "2024-01-01T01:00:00"),
        _entry("pipe_b", False, "2024-01-01T00:00:00"),
        _entry("pipe_b", True, "2024-01-01T01:00:00"),
    ]
    with _patch(entries):
        run_recovery_cmd(_args(pipeline="pipe_a", json=True))
    data = json.loads(capsys.readouterr().out)
    assert all(r["pipeline"] == "pipe_a" for r in data)
