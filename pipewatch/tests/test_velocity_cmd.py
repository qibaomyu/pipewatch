"""Tests for velocity_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pipewatch.commands.velocity_cmd import _pipeline_velocity, run_velocity_cmd
from pipewatch.history import HistoryEntry

_NOW = datetime.now(tz=timezone.utc).timestamp()


def _entry(pipeline: str, healthy: bool, age_seconds: float = 0) -> HistoryEntry:
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=_NOW - age_seconds,
        healthy=healthy,
        error_rate=0.0,
        latency=1.0,
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
        "pipewatch.commands.velocity_cmd.RunHistory.all",
        return_value=entries,
    )


# ── unit tests for _pipeline_velocity ────────────────────────────────────────

def test_pipeline_velocity_no_entries():
    result = _pipeline_velocity([], "pipe_a", 24)
    assert result["total_runs"] == 0
    assert result["runs_per_hour"] == 0.0


def test_pipeline_velocity_counts_runs():
    entries = [_entry("pipe_a", True) for _ in range(6)]
    result = _pipeline_velocity(entries, "pipe_a", 24)
    assert result["total_runs"] == 6
    assert result["runs_per_hour"] == pytest.approx(6 / 24, rel=1e-3)


def test_pipeline_velocity_counts_failures():
    entries = [
        _entry("pipe_a", True),
        _entry("pipe_a", False),
        _entry("pipe_a", False),
    ]
    result = _pipeline_velocity(entries, "pipe_a", 24)
    assert result["failed_runs"] == 2


def test_pipeline_velocity_excludes_old_entries():
    entries = [
        _entry("pipe_a", True, age_seconds=100),
        _entry("pipe_a", True, age_seconds=25 * 3600),  # outside 24h window
    ]
    result = _pipeline_velocity(entries, "pipe_a", 24)
    assert result["total_runs"] == 1


def test_pipeline_velocity_filters_by_pipeline():
    entries = [
        _entry("pipe_a", True),
        _entry("pipe_b", True),
    ]
    result = _pipeline_velocity(entries, "pipe_a", 24)
    assert result["total_runs"] == 1


# ── integration tests for run_velocity_cmd ───────────────────────────────────

def test_run_velocity_no_history(capsys):
    with _patch([]):
        rc = run_velocity_cmd(_args())
    assert rc == 0
    assert "No history" in capsys.readouterr().out


def test_run_velocity_text_output(capsys):
    entries = [_entry("pipe_a", True) for _ in range(3)]
    with _patch(entries):
        rc = run_velocity_cmd(_args())
    out = capsys.readouterr().out
    assert rc == 0
    assert "pipe_a" in out
    assert "3" in out


def test_run_velocity_json_output(capsys):
    entries = [_entry("pipe_a", False)]
    with _patch(entries):
        rc = run_velocity_cmd(_args(json=True))
    data = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"
    assert data[0]["failed_runs"] == 1


def test_run_velocity_pipeline_filter(capsys):
    entries = [_entry("pipe_a", True), _entry("pipe_b", True)]
    with _patch(entries):
        rc = run_velocity_cmd(_args(pipeline="pipe_a", json=True))
    data = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert len(data) == 1
    assert data[0]["pipeline"] == "pipe_a"
