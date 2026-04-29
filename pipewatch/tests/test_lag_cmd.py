"""Tests for pipewatch.commands.lag_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pipewatch.commands.lag_cmd import _pipeline_lag, run_lag_cmd


NOW = datetime.now(tz=timezone.utc).timestamp()


def _entry(pipeline: str, lag: float | None, offset_seconds: float = 0):
    """Build a minimal mock HistoryEntry."""
    from types import SimpleNamespace
    return SimpleNamespace(
        pipeline=pipeline,
        timestamp=NOW - offset_seconds,
        lag_seconds=lag,
        healthy=True,
    )


def _args(**kwargs):
    defaults = dict(
        hours=24.0,
        pipeline=None,
        json=False,
        history_file=".pipewatch_history.json",
    )
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.lag_cmd.RunHistory",
        return_value=type("H", (), {"all": lambda self: entries})(),
    )


# ---------------------------------------------------------------------------
# Unit tests for _pipeline_lag
# ---------------------------------------------------------------------------

def test_pipeline_lag_no_entries():
    result = _pipeline_lag([], "etl", 24)
    assert result is None


def test_pipeline_lag_none_lag_values_excluded():
    entries = [_entry("etl", None)]
    result = _pipeline_lag(entries, "etl", 24)
    assert result is None


def test_pipeline_lag_computes_stats():
    entries = [
        _entry("etl", 10.0),
        _entry("etl", 20.0),
        _entry("etl", 30.0),
    ]
    result = _pipeline_lag(entries, "etl", 24)
    assert result is not None
    assert result["count"] == 3
    assert result["avg_lag"] == 20.0
    assert result["min_lag"] == 10.0
    assert result["max_lag"] == 30.0


def test_pipeline_lag_excludes_old_entries():
    entries = [
        _entry("etl", 50.0, offset_seconds=90000),  # older than 24 h
        _entry("etl", 10.0),
    ]
    result = _pipeline_lag(entries, "etl", 24)
    assert result["count"] == 1
    assert result["avg_lag"] == 10.0


# ---------------------------------------------------------------------------
# Integration tests for run_lag_cmd
# ---------------------------------------------------------------------------

def test_run_lag_cmd_no_data(capsys):
    with _patch([]):
        rc = run_lag_cmd(_args())
    assert rc == 0
    assert "No lag data found" in capsys.readouterr().out


def test_run_lag_cmd_text_output(capsys):
    entries = [_entry("pipe_a", 5.0), _entry("pipe_a", 15.0)]
    with _patch(entries):
        rc = run_lag_cmd(_args())
    out = capsys.readouterr().out
    assert rc == 0
    assert "pipe_a" in out
    assert "10.00" in out  # avg


def test_run_lag_cmd_json_output(capsys):
    entries = [_entry("pipe_b", 8.0)]
    with _patch(entries):
        rc = run_lag_cmd(_args(json=True))
    out = capsys.readouterr().out
    assert rc == 0
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_b"
    assert data[0]["avg_lag"] == 8.0


def test_run_lag_cmd_pipeline_filter(capsys):
    entries = [_entry("pipe_a", 5.0), _entry("pipe_b", 99.0)]
    with _patch(entries):
        rc = run_lag_cmd(_args(pipeline="pipe_a"))
    out = capsys.readouterr().out
    assert rc == 0
    assert "pipe_a" in out
    assert "pipe_b" not in out
