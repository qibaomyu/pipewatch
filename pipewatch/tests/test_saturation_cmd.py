"""Tests for saturation_cmd."""
from __future__ import annotations

import argparse
import datetime
import json
from unittest.mock import patch

import pytest

from pipewatch.commands.saturation_cmd import _pipeline_saturation, run_saturation_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool = True, minutes_ago: float = 10.0) -> HistoryEntry:
    ts = datetime.datetime.utcnow() - datetime.timedelta(minutes=minutes_ago)
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts,
        healthy=healthy,
        error_rate=0.0,
        latency_ms=100.0,
        alert_count=0,
    )


def _args(**kwargs) -> argparse.Namespace:
    defaults = dict(
        hours=24.0,
        limit=10,
        pipeline=None,
        json=False,
        exit_code=False,
        history_file=".pipewatch_history.json",
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.saturation_cmd.RunHistory.all", return_value=entries)


def test_pipeline_saturation_no_entries():
    result = _pipeline_saturation([], "pipe_a", 24.0, 10)
    assert result["pipeline"] == "pipe_a"
    assert result["runs"] == 0
    assert result["saturation_pct"] == 0.0
    assert result["saturated"] is False


def test_pipeline_saturation_under_limit():
    entries = [_entry("pipe_a") for _ in range(5)]
    result = _pipeline_saturation(entries, "pipe_a", 24.0, 10)
    assert result["runs"] == 5
    assert result["saturation_pct"] == 50.0
    assert result["saturated"] is False


def test_pipeline_saturation_at_limit():
    entries = [_entry("pipe_a") for _ in range(10)]
    result = _pipeline_saturation(entries, "pipe_a", 24.0, 10)
    assert result["runs"] == 10
    assert result["saturation_pct"] == 100.0
    assert result["saturated"] is True


def test_pipeline_saturation_filters_old_entries():
    fresh = [_entry("pipe_a", minutes_ago=30)]
    old = [_entry("pipe_a", minutes_ago=120)]
    result = _pipeline_saturation(fresh + old, "pipe_a", 1.0, 10)
    assert result["runs"] == 1


def test_run_saturation_cmd_text_output(capsys):
    entries = [_entry("pipe_a") for _ in range(3)]
    with _patch(entries):
        code = run_saturation_cmd(_args(pipeline="pipe_a", limit=10))
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert code == 0


def test_run_saturation_cmd_json_output(capsys):
    entries = [_entry("pipe_a") for _ in range(5)]
    with _patch(entries):
        code = run_saturation_cmd(_args(pipeline="pipe_a", limit=10, json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data[0]["runs"] == 5
    assert code == 0


def test_run_saturation_cmd_exit_code_when_saturated():
    entries = [_entry("pipe_a") for _ in range(10)]
    with _patch(entries):
        code = run_saturation_cmd(_args(pipeline="pipe_a", limit=10, exit_code=True))
    assert code == 1


def test_run_saturation_cmd_no_entries_message(capsys):
    with _patch([]):
        code = run_saturation_cmd(_args())
    out = capsys.readouterr().out
    assert "No history" in out
    assert code == 0
