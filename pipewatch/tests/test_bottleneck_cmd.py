"""Tests for bottleneck_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from pipewatch.commands.bottleneck_cmd import _pipeline_bottleneck, run_bottleneck_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, latency: float, offset_hours: int = 1) -> HistoryEntry:
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=datetime.now(timezone.utc) - timedelta(hours=offset_hours),
        healthy=True,
        error_rate=0.0,
        latency=latency,
        alerts=[],
    )


def _args(**kwargs):
    defaults = {
        "hours": 24,
        "pipeline": None,
        "json": False,
        "history_file": ".pipewatch_history.json",
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.bottleneck_cmd.RunHistory.all", return_value=entries)


def test_pipeline_bottleneck_no_entries():
    result = _pipeline_bottleneck([], "pipe_a", 24)
    assert result["avg_latency"] is None
    assert result["count"] == 0


def test_pipeline_bottleneck_computes_avg():
    entries = [_entry("pipe_a", 1.0), _entry("pipe_a", 3.0)]
    result = _pipeline_bottleneck(entries, "pipe_a", 24)
    assert result["avg_latency"] == 2.0
    assert result["max_latency"] == 3.0
    assert result["count"] == 2


def test_pipeline_bottleneck_ignores_old_entries():
    entries = [_entry("pipe_a", 5.0, offset_hours=48)]
    result = _pipeline_bottleneck(entries, "pipe_a", 24)
    assert result["count"] == 0


def test_pipeline_bottleneck_ignores_none_latency():
    e = _entry("pipe_a", 0.0)
    e = HistoryEntry(
        pipeline="pipe_a",
        timestamp=datetime.now(timezone.utc),
        healthy=True,
        error_rate=0.0,
        latency=None,
        alerts=[],
    )
    result = _pipeline_bottleneck([e], "pipe_a", 24)
    assert result["count"] == 0


def test_run_bottleneck_no_data(capsys):
    with _patch([]):
        code = run_bottleneck_cmd(_args())
    assert code == 0
    assert "No latency data" in capsys.readouterr().out


def test_run_bottleneck_text_output(capsys):
    entries = [_entry("pipe_a", 2.5), _entry("pipe_b", 0.5)]
    with _patch(entries):
        code = run_bottleneck_cmd(_args())
    assert code == 0
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "pipe_b" in out


def test_run_bottleneck_json_output(capsys):
    entries = [_entry("pipe_a", 1.0)]
    with _patch(entries):
        code = run_bottleneck_cmd(_args(json=True))
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert data[0]["pipeline"] == "pipe_a"
    assert data[0]["avg_latency"] == 1.0


def test_run_bottleneck_sorted_by_avg_desc(capsys):
    entries = [_entry("fast", 0.1), _entry("slow", 9.9)]
    with _patch(entries):
        run_bottleneck_cmd(_args(json=True))
    data = json.loads(capsys.readouterr().out)
    assert data[0]["pipeline"] == "slow"
