"""Tests for latency_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from pipewatch.commands.latency_cmd import _pipeline_latency, _format_text, run_latency_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, latency_ms: float, hours_ago: float = 1) -> HistoryEntry:
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return HistoryEntry(pipeline=pipeline, timestamp=ts, healthy=True,
                        error_rate=0.0, latency_ms=latency_ms, alerts=[])


def _args(**kwargs):
    defaults = dict(pipeline=None, hours=24, json=False, history_file=".pipewatch_history.json")
    defaults.update(kwargs)
    return Namespace(**defaults)


def test_pipeline_latency_no_entries():
    result = _pipeline_latency([], None, 24)
    assert result == []


def test_pipeline_latency_computes_stats():
    entries = [
        _entry("pipe_a", 100.0),
        _entry("pipe_a", 200.0),
        _entry("pipe_a", 300.0),
    ]
    result = _pipeline_latency(entries, None, 24)
    assert len(result) == 1
    row = result[0]
    assert row["pipeline"] == "pipe_a"
    assert row["count"] == 3
    assert row["avg_ms"] == 200.0
    assert row["min_ms"] == 100.0
    assert row["max_ms"] == 300.0


def test_pipeline_latency_filters_by_pipeline():
    entries = [_entry("a", 50.0), _entry("b", 150.0)]
    result = _pipeline_latency(entries, "a", 24)
    assert len(result) == 1
    assert result[0]["pipeline"] == "a"


def test_pipeline_latency_respects_hours_cutoff():
    entries = [_entry("a", 100.0, hours_ago=48)]
    result = _pipeline_latency(entries, None, 24)
    assert result == []


def test_pipeline_latency_skips_none_latency():
    ts = datetime.now(timezone.utc)
    e = HistoryEntry(pipeline="a", timestamp=ts, healthy=True,
                     error_rate=0.0, latency_ms=None, alerts=[])
    result = _pipeline_latency([e], None, 24)
    assert result == []


def test_format_text_no_rows():
    assert _format_text([]) == "No latency data found."


def test_format_text_has_header_and_row():
    rows = [{"pipeline": "pipe_a", "count": 3, "avg_ms": 200.0, "min_ms": 100.0, "max_ms": 300.0}]
    out = _format_text(rows)
    assert "pipe_a" in out
    assert "200.0" in out
    assert "Pipeline" in out


def test_run_latency_cmd_text(capsys):
    entries = [_entry("pipe_x", 120.0)]
    with patch("pipewatch.commands.latency_cmd.RunHistory") as MockH:
        MockH.return_value.all.return_value = entries
        code = run_latency_cmd(_args())
    out = capsys.readouterr().out
    assert code == 0
    assert "pipe_x" in out


def test_run_latency_cmd_json(capsys):
    entries = [_entry("pipe_x", 120.0)]
    with patch("pipewatch.commands.latency_cmd.RunHistory") as MockH:
        MockH.return_value.all.return_value = entries
        code = run_latency_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert code == 0
    assert data[0]["pipeline"] == "pipe_x"
