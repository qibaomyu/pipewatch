"""Tests for retention_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.retention_cmd import _pipeline_retention, run_retention_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, hours_ago: float, healthy: bool = True) -> HistoryEntry:
    ts = datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
    e = MagicMock(spec=HistoryEntry)
    e.pipeline = pipeline
    e.timestamp = ts
    e.healthy = healthy
    return e


def _args(**kwargs) -> Namespace:
    defaults = {
        "history_file": "pipewatch_history.json",
        "pipeline": None,
        "top": 20,
        "json": False,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.retention_cmd.RunHistory", autospec=True,
                 return_value=MagicMock(all=MagicMock(return_value=entries)))


def test_pipeline_retention_no_entries():
    history = MagicMock(all=MagicMock(return_value=[]))
    result = _pipeline_retention(history, None, 20)
    assert result == []


def test_pipeline_retention_single_pipeline():
    entries = [_entry("etl", 48), _entry("etl", 24), _entry("etl", 1)]
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_retention(history, None, 20)
    assert len(result) == 1
    row = result[0]
    assert row["pipeline"] == "etl"
    assert row["run_count"] == 3
    assert row["span_hours"] >= 47


def test_pipeline_retention_filters_by_pipeline():
    entries = [_entry("etl", 10), _entry("ml", 5)]
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_retention(history, "etl", 20)
    assert len(result) == 1
    assert result[0]["pipeline"] == "etl"


def test_pipeline_retention_top_limits_results():
    entries = [_entry(f"pipe{i}", i * 2) for i in range(1, 6)]
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_retention(history, None, 3)
    assert len(result) == 3


def test_pipeline_retention_sorted_by_span_descending():
    entries = [_entry("short", 2), _entry("long", 100), _entry("medium", 50)]
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_retention(history, None, 20)
    spans = [r["span_hours"] for r in result]
    assert spans == sorted(spans, reverse=True)


def test_run_retention_cmd_no_entries(capsys):
    with _patch([]):
        code = run_retention_cmd(_args())
    assert code == 0
    assert "No history found" in capsys.readouterr().out


def test_run_retention_cmd_text_output(capsys):
    entries = [_entry("etl", 10)]
    with _patch(entries):
        code = run_retention_cmd(_args())
    assert code == 0
    out = capsys.readouterr().out
    assert "etl" in out
    assert "Span" in out


def test_run_retention_cmd_json_output(capsys):
    entries = [_entry("etl", 10)]
    with _patch(entries):
        code = run_retention_cmd(_args(json=True))
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "etl"
