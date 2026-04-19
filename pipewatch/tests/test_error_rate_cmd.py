"""Tests for error_rate_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.commands.error_rate_cmd import _pipeline_error_rate, run_error_rate_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, error_rate: float, offset_hours: int = 0) -> HistoryEntry:
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=datetime.now(timezone.utc) - timedelta(hours=offset_hours),
        healthy=error_rate == 0.0,
        error_rate=error_rate,
        latency_p99=0.1,
        alerts=[],
    )


def _args(tmp_path, pipeline=None, hours=24, as_json=False):
    return Namespace(
        history_file=str(tmp_path / "hist.json"),
        hours=hours,
        pipeline=pipeline,
        json=as_json,
    )


def _patch(monkeypatch, tmp_path, entries):
    from pipewatch import history as hist_mod
    monkeypatch.setattr(hist_mod.RunHistory, "all", lambda self: entries)


def test_pipeline_error_rate_no_entries():
    result = _pipeline_error_rate([], "pipe-a")
    assert result["count"] == 0
    assert result["avg_error_rate"] is None


def test_pipeline_error_rate_computes_avg():
    entries = [_entry("pipe-a", 0.1), _entry("pipe-a", 0.3)]
    result = _pipeline_error_rate(entries, "pipe-a")
    assert result["avg_error_rate"] == pytest.approx(0.2, abs=1e-4)
    assert result["max_error_rate"] == pytest.approx(0.3, abs=1e-4)


def test_pipeline_error_rate_ignores_other_pipelines():
    entries = [_entry("pipe-a", 0.5), _entry("pipe-b", 0.9)]
    result = _pipeline_error_rate(entries, "pipe-a")
    assert result["count"] == 1
    assert result["max_error_rate"] == pytest.approx(0.5)


def test_run_error_rate_text_output(tmp_path, monkeypatch, capsys):
    entries = [_entry("pipe-a", 0.05), _entry("pipe-a", 0.15)]
    _patch(monkeypatch, tmp_path, entries)
    rc = run_error_rate_cmd(_args(tmp_path))
    out = capsys.readouterr().out
    assert rc == 0
    assert "pipe-a" in out
    assert "10.00" in out  # avg 10%


def test_run_error_rate_json_output(tmp_path, monkeypatch, capsys):
    entries = [_entry("pipe-a", 0.2)]
    _patch(monkeypatch, tmp_path, entries)
    rc = run_error_rate_cmd(_args(tmp_path, as_json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert rc == 0
    assert data[0]["pipeline"] == "pipe-a"
    assert data[0]["avg_error_rate"] == pytest.approx(0.2)


def test_run_error_rate_no_entries_prints_message(tmp_path, monkeypatch, capsys):
    _patch(monkeypatch, tmp_path, [])
    rc = run_error_rate_cmd(_args(tmp_path))
    out = capsys.readouterr().out
    assert rc == 0
    assert "No history" in out


def test_run_error_rate_pipeline_filter(tmp_path, monkeypatch, capsys):
    entries = [_entry("pipe-a", 0.1), _entry("pipe-b", 0.9)]
    _patch(monkeypatch, tmp_path, entries)
    rc = run_error_rate_cmd(_args(tmp_path, pipeline="pipe-a"))
    out = capsys.readouterr().out
    assert rc == 0
    assert "pipe-b" not in out
