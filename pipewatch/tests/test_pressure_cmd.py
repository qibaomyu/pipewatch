"""Tests for pressure_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pipewatch.commands.pressure_cmd import _pipeline_pressure, run_pressure_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, error_rate: float, latency_ms: float, hours_ago: float = 1.0) -> HistoryEntry:
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts,
        healthy=error_rate < 0.1,
        error_rate=error_rate,
        latency_ms=latency_ms,
        alerts=[],
    )


def _args(**kwargs):
    defaults = {
        "hours": 24.0,
        "pipeline": None,
        "json": False,
        "history_file": ".pipewatch_history.json",
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.pressure_cmd.RunHistory.all", return_value=entries)


class TestPipelinePressure:
    def test_no_entries_returns_none(self):
        result = _pipeline_pressure([], "pipe_a", 24.0)
        assert result is None

    def test_computes_pressure_score(self):
        entries = [
            _entry("pipe_a", error_rate=0.2, latency_ms=500.0),
            _entry("pipe_a", error_rate=0.4, latency_ms=1000.0),
        ]
        result = _pipeline_pressure(entries, "pipe_a", 24.0)
        assert result is not None
        assert result["pipeline"] == "pipe_a"
        assert result["avg_error_rate"] == pytest.approx(0.3, abs=1e-4)
        assert result["avg_latency_ms"] == pytest.approx(750.0, abs=1e-2)
        assert result["pressure_score"] == pytest.approx(0.3 * 750.0, abs=1e-2)
        assert result["sample_size"] == 2

    def test_filters_by_pipeline(self):
        entries = [
            _entry("pipe_a", 0.1, 200.0),
            _entry("pipe_b", 0.5, 800.0),
        ]
        result = _pipeline_pressure(entries, "pipe_b", 24.0)
        assert result["pipeline"] == "pipe_b"
        assert result["sample_size"] == 1

    def test_excludes_old_entries(self):
        old = _entry("pipe_a", 0.9, 9000.0, hours_ago=48.0)
        recent = _entry("pipe_a", 0.1, 100.0, hours_ago=1.0)
        result = _pipeline_pressure([old, recent], "pipe_a", 24.0)
        assert result["sample_size"] == 1
        assert result["avg_error_rate"] == pytest.approx(0.1, abs=1e-4)


class TestRunPressureCmd:
    def test_no_data_prints_message(self, capsys):
        with _patch([]):
            rc = run_pressure_cmd(_args())
        assert rc == 0
        assert "No data" in capsys.readouterr().out

    def test_text_output(self, capsys):
        entries = [_entry("pipe_a", 0.2, 400.0)]
        with _patch(entries):
            rc = run_pressure_cmd(_args())
        assert rc == 0
        out = capsys.readouterr().out
        assert "pipe_a" in out
        assert "Pressure" in out

    def test_json_output(self, capsys):
        entries = [_entry("pipe_a", 0.2, 400.0)]
        with _patch(entries):
            rc = run_pressure_cmd(_args(json=True))
        assert rc == 0
        data = json.loads(capsys.readouterr().out)
        assert isinstance(data, list)
        assert data[0]["pipeline"] == "pipe_a"
        assert "pressure_score" in data[0]

    def test_pipeline_filter(self, capsys):
        entries = [
            _entry("pipe_a", 0.1, 100.0),
            _entry("pipe_b", 0.5, 500.0),
        ]
        with _patch(entries):
            rc = run_pressure_cmd(_args(pipeline="pipe_b", json=True))
        assert rc == 0
        data = json.loads(capsys.readouterr().out)
        assert len(data) == 1
        assert data[0]["pipeline"] == "pipe_b"
