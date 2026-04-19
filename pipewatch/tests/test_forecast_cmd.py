"""Tests for forecast_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pipewatch.commands.forecast_cmd import _linear_forecast, _pipeline_forecast, run_forecast_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, error_rate: float, latency: float) -> HistoryEntry:
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=datetime.now(timezone.utc).isoformat(),
        healthy=error_rate < 0.1,
        error_rate=error_rate,
        latency_p99=latency,
        alert_count=0,
    )


def _args(**kwargs):
    defaults = dict(history_file=".pipewatch_history.json", pipeline=None, steps=1, json=False)
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.forecast_cmd.RunHistory.get_all", return_value=entries)


# --- unit tests ---

def test_linear_forecast_empty():
    assert _linear_forecast([]) == 0.0


def test_linear_forecast_single():
    assert _linear_forecast([0.5]) == 0.5


def test_linear_forecast_flat():
    result = _linear_forecast([0.2, 0.2, 0.2], steps=1)
    assert abs(result - 0.2) < 1e-6


def test_linear_forecast_increasing():
    result = _linear_forecast([0.1, 0.2, 0.3], steps=1)
    assert result > 0.3


def test_pipeline_forecast_computes():
    entries = [_entry("pipe", 0.1 * i, 100.0 + 10 * i) for i in range(1, 4)]
    result = _pipeline_forecast(entries, steps=1)
    assert result["samples"] == 3
    assert "forecast_error_rate" in result
    assert "forecast_latency_p99" in result


# --- integration tests ---

def test_run_forecast_no_history(capsys):
    with _patch([]):
        code = run_forecast_cmd(_args())
    assert code == 0
    assert "No history" in capsys.readouterr().out


def test_run_forecast_text_output(capsys):
    entries = [_entry("alpha", 0.05 * i, 50.0 * i) for i in range(1, 4)]
    with _patch(entries):
        code = run_forecast_cmd(_args())
    out = capsys.readouterr().out
    assert code == 0
    assert "alpha" in out


def test_run_forecast_json_output(capsys):
    entries = [_entry("beta", 0.1, 200.0), _entry("beta", 0.2, 210.0)]
    with _patch(entries):
        code = run_forecast_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data[0]["pipeline"] == "beta"
    assert "forecast_error_rate" in data[0]


def test_run_forecast_pipeline_filter(capsys):
    entries = [_entry("a", 0.1, 100.0), _entry("b", 0.2, 200.0)]
    with _patch(entries):
        code = run_forecast_cmd(_args(pipeline="a", json=True))
    data = json.loads(capsys.readouterr().out)
    assert all(r["pipeline"] == "a" for r in data)
