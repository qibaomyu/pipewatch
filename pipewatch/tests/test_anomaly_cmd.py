"""Tests for the anomaly detection command."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.commands.anomaly_cmd import _pipeline_anomaly, run_anomaly_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, error_rate: float, hours_ago: float) -> HistoryEntry:
    ts = datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
    return HistoryEntry(pipeline=pipeline, timestamp=ts, error_rate=error_rate, latency=0.1, healthy=error_rate < 0.1)


def _args(**kwargs):
    defaults = dict(
        history_file=".pipewatch_history.json",
        pipeline=None,
        baseline_hours=24,
        recent_hours=1,
        threshold=0.05,
        format="text",
        exit_code=False,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_pipeline_anomaly_detects_spike():
    entries = (
        [_entry("etl", 0.02, h) for h in range(2, 25)]  # baseline: low error rate
        + [_entry("etl", 0.20, 0.5)]  # recent: spike
    )
    result = _pipeline_anomaly(entries, "etl", baseline_hours=24, recent_hours=1, threshold=0.05)
    assert result is not None
    assert result["anomaly"] is True
    assert result["deviation"] > 0.05


def test_pipeline_anomaly_no_spike():
    entries = (
        [_entry("etl", 0.02, h) for h in range(2, 25)]
        + [_entry("etl", 0.03, 0.5)]
    )
    result = _pipeline_anomaly(entries, "etl", baseline_hours=24, recent_hours=1, threshold=0.05)
    assert result is not None
    assert result["anomaly"] is False


def test_pipeline_anomaly_insufficient_data():
    entries = [_entry("etl", 0.02, 0.5)]
    result = _pipeline_anomaly(entries, "etl", baseline_hours=24, recent_hours=1, threshold=0.05)
    assert result is None


def _patch(entries):
    mock_history = MagicMock()
    mock_history.all.return_value = entries
    return patch("pipewatch.commands.anomaly_cmd.RunHistory", return_value=mock_history)


def test_run_anomaly_cmd_text_output(capsys):
    entries = (
        [_entry("pipe", 0.01, h) for h in range(2, 25)]
        + [_entry("pipe", 0.30, 0.5)]
    )
    with _patch(entries):
        code = run_anomaly_cmd(_args())
    out = capsys.readouterr().out
    assert "pipe" in out
    assert "ANOMALY" in out
    assert code == 0


def test_run_anomaly_cmd_exit_code_on_anomaly():
    entries = (
        [_entry("pipe", 0.01, h) for h in range(2, 25)]
        + [_entry("pipe", 0.30, 0.5)]
    )
    with _patch(entries):
        code = run_anomaly_cmd(_args(exit_code=True))
    assert code == 1


def test_run_anomaly_cmd_json_output(capsys):
    import json
    entries = (
        [_entry("pipe", 0.01, h) for h in range(2, 25)]
        + [_entry("pipe", 0.30, 0.5)]
    )
    with _patch(entries):
        run_anomaly_cmd(_args(format="json"))
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe"


def test_run_anomaly_cmd_empty_history(capsys):
    with _patch([]):
        code = run_anomaly_cmd(_args())
    assert "No history" in capsys.readouterr().out
    assert code == 0
