"""Tests for spike_cmd."""
import argparse
import json
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.commands.spike_cmd import _rolling_avg, _pipeline_spike, run_spike_cmd
from pipewatch.history import HistoryEntry
from datetime import datetime, timezone


def _entry(pipeline, error_rate, minutes_ago=0):
    ts = datetime.now(timezone.utc).timestamp() - minutes_ago * 60
    return HistoryEntry(pipeline=pipeline, timestamp=ts, error_rate=error_rate, latency=0.1, healthy=error_rate < 0.1)


def _args(**kwargs):
    defaults = dict(
        history_file=".pipewatch_history.json",
        pipeline=None,
        window=3,
        multiplier=2.0,
        json=False,
        exit_code=False,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.spike_cmd.RunHistory", return_value=MagicMock(all=lambda: entries))


def test_rolling_avg_empty():
    assert _rolling_avg([]) is None


def test_rolling_avg_computes():
    entries = [_entry("p", 0.1), _entry("p", 0.3)]
    assert _rolling_avg(entries) == pytest.approx(0.2)


def test_pipeline_spike_insufficient_data():
    result = _pipeline_spike("p", [], window=3, multiplier=2.0)
    assert result["spike"] is False
    assert "insufficient" in result["reason"]


def test_pipeline_spike_no_baseline():
    entries = [_entry("p", 0.1, 2), _entry("p", 0.2, 1)]
    result = _pipeline_spike("p", entries, window=5, multiplier=2.0)
    assert result["spike"] is False


def test_pipeline_spike_detects_spike():
    baseline = [_entry("p", 0.05, i + 10) for i in range(5)]
    recent = [_entry("p", 0.5, i) for i in range(3)]
    result = _pipeline_spike("p", baseline + recent, window=3, multiplier=2.0)
    assert result["spike"] is True
    assert result["ratio"] >= 2.0


def test_pipeline_spike_no_spike():
    baseline = [_entry("p", 0.1, i + 10) for i in range(5)]
    recent = [_entry("p", 0.12, i) for i in range(3)]
    result = _pipeline_spike("p", baseline + recent, window=3, multiplier=2.0)
    assert result["spike"] is False


def test_run_spike_cmd_text(capsys):
    baseline = [_entry("pipe_a", 0.05, i + 10) for i in range(5)]
    recent = [_entry("pipe_a", 0.5, i) for i in range(3)]
    with _patch(baseline + recent):
        code = run_spike_cmd(_args(pipeline="pipe_a", window=3))
    out = capsys.readouterr().out
    assert "pipe_a" in out


def test_run_spike_cmd_json(capsys):
    entries = [_entry("pipe_a", 0.1, i + 5) for i in range(4)] + [_entry("pipe_a", 0.1, i) for i in range(2)]
    with _patch(entries):
        run_spike_cmd(_args(pipeline="pipe_a", json=True, window=2))
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"


def test_run_spike_cmd_exit_code_on_spike():
    baseline = [_entry("p", 0.02, i + 10) for i in range(5)]
    recent = [_entry("p", 0.9, i) for i in range(3)]
    with _patch(baseline + recent):
        code = run_spike_cmd(_args(pipeline="p", window=3, exit_code=True))
    assert code == 1


def test_run_spike_cmd_no_exit_code_when_healthy():
    entries = [_entry("p", 0.05, i + 5) for i in range(4)] + [_entry("p", 0.06, i) for i in range(3)]
    with _patch(entries):
        code = run_spike_cmd(_args(pipeline="p", window=3, exit_code=True))
    assert code == 0
