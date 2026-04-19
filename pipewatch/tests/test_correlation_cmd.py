"""Tests for correlation_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from pipewatch.commands.correlation_cmd import _pearson, _pipeline_correlations, run_correlation_cmd
from pipewatch.history import HistoryEntry, RunHistory


def _entry(pipeline: str, error_rate: float, offset_minutes: int = 0) -> HistoryEntry:
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=datetime(2024, 1, 1, 12, 0) + timedelta(minutes=offset_minutes),
        healthy=error_rate < 0.1,
        error_rate=error_rate,
        latency_ms=100.0,
        alert_count=0,
    )


def _args(**kwargs):
    defaults = {"history_file": ".pipewatch_history.json", "min_entries": 3, "json": False}
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch.object(RunHistory, "all", return_value=entries)


def test_pearson_perfect_positive():
    a = [1.0, 2.0, 3.0, 4.0]
    b = [2.0, 4.0, 6.0, 8.0]
    assert abs(_pearson(a, b) - 1.0) < 1e-9


def test_pearson_perfect_negative():
    a = [1.0, 2.0, 3.0]
    b = [3.0, 2.0, 1.0]
    assert abs(_pearson(a, b) - (-1.0)) < 1e-9


def test_pearson_constant_returns_zero():
    assert _pearson([1.0, 1.0, 1.0], [1.0, 2.0, 3.0]) == 0.0


def test_pearson_too_short_returns_zero():
    assert _pearson([0.5], [0.5]) == 0.0


def test_pipeline_correlations_insufficient_data():
    entries = [_entry("p1", 0.1, i) for i in range(2)]  # only 2, need 3
    with _patch(entries):
        pairs = _pipeline_correlations(RunHistory(".x"), min_entries=3)
    assert pairs == []


def test_pipeline_correlations_computes_pair():
    entries = [
        *[_entry("alpha", float(i) / 10, i * 10) for i in range(5)],
        *[_entry("beta", float(i) / 10, i * 10) for i in range(5)],
    ]
    with _patch(entries):
        pairs = _pipeline_correlations(RunHistory(".x"), min_entries=3)
    assert len(pairs) == 1
    a, b, r = pairs[0]
    assert set([a, b]) == {"alpha", "beta"}
    assert abs(r - 1.0) < 0.01


def test_run_correlation_cmd_text(capsys):
    entries = [
        *[_entry("alpha", float(i) / 10, i * 10) for i in range(4)],
        *[_entry("beta", float(i) / 10, i * 10) for i in range(4)],
    ]
    with _patch(entries):
        rc = run_correlation_cmd(_args())
    out = capsys.readouterr().out
    assert rc == 0
    assert "alpha" in out
    assert "beta" in out


def test_run_correlation_cmd_json(capsys):
    entries = [
        *[_entry("alpha", float(i) / 10, i * 10) for i in range(4)],
        *[_entry("beta", 1.0 - float(i) / 10, i * 10) for i in range(4)],
    ]
    with _patch(entries):
        rc = run_correlation_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert rc == 0
    assert data[0]["pipeline_a"] in {"alpha", "beta"}
    assert "r" in data[0]


def test_run_correlation_cmd_no_data(capsys):
    with _patch([]):
        rc = run_correlation_cmd(_args())
    assert rc == 0
    assert "insufficient" in capsys.readouterr().out
