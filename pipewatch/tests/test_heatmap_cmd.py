"""Tests for heatmap_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pipewatch.commands.heatmap_cmd import _build_heatmap, _symbol, run_heatmap_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, hours_ago: float, error_rate: float) -> HistoryEntry:
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts.isoformat(),
        healthy=error_rate < 0.1,
        error_rate=error_rate,
        latency_p99=0.5,
    )


def _args(**kwargs):
    defaults = dict(hours=24, pipeline=None, json=False, history_file="x.json")
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.heatmap_cmd.RunHistory", autospec=True,
                 return_value=type("H", (), {"all": lambda self: entries})())


# --- unit: _symbol ---

def test_symbol_zero():
    assert _symbol(0) == " "


def test_symbol_low():
    assert _symbol(0.05) == "░"


def test_symbol_high():
    assert _symbol(0.9) == "█"


# --- unit: _build_heatmap ---

def test_build_heatmap_empty():
    result = _build_heatmap([], 24, None)
    assert result == {}


def test_build_heatmap_filters_old_entries():
    entries = [_entry("pipe_a", 48, 0.5)]
    result = _build_heatmap(entries, 24, None)
    assert result == {}


def test_build_heatmap_groups_by_pipeline_and_hour():
    entries = [
        _entry("pipe_a", 1, 0.2),
        _entry("pipe_a", 1, 0.4),
        _entry("pipe_b", 2, 0.1),
    ]
    result = _build_heatmap(entries, 24, None)
    assert "pipe_a" in result
    assert "pipe_b" in result
    pipe_a_avg = list(result["pipe_a"].values())[0]
    assert abs(pipe_a_avg - 0.3) < 1e-9


def test_build_heatmap_pipeline_filter():
    entries = [_entry("pipe_a", 1, 0.2), _entry("pipe_b", 1, 0.5)]
    result = _build_heatmap(entries, 24, "pipe_a")
    assert "pipe_a" in result
    assert "pipe_b" not in result


# --- integration: run_heatmap_cmd ---

def test_run_heatmap_no_data(capsys):
    with _patch([]):
        rc = run_heatmap_cmd(_args())
    assert rc == 0
    assert "No data" in capsys.readouterr().out


def test_run_heatmap_json_output(capsys):
    entries = [_entry("pipe_a", 1, 0.3)]
    with _patch(entries):
        rc = run_heatmap_cmd(_args(json=True))
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert "pipe_a" in data


def test_run_heatmap_text_output(capsys):
    entries = [_entry("pipe_a", 1, 0.3)]
    with _patch(entries):
        rc = run_heatmap_cmd(_args())
    assert rc == 0
    out = capsys.readouterr().out
    assert "pipe_a" in out
