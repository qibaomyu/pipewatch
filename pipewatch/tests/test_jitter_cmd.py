"""Tests for jitter_cmd."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.jitter_cmd import _pipeline_jitter, run_jitter_cmd


def _entry(pipeline: str, ts: float, healthy: bool = True):
    e = MagicMock()
    e.pipeline = pipeline
    e.timestamp = ts
    e.healthy = healthy
    return e


def _args(**kwargs):
    defaults = {
        "hours": 24,
        "pipeline": None,
        "json": False,
        "history_file": ".pipewatch_history.json",
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


NOW = datetime.now(timezone.utc).timestamp()


def _patch(entries):
    history = MagicMock()
    history.all.return_value = entries
    return patch("pipewatch.commands.jitter_cmd.RunHistory", return_value=history)


def test_pipeline_jitter_no_entries():
    rows = _pipeline_jitter(MagicMock(all=lambda: []), None, 24)
    assert rows == []


def test_pipeline_jitter_single_run_returns_none_jitter():
    history = MagicMock()
    history.all.return_value = [_entry("pipe_a", NOW - 100)]
    rows = _pipeline_jitter(history, None, 24)
    assert len(rows) == 1
    assert rows[0]["jitter_seconds"] is None
    assert rows[0]["runs"] == 1


def test_pipeline_jitter_uniform_intervals_zero_jitter():
    # Three runs exactly 60 s apart → jitter should be 0.0
    history = MagicMock()
    history.all.return_value = [
        _entry("pipe_a", NOW - 120),
        _entry("pipe_a", NOW - 60),
        _entry("pipe_a", NOW),
    ]
    rows = _pipeline_jitter(history, None, 24)
    assert len(rows) == 1
    assert rows[0]["jitter_seconds"] == 0.0
    assert rows[0]["mean_interval_seconds"] == 60.0


def test_pipeline_jitter_variable_intervals():
    history = MagicMock()
    # gaps: 10s, 90s → mean=50, variance=1600, std=40
    history.all.return_value = [
        _entry("pipe_b", NOW - 100),
        _entry("pipe_b", NOW - 90),
        _entry("pipe_b", NOW),
    ]
    rows = _pipeline_jitter(history, None, 24)
    assert rows[0]["jitter_seconds"] == 40.0
    assert rows[0]["mean_interval_seconds"] == 50.0


def test_pipeline_jitter_filters_by_pipeline():
    history = MagicMock()
    history.all.return_value = [
        _entry("pipe_a", NOW - 120),
        _entry("pipe_a", NOW - 60),
        _entry("pipe_b", NOW - 50),
        _entry("pipe_b", NOW - 10),
    ]
    rows = _pipeline_jitter(history, "pipe_a", 24)
    assert all(r["pipeline"] == "pipe_a" for r in rows)
    assert len(rows) == 1


def test_run_jitter_cmd_json_output(capsys):
    entries = [
        _entry("pipe_a", NOW - 120),
        _entry("pipe_a", NOW - 60),
        _entry("pipe_a", NOW),
    ]
    with _patch(entries):
        rc = run_jitter_cmd(_args(json=True))
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert rc == 0
    assert data[0]["pipeline"] == "pipe_a"
    assert data[0]["jitter_seconds"] == 0.0


def test_run_jitter_cmd_text_output(capsys):
    entries = [
        _entry("pipe_a", NOW - 120),
        _entry("pipe_a", NOW - 60),
        _entry("pipe_a", NOW),
    ]
    with _patch(entries):
        rc = run_jitter_cmd(_args())
    captured = capsys.readouterr()
    assert rc == 0
    assert "pipe_a" in captured.out
    assert "0.00" in captured.out


def test_run_jitter_cmd_no_entries_prints_message(capsys):
    with _patch([]):
        rc = run_jitter_cmd(_args())
    captured = capsys.readouterr()
    assert rc == 0
    assert "No history" in captured.out
