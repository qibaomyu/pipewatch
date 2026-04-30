"""Tests for pipewatch.commands.interval_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.interval_cmd import _pipeline_interval, run_interval_cmd


NOW = datetime.now(timezone.utc).timestamp()


def _entry(pipeline: str, offset_s: float, healthy: bool = True):
    e = MagicMock()
    e.pipeline = pipeline
    e.timestamp = NOW - offset_s
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
    return Namespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.interval_cmd.RunHistory",
        return_value=MagicMock(all=MagicMock(return_value=entries)),
    )


def test_pipeline_interval_no_entries():
    result = _pipeline_interval(MagicMock(all=MagicMock(return_value=[])), None, 24)
    assert result == []


def test_pipeline_interval_single_run():
    entries = [_entry("pipe_a", 100)]
    hist = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_interval(hist, None, 24)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"
    assert result[0]["avg_interval_s"] is None
    assert result[0]["run_count"] == 1


def test_pipeline_interval_computes_avg():
    entries = [
        _entry("pipe_a", 300),
        _entry("pipe_a", 200),
        _entry("pipe_a", 100),
    ]
    hist = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_interval(hist, None, 24)
    assert len(result) == 1
    assert result[0]["avg_interval_s"] == 100.0
    assert result[0]["run_count"] == 3


def test_pipeline_interval_filters_by_pipeline():
    entries = [
        _entry("pipe_a", 300),
        _entry("pipe_a", 100),
        _entry("pipe_b", 200),
        _entry("pipe_b", 50),
    ]
    hist = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_interval(hist, "pipe_a", 24)
    assert all(r["pipeline"] == "pipe_a" for r in result)
    assert len(result) == 1


def test_run_interval_cmd_no_entries(capsys):
    with _patch([]):
        code = run_interval_cmd(_args())
    assert code == 0
    assert "No history" in capsys.readouterr().out


def test_run_interval_cmd_text_output(capsys):
    entries = [_entry("pipe_a", 200), _entry("pipe_a", 100)]
    with _patch(entries):
        code = run_interval_cmd(_args())
    out = capsys.readouterr().out
    assert code == 0
    assert "pipe_a" in out
    assert "100.0s" in out


def test_run_interval_cmd_json_output(capsys):
    entries = [_entry("pipe_a", 200), _entry("pipe_a", 100)]
    with _patch(entries):
        code = run_interval_cmd(_args(json=True))
    out = capsys.readouterr().out
    assert code == 0
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"
