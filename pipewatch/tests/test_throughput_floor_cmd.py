"""Tests for throughput_floor_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.commands.throughput_floor_cmd import (
    _pipeline_throughput_floor,
    run_throughput_floor_cmd,
)


def _entry(pipeline: str, hours_ago: float = 1.0, healthy: bool = True):
    e = MagicMock()
    e.pipeline = pipeline
    e.healthy = healthy
    ts = datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
    e.timestamp = ts.isoformat()
    return e


def _args(**kwargs):
    defaults = {
        "history_file": ".pipewatch_history.json",
        "hours": 24,
        "min_runs": 3,
        "pipeline": None,
        "json": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.throughput_floor_cmd.RunHistory",
        return_value=MagicMock(all=MagicMock(return_value=entries)),
    )


def test_pipeline_throughput_floor_no_entries():
    history = MagicMock(all=MagicMock(return_value=[]))
    result = _pipeline_throughput_floor(history, pipeline=None, hours=24, min_runs=3)
    assert result == []


def test_pipeline_throughput_floor_below_floor():
    entries = [_entry("pipe_a"), _entry("pipe_a")]  # 2 runs, floor=3
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_throughput_floor(history, pipeline=None, hours=24, min_runs=3)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"
    assert result[0]["runs"] == 2
    assert result[0]["below_floor"] is True


def test_pipeline_throughput_floor_above_floor():
    entries = [_entry("pipe_b")] * 5  # 5 runs, floor=3
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_throughput_floor(history, pipeline=None, hours=24, min_runs=3)
    assert result[0]["below_floor"] is False


def test_pipeline_throughput_floor_filters_by_pipeline():
    entries = [_entry("pipe_a")] * 2 + [_entry("pipe_b")] * 5
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_throughput_floor(history, pipeline="pipe_b", hours=24, min_runs=3)
    assert all(r["pipeline"] == "pipe_b" for r in result)
    assert len(result) == 1
    assert result[0]["below_floor"] is False


def test_pipeline_throughput_floor_excludes_old_entries():
    entries = [_entry("pipe_a", hours_ago=48)]  # outside 24h window
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_throughput_floor(history, pipeline=None, hours=24, min_runs=1)
    assert result == []


def test_run_throughput_floor_cmd_no_data(capsys):
    with _patch([]) as mock_history:
        rc = run_throughput_floor_cmd(_args())
    captured = capsys.readouterr()
    assert rc == 0
    assert "No pipeline data found" in captured.out


def test_run_throughput_floor_cmd_text_output(capsys):
    entries = [_entry("pipe_a")] * 2
    with _patch(entries):
        rc = run_throughput_floor_cmd(_args(min_runs=3))
    captured = capsys.readouterr()
    assert "pipe_a" in captured.out
    assert "BELOW FLOOR" in captured.out
    assert rc == 0


def test_run_throughput_floor_cmd_json_output(capsys):
    entries = [_entry("pipe_a")] * 4
    with _patch(entries):
        rc = run_throughput_floor_cmd(_args(min_runs=3, json=True))
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"
    assert data[0]["below_floor"] is False


def test_run_throughput_floor_cmd_exit_code_when_below(capsys):
    entries = [_entry("pipe_a")]
    with _patch(entries):
        rc = run_throughput_floor_cmd(_args(min_runs=3, exit_code=True))
    assert rc == 1


def test_run_throughput_floor_cmd_exit_code_when_ok(capsys):
    entries = [_entry("pipe_a")] * 5
    with _patch(entries):
        rc = run_throughput_floor_cmd(_args(min_runs=3, exit_code=True))
    assert rc == 0
