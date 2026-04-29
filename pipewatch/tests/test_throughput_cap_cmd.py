"""Tests for throughput_cap_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.throughput_cap_cmd import (
    _pipeline_throughput_cap,
    run_throughput_cap_cmd,
)


def _entry(pipeline: str, ts_offset: float = 0):
    e = MagicMock()
    e.pipeline = pipeline
    e.timestamp = datetime.now(tz=timezone.utc).timestamp() - ts_offset
    return e


def _args(**kwargs):
    defaults = {
        "hours": 24,
        "limit": 10,
        "pipeline": None,
        "json": False,
        "exit_code": False,
        "history_file": ".pipewatch_history.json",
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.throughput_cap_cmd.RunHistory",
        return_value=MagicMock(all=MagicMock(return_value=entries)),
    )


def test_pipeline_throughput_cap_no_entries():
    history = MagicMock(all=MagicMock(return_value=[]))
    result = _pipeline_throughput_cap(history, pipeline=None, hours=24, limit=10)
    assert result == []


def test_pipeline_throughput_cap_under_limit():
    history = MagicMock(all=MagicMock(return_value=[_entry("pipe_a")] * 5))
    result = _pipeline_throughput_cap(history, pipeline=None, hours=24, limit=10)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"
    assert result[0]["runs"] == 5
    assert result[0]["exceeded"] is False


def test_pipeline_throughput_cap_over_limit():
    history = MagicMock(all=MagicMock(return_value=[_entry("pipe_b")] * 15))
    result = _pipeline_throughput_cap(history, pipeline=None, hours=24, limit=10)
    assert result[0]["exceeded"] is True
    assert result[0]["runs"] == 15


def test_pipeline_throughput_cap_filters_by_pipeline():
    entries = [_entry("pipe_a")] * 3 + [_entry("pipe_b")] * 20
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_throughput_cap(history, pipeline="pipe_a", hours=24, limit=10)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"


def test_pipeline_throughput_cap_excludes_old_entries():
    old = _entry("pipe_a", ts_offset=48 * 3600)  # 48 h ago
    recent = _entry("pipe_a")
    history = MagicMock(all=MagicMock(return_value=[old, recent]))
    result = _pipeline_throughput_cap(history, pipeline=None, hours=24, limit=10)
    assert result[0]["runs"] == 1


def test_run_throughput_cap_cmd_no_entries_prints_message(capsys):
    with _patch([]):
        rc = run_throughput_cap_cmd(_args())
    out = capsys.readouterr().out
    assert "No pipeline runs found" in out
    assert rc == 0


def test_run_throughput_cap_cmd_json_output(capsys):
    entries = [_entry("pipe_a")] * 3
    with _patch(entries):
        rc = run_throughput_cap_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data[0]["pipeline"] == "pipe_a"
    assert rc == 0


def test_run_throughput_cap_cmd_exit_code_when_exceeded():
    entries = [_entry("pipe_x")] * 50
    with _patch(entries):
        rc = run_throughput_cap_cmd(_args(limit=10, exit_code=True))
    assert rc == 1


def test_run_throughput_cap_cmd_no_exit_code_flag_returns_0():
    entries = [_entry("pipe_x")] * 50
    with _patch(entries):
        rc = run_throughput_cap_cmd(_args(limit=10, exit_code=False))
    assert rc == 0
