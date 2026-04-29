"""Tests for throttle_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.throttle_cmd import _pipeline_throttle, run_throttle_cmd


def _entry(pipeline: str, ts_offset: float = 0.0):
    e = MagicMock()
    e.pipeline = pipeline
    e.timestamp = datetime.now(tz=timezone.utc).timestamp() - ts_offset
    return e


def _args(**kwargs):
    defaults = dict(
        pipeline=None,
        hours=1.0,
        max_runs=5,
        json=False,
        exit_code=False,
        history_file=".pipewatch_history.json",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries_by_pipeline: dict):
    history = MagicMock()
    history.pipelines.return_value = list(entries_by_pipeline.keys())
    history.get.side_effect = lambda name: entries_by_pipeline.get(name, [])
    return patch("pipewatch.commands.throttle_cmd.RunHistory", return_value=history)


def test_pipeline_throttle_no_entries():
    history = MagicMock()
    history.get.return_value = []
    result = _pipeline_throttle(history, "pipe_a", 1.0, 5)
    assert result is None


def test_pipeline_throttle_under_limit():
    history = MagicMock()
    history.get.return_value = [_entry("pipe_a") for _ in range(3)]
    result = _pipeline_throttle(history, "pipe_a", 1.0, 5)
    assert result is not None
    assert result["run_count"] == 3
    assert result["exceeded"] is False


def test_pipeline_throttle_exceeds_limit():
    history = MagicMock()
    history.get.return_value = [_entry("pipe_a") for _ in range(8)]
    result = _pipeline_throttle(history, "pipe_a", 1.0, 5)
    assert result["run_count"] == 8
    assert result["exceeded"] is True


def test_pipeline_throttle_excludes_old_entries():
    history = MagicMock()
    # 2 recent, 3 old (beyond 1h window)
    recent = [_entry("pipe_a", ts_offset=100) for _ in range(2)]
    old = [_entry("pipe_a", ts_offset=7200) for _ in range(3)]
    history.get.return_value = recent + old
    result = _pipeline_throttle(history, "pipe_a", 1.0, 5)
    assert result["run_count"] == 2
    assert result["exceeded"] is False


def test_run_throttle_cmd_no_data_prints_message(capsys):
    with _patch({}):
        code = run_throttle_cmd(_args())
    out = capsys.readouterr().out
    assert "No pipeline run data found" in out
    assert code == 0


def test_run_throttle_cmd_text_output(capsys):
    entries = {"pipe_a": [_entry("pipe_a") for _ in range(3)]}
    with _patch(entries):
        code = run_throttle_cmd(_args(max_runs=5))
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "3 runs" in out
    assert code == 0


def test_run_throttle_cmd_throttled_text_flag(capsys):
    entries = {"pipe_a": [_entry("pipe_a") for _ in range(8)]}
    with _patch(entries):
        code = run_throttle_cmd(_args(max_runs=5, exit_code=True))
    out = capsys.readouterr().out
    assert "THROTTLED" in out
    assert code == 1


def test_run_throttle_cmd_json_output(capsys):
    entries = {"pipe_a": [_entry("pipe_a") for _ in range(3)]}
    with _patch(entries):
        code = run_throttle_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"
    assert code == 0


def test_run_throttle_cmd_json_exit_code_when_exceeded(capsys):
    entries = {"pipe_a": [_entry("pipe_a") for _ in range(10)]}
    with _patch(entries):
        code = run_throttle_cmd(_args(json=True, max_runs=5, exit_code=True))
    assert code == 1
