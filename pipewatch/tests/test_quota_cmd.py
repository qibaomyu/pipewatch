"""Tests for quota_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.commands.quota_cmd import _pipeline_quota, run_quota_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, hours_ago: float = 1.0, healthy: bool = True) -> HistoryEntry:
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts,
        healthy=healthy,
        error_rate=0.0,
        latency=1.0,
        alerts=[],
    )


def _args(**kwargs) -> Namespace:
    defaults = dict(
        pipeline=None,
        hours=24,
        limit=5,
        json=False,
        exit_code=False,
        history_file=".pipewatch_history.json",
    )
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    mock_history = MagicMock()
    mock_history.all.return_value = entries
    return patch("pipewatch.commands.quota_cmd.RunHistory", return_value=mock_history)


def test_pipeline_quota_no_entries():
    result = _pipeline_quota([], "pipe_a", 24, 10)
    assert result["runs"] == 0
    assert result["exceeded"] is False


def test_pipeline_quota_under_limit():
    entries = [_entry("pipe_a") for _ in range(3)]
    result = _pipeline_quota(entries, "pipe_a", 24, 5)
    assert result["runs"] == 3
    assert result["exceeded"] is False


def test_pipeline_quota_exceeded():
    entries = [_entry("pipe_a") for _ in range(6)]
    result = _pipeline_quota(entries, "pipe_a", 24, 5)
    assert result["runs"] == 6
    assert result["exceeded"] is True


def test_pipeline_quota_ignores_old_entries():
    old = _entry("pipe_a", hours_ago=48)
    recent = _entry("pipe_a", hours_ago=1)
    result = _pipeline_quota([old, recent], "pipe_a", 24, 5)
    assert result["runs"] == 1


def test_run_quota_no_history(capsys):
    with _patch([]):
        rc = run_quota_cmd(_args())
    assert rc == 0
    assert "No history" in capsys.readouterr().out


def test_run_quota_json_output(capsys):
    entries = [_entry("pipe_a") for _ in range(3)]
    with _patch(entries):
        rc = run_quota_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data[0]["pipeline"] == "pipe_a"
    assert data[0]["runs"] == 3
    assert rc == 0


def test_run_quota_exit_code_when_exceeded():
    entries = [_entry("pipe_a") for _ in range(10)]
    with _patch(entries):
        rc = run_quota_cmd(_args(limit=5, exit_code=True))
    assert rc == 1


def test_run_quota_no_exit_code_flag():
    entries = [_entry("pipe_a") for _ in range(10)]
    with _patch(entries):
        rc = run_quota_cmd(_args(limit=5, exit_code=False))
    assert rc == 0
