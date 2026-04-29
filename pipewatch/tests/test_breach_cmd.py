"""Tests for breach_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.commands.breach_cmd import _pipeline_breach, run_breach_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, hours_ago: float = 1.0, alerts=None):
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=datetime.now(timezone.utc) - timedelta(hours=hours_ago),
        healthy=not bool(alerts),
        alerts=alerts or [],
        error_rate=0.5 if alerts else 0.0,
        latency_p99=1.0,
    )


def _args(**kwargs):
    defaults = dict(
        hours=24,
        min_breaches=3,
        pipeline=None,
        json=False,
        exit_code=False,
        history_file=".pipewatch_history.json",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries):
    mock_history = MagicMock()
    mock_history.all.return_value = entries
    return patch("pipewatch.commands.breach_cmd.RunHistory", return_value=mock_history)


def test_pipeline_breach_no_entries():
    result = _pipeline_breach([], "pipe_a", hours=24, min_breaches=3)
    assert result is None


def test_pipeline_breach_below_threshold():
    entries = [_entry("pipe_a", alerts=["err"]), _entry("pipe_a")]
    result = _pipeline_breach(entries, "pipe_a", hours=24, min_breaches=3)
    assert result is not None
    assert result["breached"] is False
    assert result["breach_count"] == 1


def test_pipeline_breach_at_threshold():
    entries = [_entry("pipe_a", alerts=["e"]) for _ in range(3)]
    result = _pipeline_breach(entries, "pipe_a", hours=24, min_breaches=3)
    assert result["breached"] is True
    assert result["breach_count"] == 3


def test_pipeline_breach_ignores_old_entries():
    entries = [
        _entry("pipe_a", hours_ago=25, alerts=["e"]),
        _entry("pipe_a", hours_ago=26, alerts=["e"]),
        _entry("pipe_a", hours_ago=27, alerts=["e"]),
    ]
    result = _pipeline_breach(entries, "pipe_a", hours=24, min_breaches=3)
    assert result is None


def test_run_breach_cmd_no_entries(capsys):
    with _patch([]):
        code = run_breach_cmd(_args())
    out = capsys.readouterr().out
    assert "No history" in out
    assert code == 0


def test_run_breach_cmd_text_output(capsys):
    entries = [
        _entry("pipe_a", alerts=["e"]),
        _entry("pipe_a", alerts=["e"]),
        _entry("pipe_a", alerts=["e"]),
    ]
    with _patch(entries):
        code = run_breach_cmd(_args())
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "BREACHED" in out
    assert code == 0


def test_run_breach_cmd_json_output(capsys):
    entries = [_entry("pipe_b", alerts=["e"]) for _ in range(4)]
    with _patch(entries):
        code = run_breach_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_b"
    assert data[0]["breached"] is True


def test_run_breach_cmd_exit_code_when_breached():
    entries = [_entry("pipe_c", alerts=["e"]) for _ in range(5)]
    with _patch(entries):
        code = run_breach_cmd(_args(exit_code=True))
    assert code == 1


def test_run_breach_cmd_exit_code_ok_when_healthy():
    entries = [_entry("pipe_d") for _ in range(5)]
    with _patch(entries):
        code = run_breach_cmd(_args(exit_code=True))
    assert code == 0
