"""Tests for pipewatch.commands.idle_cmd."""
from __future__ import annotations

import json
import time
from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.idle_cmd import _pipeline_idle, run_idle_cmd


def _entry(pipeline: str, age_hours: float) -> MagicMock:
    """Return a fake history entry whose timestamp is *age_hours* hours ago."""
    entry = MagicMock()
    entry.pipeline = pipeline
    entry.timestamp = time.time() - age_hours * 3600
    return entry


def _args(**kwargs) -> Namespace:
    defaults = {
        "hours": 24.0,
        "pipeline": None,
        "history_file": ".pipewatch_history.json",
        "json": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    """Patch RunHistory.all to return *entries*."""
    return patch(
        "pipewatch.commands.idle_cmd.RunHistory",
        return_value=MagicMock(all=MagicMock(return_value=entries)),
    )


# ---------------------------------------------------------------------------
# Unit tests for _pipeline_idle
# ---------------------------------------------------------------------------


def test_pipeline_idle_no_entries():
    history = MagicMock(all=MagicMock(return_value=[]))
    result = _pipeline_idle(history, None, 24.0)
    assert result == []


def test_pipeline_idle_recent_run_not_reported():
    history = MagicMock(
        all=MagicMock(return_value=[_entry("pipe_a", age_hours=1.0)])
    )
    result = _pipeline_idle(history, None, 24.0)
    assert result == []


def test_pipeline_idle_old_run_reported():
    history = MagicMock(
        all=MagicMock(return_value=[_entry("pipe_a", age_hours=30.0)])
    )
    result = _pipeline_idle(history, None, 24.0)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"
    assert result[0]["idle"] is True
    assert result[0]["last_run_age_hours"] >= 30.0


def test_pipeline_idle_uses_most_recent_entry():
    """Only the most-recent run per pipeline should be considered."""
    entries = [
        _entry("pipe_b", age_hours=48.0),
        _entry("pipe_b", age_hours=2.0),  # recent run
    ]
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_idle(history, None, 24.0)
    # The recent run (2 h ago) is within 24 h, so pipe_b should NOT be idle.
    assert result == []


def test_pipeline_idle_filters_by_pipeline():
    entries = [
        _entry("pipe_a", age_hours=30.0),
        _entry("pipe_b", age_hours=30.0),
    ]
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_idle(history, "pipe_a", 24.0)
    assert all(r["pipeline"] == "pipe_a" for r in result)


def test_pipeline_idle_sorted_by_age_descending():
    entries = [
        _entry("pipe_a", age_hours=25.0),
        _entry("pipe_b", age_hours=50.0),
        _entry("pipe_c", age_hours=35.0),
    ]
    history = MagicMock(all=MagicMock(return_value=entries))
    result = _pipeline_idle(history, None, 24.0)
    ages = [r["last_run_age_hours"] for r in result]
    assert ages == sorted(ages, reverse=True)


# ---------------------------------------------------------------------------
# Integration-style tests for run_idle_cmd
# ---------------------------------------------------------------------------


def test_run_idle_cmd_no_idle_returns_0(capsys):
    with _patch([_entry("pipe_a", age_hours=1.0)]):
        code = run_idle_cmd(_args())
    assert code == 0
    captured = capsys.readouterr()
    assert "No idle" in captured.out


def test_run_idle_cmd_text_output(capsys):
    with _patch([_entry("pipe_a", age_hours=30.0)]):
        code = run_idle_cmd(_args())
    assert code == 0
    captured = capsys.readouterr()
    assert "pipe_a" in captured.out


def test_run_idle_cmd_json_output(capsys):
    with _patch([_entry("pipe_a", age_hours=30.0)]):
        code = run_idle_cmd(_args(json=True))
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"


def test_run_idle_cmd_exit_code_flag():
    with _patch([_entry("pipe_a", age_hours=30.0)]):
        code = run_idle_cmd(_args(exit_code=True))
    assert code == 1


def test_run_idle_cmd_exit_code_flag_no_idle():
    with _patch([]):
        code = run_idle_cmd(_args(exit_code=True))
    assert code == 0
