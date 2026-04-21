"""Tests for pipewatch/commands/coverage_cmd.py"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.coverage_cmd import _pipeline_coverage, run_coverage_cmd
from pipewatch.history import HistoryEntry


NOW = datetime.now(tz=timezone.utc)


def _entry(pipeline: str, hours_ago: float = 1.0, healthy: bool = True) -> HistoryEntry:
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=NOW - timedelta(hours=hours_ago),
        healthy=healthy,
        error_rate=0.0,
        latency=0.1,
        alerts=[],
    )


def _args(**kwargs):
    defaults = dict(
        config="pipewatch.yaml",
        history_file=".pipewatch_history.json",
        hours=24,
        pipeline=None,
        json=False,
        exit_code=False,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(pipelines, entries_by_name):
    """Return context managers that patch load_config and RunHistory."""
    cfg = MagicMock()
    cfg.pipelines = {p: MagicMock() for p in pipelines}

    history = MagicMock()
    history.get.side_effect = lambda name: entries_by_name.get(name, [])

    return cfg, history


# ---------------------------------------------------------------------------
# Unit tests for _pipeline_coverage
# ---------------------------------------------------------------------------

def test_pipeline_coverage_all_covered():
    history = MagicMock()
    history.get.side_effect = lambda n: [_entry(n)]
    rows = _pipeline_coverage(["a", "b"], history, hours=24, pipeline_filter=None)
    assert all(r["covered"] for r in rows)
    assert all(r["run_count"] == 1 for r in rows)


def test_pipeline_coverage_none_covered():
    history = MagicMock()
    history.get.return_value = []
    rows = _pipeline_coverage(["a", "b"], history, hours=24, pipeline_filter=None)
    assert all(not r["covered"] for r in rows)
    assert all(r["last_run"] is None for r in rows)


def test_pipeline_coverage_filters_by_pipeline():
    history = MagicMock()
    history.get.side_effect = lambda n: [_entry(n)]
    rows = _pipeline_coverage(["a", "b"], history, hours=24, pipeline_filter="a")
    assert len(rows) == 1
    assert rows[0]["pipeline"] == "a"


def test_pipeline_coverage_respects_time_window():
    history = MagicMock()
    # entry is 48 h ago, window is 24 h
    history.get.side_effect = lambda n: [_entry(n, hours_ago=48)]
    rows = _pipeline_coverage(["a"], history, hours=24, pipeline_filter=None)
    assert not rows[0]["covered"]


# ---------------------------------------------------------------------------
# Integration tests for run_coverage_cmd
# ---------------------------------------------------------------------------

def test_run_coverage_cmd_text_output(capsys):
    cfg, history = _patch(["pipe_a", "pipe_b"], {"pipe_a": [_entry("pipe_a")]})
    with patch("pipewatch.commands.coverage_cmd.load_config", return_value=cfg), \
         patch("pipewatch.commands.coverage_cmd.RunHistory", return_value=history):
        rc = run_coverage_cmd(_args())
    out = capsys.readouterr().out
    assert "1/2" in out
    assert "pipe_a" in out
    assert rc == 0


def test_run_coverage_cmd_json_output(capsys):
    cfg, history = _patch(["pipe_a"], {"pipe_a": [_entry("pipe_a")]})
    with patch("pipewatch.commands.coverage_cmd.load_config", return_value=cfg), \
         patch("pipewatch.commands.coverage_cmd.RunHistory", return_value=history):
        rc = run_coverage_cmd(_args(json=True))
    data = json.loads(capsys.readouterr().out)
    assert data["coverage_pct"] == 100.0
    assert data["pipelines"][0]["pipeline"] == "pipe_a"
    assert rc == 0


def test_run_coverage_cmd_exit_code_when_gap(capsys):
    cfg, history = _patch(["pipe_a", "pipe_b"], {"pipe_a": [_entry("pipe_a")]})
    with patch("pipewatch.commands.coverage_cmd.load_config", return_value=cfg), \
         patch("pipewatch.commands.coverage_cmd.RunHistory", return_value=history):
        rc = run_coverage_cmd(_args(exit_code=True))
    assert rc == 1


def test_run_coverage_cmd_missing_config(capsys):
    with patch("pipewatch.commands.coverage_cmd.load_config", return_value=None):
        rc = run_coverage_cmd(_args())
    assert rc == 2
