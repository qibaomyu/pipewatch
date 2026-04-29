"""Tests for rerun_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pipewatch.commands.rerun_cmd import _pipeline_rerun, run_rerun_cmd


def _entry(
    pipeline: str,
    healthy: bool = True,
    rerun: bool = False,
    offset_seconds: float = 60,
):
    ts = datetime.now(timezone.utc).timestamp() - offset_seconds
    return SimpleNamespace(pipeline=pipeline, healthy=healthy, rerun=rerun, timestamp=ts)


def _args(**kwargs):
    defaults = {
        "pipeline": None,
        "hours": 24.0,
        "json": False,
        "history_file": ".pipewatch_history.json",
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.rerun_cmd.RunHistory",
        return_value=SimpleNamespace(all=lambda: entries),
    )


# ---------------------------------------------------------------------------
# Unit tests for _pipeline_rerun
# ---------------------------------------------------------------------------

def test_pipeline_rerun_no_entries():
    result = _pipeline_rerun([], "pipe_a", 24.0)
    assert result is None


def test_pipeline_rerun_no_reruns():
    entries = [_entry("pipe_a", healthy=True, rerun=False)]
    result = _pipeline_rerun(entries, "pipe_a", 24.0)
    assert result is not None
    assert result["rerun_count"] == 0
    assert result["success_rate_pct"] == 0.0


def test_pipeline_rerun_counts_reruns():
    entries = [
        _entry("pipe_a", healthy=True, rerun=True),
        _entry("pipe_a", healthy=False, rerun=True),
        _entry("pipe_a", healthy=True, rerun=False),
    ]
    result = _pipeline_rerun(entries, "pipe_a", 24.0)
    assert result["total_runs"] == 3
    assert result["rerun_count"] == 2
    assert result["rerun_success"] == 1
    assert result["success_rate_pct"] == 50.0


def test_pipeline_rerun_outside_window_excluded():
    entries = [_entry("pipe_a", rerun=True, offset_seconds=7200)]
    result = _pipeline_rerun(entries, "pipe_a", 1.0)  # 1 hour window
    assert result is None


def test_pipeline_rerun_filters_by_pipeline():
    entries = [
        _entry("pipe_a", rerun=True),
        _entry("pipe_b", rerun=True),
    ]
    result = _pipeline_rerun(entries, "pipe_a", 24.0)
    assert result["total_runs"] == 1


# ---------------------------------------------------------------------------
# Integration tests for run_rerun_cmd
# ---------------------------------------------------------------------------

def test_run_rerun_cmd_no_data_prints_message(capsys):
    with _patch([]):
        rc = run_rerun_cmd(_args())
    assert rc == 0
    assert "No rerun data" in capsys.readouterr().out


def test_run_rerun_cmd_text_output(capsys):
    entries = [
        _entry("pipe_a", healthy=True, rerun=True),
        _entry("pipe_a", healthy=False, rerun=False),
    ]
    with _patch(entries):
        rc = run_rerun_cmd(_args())
    out = capsys.readouterr().out
    assert rc == 0
    assert "pipe_a" in out


def test_run_rerun_cmd_json_output(capsys):
    entries = [_entry("pipe_a", rerun=True)]
    with _patch(entries):
        rc = run_rerun_cmd(_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"


def test_run_rerun_cmd_pipeline_filter(capsys):
    entries = [
        _entry("pipe_a", rerun=True),
        _entry("pipe_b", rerun=True),
    ]
    with _patch(entries):
        rc = run_rerun_cmd(_args(pipeline="pipe_a", json=True))
    data = json.loads(capsys.readouterr().out)
    assert all(r["pipeline"] == "pipe_a" for r in data)
