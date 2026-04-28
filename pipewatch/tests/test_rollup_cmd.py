"""Tests for rollup_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pipewatch.commands.rollup_cmd import _pipeline_rollup, run_rollup_cmd
from pipewatch.history import HistoryEntry


NOW = datetime.now(tz=timezone.utc).timestamp()


def _entry(
    pipeline: str,
    healthy: bool = True,
    error_rate: float = 0.0,
    latency: float = 1.0,
    offset: float = 0,
) -> HistoryEntry:
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=NOW - offset,
        healthy=healthy,
        error_rate=error_rate,
        latency=latency,
        alerts=[],
    )


def _args(**kwargs):
    defaults = dict(hours=24.0, pipeline=None, json=False, history_file=".pipewatch_history.json")
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.rollup_cmd.RunHistory.all", return_value=entries)


# ---------------------------------------------------------------------------
# unit tests for _pipeline_rollup
# ---------------------------------------------------------------------------

def test_pipeline_rollup_no_entries():
    with _patch([]):
        from pipewatch.history import RunHistory
        h = RunHistory(".pipewatch_history.json")
        result = _pipeline_rollup(h, None, 24)
    assert result == []


def test_pipeline_rollup_single_pipeline():
    entries = [_entry("etl", healthy=True, error_rate=0.01, latency=2.0)]
    with _patch(entries):
        from pipewatch.history import RunHistory
        h = RunHistory(".pipewatch_history.json")
        result = _pipeline_rollup(h, None, 24)
    assert len(result) == 1
    assert result[0]["pipeline"] == "etl"
    assert result[0]["total_runs"] == 1
    assert result[0]["failed_runs"] == 0
    assert result[0]["success_rate"] == 100.0
    assert result[0]["avg_error_rate"] == pytest.approx(0.01)
    assert result[0]["avg_latency"] == pytest.approx(2.0)


def test_pipeline_rollup_counts_failures():
    entries = [
        _entry("etl", healthy=True),
        _entry("etl", healthy=False),
        _entry("etl", healthy=False),
    ]
    with _patch(entries):
        from pipewatch.history import RunHistory
        h = RunHistory(".pipewatch_history.json")
        result = _pipeline_rollup(h, None, 24)
    assert result[0]["failed_runs"] == 2
    assert result[0]["success_rate"] == pytest.approx(33.3, rel=0.01)


def test_pipeline_rollup_filters_by_pipeline():
    entries = [_entry("etl"), _entry("ingest")]
    with _patch(entries):
        from pipewatch.history import RunHistory
        h = RunHistory(".pipewatch_history.json")
        result = _pipeline_rollup(h, "etl", 24)
    assert all(r["pipeline"] == "etl" for r in result)


def test_pipeline_rollup_excludes_old_entries():
    entries = [_entry("etl", offset=999999)]
    with _patch(entries):
        from pipewatch.history import RunHistory
        h = RunHistory(".pipewatch_history.json")
        result = _pipeline_rollup(h, None, 1)
    assert result == []


# ---------------------------------------------------------------------------
# integration tests for run_rollup_cmd
# ---------------------------------------------------------------------------

def test_run_rollup_cmd_no_entries(capsys):
    with _patch([]):
        code = run_rollup_cmd(_args())
    assert code == 0
    assert "No history" in capsys.readouterr().out


def test_run_rollup_cmd_text_output(capsys):
    entries = [_entry("etl", healthy=True, error_rate=0.05, latency=1.5)]
    with _patch(entries):
        code = run_rollup_cmd(_args())
    out = capsys.readouterr().out
    assert code == 0
    assert "etl" in out
    assert "100.0" in out


def test_run_rollup_cmd_json_output(capsys):
    entries = [_entry("etl", healthy=False, error_rate=0.2, latency=3.0)]
    with _patch(entries):
        code = run_rollup_cmd(_args(json=True))
    out = capsys.readouterr().out
    assert code == 0
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "etl"
    assert data[0]["failed_runs"] == 1
