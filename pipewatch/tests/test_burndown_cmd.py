"""Tests for burndown_cmd."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from pipewatch.commands.burndown_cmd import _bucket_burndown, run_burndown_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool, minutes_ago: float) -> HistoryEntry:
    ts = datetime.now(tz=timezone.utc) - timedelta(minutes=minutes_ago)
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=ts,
        healthy=healthy,
        error_rate=0.0 if healthy else 0.5,
        latency_p99=1.0,
        alert_count=0 if healthy else 1,
    )


def _args(**kwargs):
    defaults = dict(
        hours=24,
        buckets=4,
        pipeline=None,
        json=False,
        history_file=".pipewatch_history.json",
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# _bucket_burndown unit tests
# ---------------------------------------------------------------------------

def test_bucket_burndown_empty():
    result = _bucket_burndown([], hours=24, buckets=4)
    assert len(result) == 4
    assert all(b["failing_count"] == 0 for b in result)


def test_bucket_burndown_counts_failing():
    entries = [
        _entry("pipe_a", healthy=False, minutes_ago=10),
        _entry("pipe_b", healthy=False, minutes_ago=10),
        _entry("pipe_a", healthy=True, minutes_ago=10),  # healthy — should not count
    ]
    result = _bucket_burndown(entries, hours=1, buckets=2)
    last_bucket = result[-1]
    assert last_bucket["failing_count"] == 2
    assert "pipe_a" in last_bucket["failing_pipelines"]
    assert "pipe_b" in last_bucket["failing_pipelines"]


def test_bucket_burndown_deduplicates_per_slot():
    """Same pipeline failing twice in the same slot counts once."""
    entries = [
        _entry("pipe_a", healthy=False, minutes_ago=5),
        _entry("pipe_a", healthy=False, minutes_ago=8),
    ]
    result = _bucket_burndown(entries, hours=1, buckets=2)
    last_bucket = result[-1]
    assert last_bucket["failing_count"] == 1


def test_bucket_burndown_slot_count():
    result = _bucket_burndown([], hours=12, buckets=6)
    assert len(result) == 6


# ---------------------------------------------------------------------------
# run_burndown_cmd integration tests
# ---------------------------------------------------------------------------

def _patch(entries):
    return patch(
        "pipewatch.commands.burndown_cmd.RunHistory.all",
        return_value=entries,
    )


def test_run_burndown_no_failures_prints_message(capsys):
    with _patch([_entry("pipe_a", healthy=True, minutes_ago=30)]):
        rc = run_burndown_cmd(_args())
    assert rc == 0
    assert "No failures" in capsys.readouterr().out


def test_run_burndown_text_output(capsys):
    entries = [_entry("pipe_x", healthy=False, minutes_ago=10)]
    with _patch(entries):
        rc = run_burndown_cmd(_args(hours=1, buckets=2))
    assert rc == 0
    out = capsys.readouterr().out
    assert "pipe_x" in out


def test_run_burndown_json_output(capsys):
    entries = [_entry("pipe_y", healthy=False, minutes_ago=10)]
    with _patch(entries):
        rc = run_burndown_cmd(_args(hours=1, buckets=2, json=True))
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert any(b["failing_count"] > 0 for b in data)


def test_run_burndown_pipeline_filter(capsys):
    entries = [
        _entry("pipe_a", healthy=False, minutes_ago=5),
        _entry("pipe_b", healthy=False, minutes_ago=5),
    ]
    with _patch(entries):
        rc = run_burndown_cmd(_args(hours=1, buckets=2, pipeline="pipe_a", json=True))
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    for bucket in data:
        assert "pipe_b" not in bucket["failing_pipelines"]
