"""Tests for digest_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.commands.digest_cmd import _pipeline_digest, run_digest_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline="pipe1", healthy=True, latency=1.0, error_rate=0.01, hours_ago=1):
    from datetime import timedelta
    ts = datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
    return HistoryEntry(pipeline=pipeline, timestamp=ts, healthy=healthy,
                        latency=latency, error_rate=error_rate, alerts=[])


def _args(**kwargs):
    defaults = dict(history_file="h.json", hours=24, pipeline=None, format="text")
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries):
    m = MagicMock()
    m.all.return_value = entries
    return patch("pipewatch.commands.digest_cmd.RunHistory", return_value=m)


def test_pipeline_digest_computes_correctly():
    entries = [_entry(healthy=True, latency=2.0, error_rate=0.1),
               _entry(healthy=False, latency=4.0, error_rate=0.2)]
    result = _pipeline_digest(entries, "pipe1")
    assert result["total_runs"] == 2
    assert result["failures"] == 1
    assert result["failure_rate"] == 0.5
    assert result["avg_latency"] == 3.0
    assert result["avg_error_rate"] == 0.15


def test_pipeline_digest_no_entries():
    assert _pipeline_digest([], "pipe1") is None


def test_empty_history_prints_message(capsys):
    with _patch([]):
        rc = run_digest_cmd(_args())
    assert rc == 0
    assert "No data" in capsys.readouterr().out


def test_text_output(capsys):
    entries = [_entry("pipe1", healthy=True), _entry("pipe1", healthy=False)]
    with _patch(entries):
        rc = run_digest_cmd(_args())
    out = capsys.readouterr().out
    assert rc == 0
    assert "pipe1" in out
    assert "1/2 failures" in out


def test_json_output(capsys):
    entries = [_entry("pipe1")]
    with _patch(entries):
        rc = run_digest_cmd(_args(format="json"))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe1"


def test_filter_by_pipeline(capsys):
    entries = [_entry("pipe1"), _entry("pipe2")]
    with _patch(entries):
        rc = run_digest_cmd(_args(pipeline="pipe1", format="json"))
    data = json.loads(capsys.readouterr().out)
    assert all(d["pipeline"] == "pipe1" for d in data)
