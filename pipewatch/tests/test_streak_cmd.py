"""Tests for streak_cmd."""
import argparse
import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pipewatch.commands.streak_cmd import _pipeline_streak, run_streak_cmd
from pipewatch.history import HistoryEntry


def _entry(healthy: bool, ts: float = 0.0) -> HistoryEntry:
    return HistoryEntry(
        pipeline="pipe",
        timestamp=datetime.fromtimestamp(ts, tz=timezone.utc),
        healthy=healthy,
        error_rate=0.0,
        latency_p99=0.0,
        alert_count=0,
    )


def _args(**kwargs):
    defaults = {
        "history_file": ".pipewatch_history.json",
        "pipeline": None,
        "json": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_pipeline_streak_empty():
    result = _pipeline_streak([])
    assert result["streak"] == 0
    assert result["state"] is None


def test_pipeline_streak_all_healthy():
    entries = [_entry(True, i) for i in range(4)]
    result = _pipeline_streak(entries)
    assert result == {"streak": 4, "state": "healthy"}


def test_pipeline_streak_all_failing():
    entries = [_entry(False, i) for i in range(3)]
    result = _pipeline_streak(entries)
    assert result == {"streak": 3, "state": "failing"}


def test_pipeline_streak_mixed():
    entries = [_entry(True, 0), _entry(True, 1), _entry(False, 2), _entry(False, 3)]
    result = _pipeline_streak(entries)
    assert result == {"streak": 2, "state": "failing"}


def _patch(monkeypatch, data: dict):
    class FakeHistory:
        def __init__(self, _): pass
        def pipelines(self): return list(data.keys())
        def get(self, name): return data.get(name, [])

    monkeypatch.setattr("pipewatch.commands.streak_cmd.RunHistory", FakeHistory)


def test_run_streak_no_history(monkeypatch, capsys):
    _patch(monkeypatch, {})
    rc = run_streak_cmd(_args())
    assert rc == 0
    assert "No history" in capsys.readouterr().out


def test_run_streak_text_output(monkeypatch, capsys):
    entries = [_entry(True, i) for i in range(3)]
    _patch(monkeypatch, {"pipe": entries})
    rc = run_streak_cmd(_args())
    assert rc == 0
    out = capsys.readouterr().out
    assert "pipe" in out
    assert "3" in out
    assert "healthy" in out


def test_run_streak_json_output(monkeypatch, capsys):
    entries = [_entry(False, i) for i in range(2)]
    _patch(monkeypatch, {"pipe": entries})
    rc = run_streak_cmd(_args(json=True))
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert data["pipe"]["streak"] == 2
    assert data["pipe"]["state"] == "failing"


def test_run_streak_unknown_pipeline(monkeypatch, capsys):
    _patch(monkeypatch, {"pipe": []})
    rc = run_streak_cmd(_args(pipeline="other"))
    assert rc == 2
