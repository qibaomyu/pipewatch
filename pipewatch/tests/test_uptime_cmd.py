"""Tests for uptime_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.commands.uptime_cmd import _pipeline_uptime, _format_text, run_uptime_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool, hours_ago: float = 1.0) -> HistoryEntry:
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return HistoryEntry(pipeline=pipeline, timestamp=ts, healthy=healthy, error_rate=0.0, latency=0.0)


def _args(**kwargs) -> Namespace:
    defaults = dict(
        hours=24.0,
        pipeline=None,
        json=False,
        exit_code=False,
        min_uptime=95.0,
        history_file=".pipewatch_history.json",
    )
    defaults.update(kwargs)
    return Namespace(**defaults)


def test_pipeline_uptime_empty():
    result = _pipeline_uptime([])
    assert result == {"total": 0, "healthy": 0, "uptime_pct": None}


def test_pipeline_uptime_all_healthy():
    entries = [_entry("p", True) for _ in range(4)]
    result = _pipeline_uptime(entries)
    assert result["uptime_pct"] == 100.0
    assert result["total"] == 4


def test_pipeline_uptime_mixed():
    entries = [_entry("p", True)] * 3 + [_entry("p", False)] * 1
    result = _pipeline_uptime(entries)
    assert result["uptime_pct"] == 75.0


def test_format_text_no_data():
    out = _format_text({"pipe": {"total": 0, "healthy": 0, "uptime_pct": None}})
    assert "no data" in out


def test_format_text_with_data():
    out = _format_text({"pipe": {"total": 10, "healthy": 9, "uptime_pct": 90.0}})
    assert "90.0%" in out
    assert "9/10" in out


@pytest.fixture()
def _patch(tmp_path):
    entries = [
        _entry("alpha", True),
        _entry("alpha", True),
        _entry("alpha", False),
        _entry("beta", True),
    ]
    with patch("pipewatch.commands.uptime_cmd.RunHistory") as MockH:
        MockH.return_value.all.return_value = entries
        yield MockH


def test_run_uptime_text_output(_patch, capsys):
    rc = run_uptime_cmd(_args())
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out
    assert rc == 0


def test_run_uptime_json_output(_patch, capsys):
    rc = run_uptime_cmd(_args(json=True))
    data = json.loads(capsys.readouterr().out)
    assert "alpha" in data
    assert data["beta"]["uptime_pct"] == 100.0


def test_run_uptime_exit_code_failing(_patch):
    rc = run_uptime_cmd(_args(exit_code=True, min_uptime=99.0))
    assert rc == 1


def test_run_uptime_no_history(_patch, capsys):
    _patch.return_value.all.return_value = []
    rc = run_uptime_cmd(_args())
    assert "No history" in capsys.readouterr().out
    assert rc == 0
