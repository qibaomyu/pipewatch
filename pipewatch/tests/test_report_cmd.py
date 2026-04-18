"""Tests for report_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.history import RunHistory, HistoryEntry
from pipewatch.commands.report_cmd import run_report_cmd


@pytest.fixture()
def history_path(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


def _args(history_path: Path, fmt: str = "text", hours: int = 24, exit_code: bool = False) -> Namespace:
    return Namespace(history_file=str(history_path), format=fmt, hours=hours, exit_code=exit_code)


def _add(history_path: Path, pipeline: str, healthy: bool, hours_ago: float = 1.0):
    from datetime import timedelta
    h = RunHistory(path=str(history_path))
    ts = datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
    h.record(HistoryEntry(pipeline=pipeline, timestamp=ts, healthy=healthy, alerts=[]))


def test_empty_history_prints_message(history_path, capsys):
    rc = run_report_cmd(_args(history_path))
    out = capsys.readouterr().out
    assert "No history" in out
    assert rc == 0


def test_text_output_shows_pipelines(history_path, capsys):
    _add(history_path, "pipe_a", healthy=True)
    _add(history_path, "pipe_a", healthy=False)
    _add(history_path, "pipe_b", healthy=True)
    rc = run_report_cmd(_args(history_path))
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "pipe_b" in out
    assert rc == 0


def test_json_output(history_path, capsys):
    _add(history_path, "pipe_a", healthy=False)
    run_report_cmd(_args(history_path, fmt="json"))
    data = json.loads(capsys.readouterr().out)
    assert "pipelines" in data
    assert data["pipelines"][0]["pipeline"] == "pipe_a"


def test_exit_code_when_failures(history_path):
    _add(history_path, "pipe_a", healthy=False)
    rc = run_report_cmd(_args(history_path, exit_code=True))
    assert rc == 1


def test_no_exit_code_when_all_healthy(history_path):
    _add(history_path, "pipe_a", healthy=True)
    rc = run_report_cmd(_args(history_path, exit_code=True))
    assert rc == 0


def test_window_filters_old_entries(history_path, capsys):
    _add(history_path, "pipe_old", healthy=False, hours_ago=48)
    rc = run_report_cmd(_args(history_path, hours=24))
    out = capsys.readouterr().out
    assert "No history" in out
    assert rc == 0
