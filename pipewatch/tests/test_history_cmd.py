"""Tests for pipewatch.commands.history_cmd."""

import pytest

from pipewatch.history import RunHistory, HistoryEntry
from pipewatch.commands.history_cmd import run_history_cmd


@pytest.fixture
def history_path(tmp_path):
    return str(tmp_path / "history.json")


def _populate(path, pipelines=("pipe1", "pipe2"), count=2):
    h = RunHistory(path=path)
    for name in pipelines:
        for i in range(count):
            h.record(HistoryEntry(
                pipeline=name,
                timestamp=f"2024-01-0{i+1}T00:00:00",
                healthy=i % 2 == 0,
                error_rate=0.01 * i,
                latency=float(i + 1),
                alert_count=i,
            ))
    return h


def test_empty_history_prints_message(history_path, capsys):
    code = run_history_cmd(history_path)
    assert code == 0
    out = capsys.readouterr().out
    assert "No history" in out


def test_shows_entries(history_path, capsys):
    _populate(history_path)
    code = run_history_cmd(history_path)
    assert code == 0
    out = capsys.readouterr().out
    assert "pipe1" in out
    assert "pipe2" in out


def test_filter_by_pipeline(history_path, capsys):
    _populate(history_path)
    code = run_history_cmd(history_path, pipeline="pipe1")
    assert code == 0
    out = capsys.readouterr().out
    assert "pipe1" in out
    assert "pipe2" not in out


def test_limit_entries(history_path, capsys):
    _populate(history_path, pipelines=("p",), count=10)
    run_history_cmd(history_path, pipeline="p", limit=3)
    out = capsys.readouterr().out
    assert "3 entries" in out


def test_clear_pipeline(history_path, capsys):
    _populate(history_path)
    code = run_history_cmd(history_path, pipeline="pipe1", clear=True)
    assert code == 0
    h = RunHistory(path=history_path)
    assert h.get("pipe1") == []
    assert len(h.get("pipe2")) > 0


def test_clear_all(history_path, capsys):
    _populate(history_path)
    code = run_history_cmd(history_path, clear=True)
    assert code == 0
    h = RunHistory(path=history_path)
    assert h.get() == []
