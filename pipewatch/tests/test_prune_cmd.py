"""Tests for pipewatch.commands.prune_cmd."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from pipewatch.history import RunHistory, HistoryEntry
from pipewatch.commands.prune_cmd import run_prune_cmd


@pytest.fixture()
def history_path(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


def _args(history_path: Path, days: int = 7, pipeline: str | None = None, dry_run: bool = False):
    return argparse.Namespace(
        history_file=str(history_path),
        days=days,
        pipeline=pipeline,
        dry_run=dry_run,
    )


def _add_entry(history: RunHistory, pipeline: str, age_days: int, healthy: bool = True):
    ts = datetime.utcnow() - timedelta(days=age_days)
    entry = HistoryEntry(
        pipeline=pipeline,
        timestamp=ts.isoformat(),
        healthy=healthy,
        alert_count=0,
        alerts=[],
    )
    history.record(entry)


def test_prune_no_history_file(tmp_path: Path, capsys):
    missing = tmp_path / "nope.json"
    result = run_prune_cmd(_args(missing))
    assert result == 0
    assert "nothing to prune" in capsys.readouterr().out


def test_prune_removes_old_entries(history_path: Path, capsys):
    h = RunHistory(history_path)
    _add_entry(h, "etl", age_days=10)
    _add_entry(h, "etl", age_days=2)

    result = run_prune_cmd(_args(history_path, days=7))
    assert result == 0
    remaining = RunHistory(history_path).get(pipeline="etl")
    assert len(remaining) == 1
    out = capsys.readouterr().out
    assert "1" in out


def test_prune_dry_run_does_not_persist(history_path: Path, capsys):
    h = RunHistory(history_path)
    _add_entry(h, "etl", age_days=10)
    _add_entry(h, "etl", age_days=2)

    result = run_prune_cmd(_args(history_path, days=7, dry_run=True))
    assert result == 0
    out = capsys.readouterr().out
    assert "dry-run" in out


def test_prune_filter_by_pipeline(history_path: Path):
    h = RunHistory(history_path)
    _add_entry(h, "etl", age_days=10)
    _add_entry(h, "reporting", age_days=10)

    run_prune_cmd(_args(history_path, days=7, pipeline="etl"))
    assert len(RunHistory(history_path).get(pipeline="etl")) == 0
    assert len(RunHistory(history_path).get(pipeline="reporting")) == 1
