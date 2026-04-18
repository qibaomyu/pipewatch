"""Tests for the summary command."""
from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path
from datetime import datetime, timezone

import pytest

from pipewatch.history import RunHistory, HistoryEntry
from pipewatch.commands.summary_cmd import run_summary_cmd


@pytest.fixture()
def history_path(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


def _args(history_path: Path, fmt: str = "text", last: int = 50) -> Namespace:
    return Namespace(history_file=str(history_path), format=fmt, last=last)


def _add(history: RunHistory, pipeline: str, status: str) -> None:
    history.record(
        HistoryEntry(
            pipeline=pipeline,
            timestamp=datetime.now(timezone.utc).isoformat(),
            status=status,
            alerts=[],
        )
    )


def test_empty_history_prints_message(history_path, capsys):
    code = run_summary_cmd(_args(history_path))
    assert code == 0
    assert "No history" in capsys.readouterr().out


def test_text_output_shows_pipeline(history_path, capsys):
    h = RunHistory(str(history_path))
    _add(h, "etl", "healthy")
    _add(h, "etl", "failing")
    code = run_summary_cmd(_args(history_path))
    assert code == 0
    out = capsys.readouterr().out
    assert "etl" in out
    assert "2" in out


def test_json_output_structure(history_path):
    h = RunHistory(str(history_path))
    _add(h, "pipe-a", "healthy")
    _add(h, "pipe-a", "warning")
    _add(h, "pipe-a", "failing")
    code = run_summary_cmd(_args(history_path, fmt="json"))
    assert code == 0


def test_failure_rate_calculation(history_path, capsys):
    h = RunHistory(str(history_path))
    for _ in range(3):
        _add(h, "pipe-b", "failing")
    _add(h, "pipe-b", "healthy")
    run_summary_cmd(_args(history_path, fmt="json"))
    # re-run capturing json
    import io, sys
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    run_summary_cmd(_args(history_path, fmt="json"))
    sys.stdout = old
    data = json.loads(buf.getvalue())
    entry = data[0]
    assert entry["failures"] == 3
    assert entry["failure_rate"] == 0.75


def test_multiple_pipelines_sorted(history_path, capsys):
    h = RunHistory(str(history_path))
    _add(h, "zzz", "healthy")
    _add(h, "aaa", "healthy")
    run_summary_cmd(_args(history_path))
    out = capsys.readouterr().out
    assert out.index("aaa") < out.index("zzz")
