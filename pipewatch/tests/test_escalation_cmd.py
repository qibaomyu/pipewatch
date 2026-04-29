"""Unit tests for escalation_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.commands.escalation_cmd import (
    DEFAULT_ESCALATION_FILE,
    _pipeline_escalation,
    record_escalation,
    run_escalation_cmd,
)


def _entry(hours_ago: float = 1.0, level: str = "critical") -> dict:
    ts = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()
    return {"ts": ts, "level": level}


@pytest.fixture()
def escalation_file(tmp_path: Path) -> Path:
    return tmp_path / "escalations.json"


def _args(escalation_file, **kwargs):
    import argparse
    defaults = dict(
        hours=24,
        threshold=3,
        pipeline=None,
        json=False,
        exit_code=False,
        escalation_file=str(escalation_file),
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _patch(escalation_file, data):
    return patch(
        "pipewatch.commands.escalation_cmd._load_escalations",
        return_value=data,
    )


# --- _pipeline_escalation ---

def test_pipeline_escalation_no_entries():
    result = _pipeline_escalation("p", [], 24, 3)
    assert result["count"] == 0
    assert result["escalated"] is False


def test_pipeline_escalation_below_threshold():
    events = [_entry(1), _entry(2)]
    result = _pipeline_escalation("p", events, 24, 3)
    assert result["count"] == 2
    assert result["escalated"] is False


def test_pipeline_escalation_at_threshold():
    events = [_entry(1), _entry(2), _entry(3)]
    result = _pipeline_escalation("p", events, 24, 3)
    assert result["count"] == 3
    assert result["escalated"] is True


def test_pipeline_escalation_ignores_old_entries():
    events = [_entry(1), _entry(2), _entry(30)]  # last one is outside 24h window
    result = _pipeline_escalation("p", events, 24, 3)
    assert result["count"] == 2
    assert result["escalated"] is False


# --- run_escalation_cmd ---

def test_run_escalation_cmd_no_data(escalation_file, capsys):
    with _patch(escalation_file, {}):
        code = run_escalation_cmd(_args(escalation_file))
    assert code == 0
    assert "No escalation data" in capsys.readouterr().out


def test_run_escalation_cmd_text_output(escalation_file, capsys):
    data = {"pipe_a": [_entry(1), _entry(2), _entry(3)]}
    with _patch(escalation_file, data):
        code = run_escalation_cmd(_args(escalation_file))
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "ESCALATED" in out
    assert code == 0


def test_run_escalation_cmd_exit_code_when_escalated(escalation_file):
    data = {"pipe_a": [_entry(1), _entry(2), _entry(3)]}
    with _patch(escalation_file, data):
        code = run_escalation_cmd(_args(escalation_file, exit_code=True))
    assert code == 1


def test_run_escalation_cmd_json_output(escalation_file, capsys):
    data = {"pipe_a": [_entry(1)]}
    with _patch(escalation_file, data):
        run_escalation_cmd(_args(escalation_file, json=True))
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert isinstance(parsed, list)
    assert parsed[0]["pipeline"] == "pipe_a"


def test_run_escalation_cmd_pipeline_filter(escalation_file, capsys):
    data = {"pipe_a": [_entry(1)], "pipe_b": [_entry(1)]}
    with _patch(escalation_file, data):
        run_escalation_cmd(_args(escalation_file, pipeline="pipe_a"))
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "pipe_b" not in out


# --- record_escalation ---

def test_record_escalation_creates_entry(escalation_file):
    record_escalation("pipe_x", "warning", path=str(escalation_file))
    data = json.loads(escalation_file.read_text())
    assert "pipe_x" in data
    assert data["pipe_x"][0]["level"] == "warning"


def test_record_escalation_appends(escalation_file):
    record_escalation("pipe_x", "critical", path=str(escalation_file))
    record_escalation("pipe_x", "warning", path=str(escalation_file))
    data = json.loads(escalation_file.read_text())
    assert len(data["pipe_x"]) == 2
