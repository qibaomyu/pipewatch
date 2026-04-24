"""Tests for the dead-letter command."""
from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.deadletter_cmd import _pipeline_deadletter, run_deadletter_cmd


def _entry(pipeline: str, healthy: bool, ts: float = 0.0):
    e = MagicMock()
    e.pipeline = pipeline
    e.healthy = healthy
    e.timestamp = ts
    return e


def _args(**kwargs):
    defaults = dict(
        pipeline=None,
        threshold=5,
        history_file=".pipewatch_history.json",
        json=False,
        exit_code=False,
    )
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.deadletter_cmd.RunHistory",
        return_value=MagicMock(all=MagicMock(return_value=entries)),
    )


# --- unit tests for _pipeline_deadletter ---


def test_pipeline_deadletter_no_entries():
    result = _pipeline_deadletter([], "pipe_a", threshold=5)
    assert result["dead"] is False
    assert result["consecutive_failures"] == 0


def test_pipeline_deadletter_below_threshold():
    entries = [_entry("pipe_a", False, i) for i in range(3)]
    result = _pipeline_deadletter(entries, "pipe_a", threshold=5)
    assert result["consecutive_failures"] == 3
    assert result["dead"] is False


def test_pipeline_deadletter_at_threshold():
    entries = [_entry("pipe_a", False, i) for i in range(5)]
    result = _pipeline_deadletter(entries, "pipe_a", threshold=5)
    assert result["consecutive_failures"] == 5
    assert result["dead"] is True


def test_pipeline_deadletter_resets_on_success():
    entries = [
        _entry("pipe_a", False, 0),
        _entry("pipe_a", False, 1),
        _entry("pipe_a", True, 2),
        _entry("pipe_a", False, 3),
    ]
    result = _pipeline_deadletter(entries, "pipe_a", threshold=5)
    assert result["consecutive_failures"] == 1
    assert result["max_consecutive"] == 2
    assert result["dead"] is False


def test_pipeline_deadletter_filters_other_pipelines():
    entries = [
        _entry("pipe_a", False, i) for i in range(6)
    ] + [
        _entry("pipe_b", False, i) for i in range(6)
    ]
    result = _pipeline_deadletter(entries, "pipe_a", threshold=5)
    assert result["consecutive_failures"] == 6


# --- integration tests for run_deadletter_cmd ---


def test_run_no_history_prints_message(capsys):
    with _patch([]):
        rc = run_deadletter_cmd(_args())
    out = capsys.readouterr().out
    assert "No history" in out
    assert rc == 0


def test_run_text_output_dead_pipeline(capsys):
    entries = [_entry("pipe_a", False, i) for i in range(6)]
    with _patch(entries):
        rc = run_deadletter_cmd(_args(threshold=5))
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "DEAD" in out


def test_run_json_output(capsys):
    entries = [_entry("pipe_a", False, i) for i in range(6)]
    with _patch(entries):
        rc = run_deadletter_cmd(_args(threshold=5, json=True))
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["dead"] is True


def test_run_exit_code_when_dead():
    entries = [_entry("pipe_a", False, i) for i in range(6)]
    with _patch(entries):
        rc = run_deadletter_cmd(_args(threshold=5, exit_code=True))
    assert rc == 1


def test_run_exit_code_zero_when_healthy():
    entries = [_entry("pipe_a", True, i) for i in range(6)]
    with _patch(entries):
        rc = run_deadletter_cmd(_args(threshold=5, exit_code=True))
    assert rc == 0
