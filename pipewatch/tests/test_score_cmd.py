"""Tests for the score command."""
from __future__ import annotations

import json
from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.score_cmd import _pipeline_score, run_score_cmd


def _entry(healthy: bool, error_rate: float = 0.0, latency_p99: float = 1.0):
    e = MagicMock()
    e.healthy = healthy
    e.error_rate = error_rate
    e.latency_p99 = latency_p99
    return e


def _args(**kwargs):
    defaults = {
        "pipeline": None,
        "hours": 24,
        "history_file": ".pipewatch_history.json",
        "json": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def test_pipeline_score_no_entries():
    r = _pipeline_score([])
    assert r["score"] is None
    assert r["grade"] == "N/A"
    assert r["samples"] == 0


def test_pipeline_score_all_healthy():
    entries = [_entry(True) for _ in range(10)]
    r = _pipeline_score(entries)
    assert r["score"] > 90
    assert r["grade"] == "A"
    assert r["samples"] == 10


def test_pipeline_score_all_failing():
    entries = [_entry(False, error_rate=0.9, latency_p99=120.0) for _ in range(10)]
    r = _pipeline_score(entries)
    assert r["score"] < 40
    assert r["grade"] == "F"


def test_pipeline_score_mixed():
    entries = [_entry(i % 2 == 0, error_rate=0.1) for i in range(10)]
    r = _pipeline_score(entries)
    assert 0 < r["score"] < 100
    assert r["grade"] in {"A", "B", "C", "D", "F"}


def _patch(entries_map: dict):
    mock_history = MagicMock()
    mock_history.pipelines.return_value = list(entries_map.keys())
    mock_history.get.side_effect = lambda name, hours: entries_map.get(name, [])
    return patch("pipewatch.commands.score_cmd.RunHistory", return_value=mock_history)


def test_run_score_cmd_text_output(capsys):
    entries = {"pipe_a": [_entry(True) for _ in range(5)]}
    with _patch(entries):
        rc = run_score_cmd(_args())
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert rc == 0


def test_run_score_cmd_json_output(capsys):
    entries = {"pipe_a": [_entry(True) for _ in range(5)]}
    with _patch(entries):
        rc = run_score_cmd(_args(json=True))
    data = json.loads(capsys.readouterr().out)
    assert "pipe_a" in data
    assert data["pipe_a"]["score"] is not None


def test_run_score_cmd_exit_code_failing():
    entries = {"bad": [_entry(False, error_rate=0.9, latency_p99=120.0) for _ in range(5)]}
    with _patch(entries):
        rc = run_score_cmd(_args(exit_code=True))
    assert rc == 1


def test_run_score_cmd_no_history(capsys):
    mock_history = MagicMock()
    mock_history.pipelines.return_value = []
    with patch("pipewatch.commands.score_cmd.RunHistory", return_value=mock_history):
        rc = run_score_cmd(_args())
    assert rc == 0
    assert "No history" in capsys.readouterr().out
