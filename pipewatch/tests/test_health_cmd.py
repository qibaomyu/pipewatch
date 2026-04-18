"""Tests for pipewatch/commands/health_cmd.py"""
from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.commands.health_cmd import _pipeline_health, _format_text, run_health_cmd
from pipewatch.history import RunHistory, HistoryEntry


@pytest.fixture()
def history_path(tmp_path):
    return tmp_path / "hist.json"


def _entry(pipeline, healthy, latency=0.5):
    return HistoryEntry(
        pipeline=pipeline,
        timestamp="2024-01-01T00:00:00",
        healthy=healthy,
        latency=latency,
        error_rate=0.0 if healthy else 0.5,
        alert_count=0,
    )


def test_pipeline_health_no_entries(history_path):
    h = RunHistory(str(history_path))
    result = _pipeline_health("pipe_a", h, window=10)
    assert result["status"] == "unknown"
    assert result["entries"] == 0


def test_pipeline_health_all_healthy(history_path):
    h = RunHistory(str(history_path))
    for _ in range(5):
        h.record(_entry("pipe_a", healthy=True, latency=1.0))
    result = _pipeline_health("pipe_a", h, window=10)
    assert result["status"] == "healthy"
    assert result["error_rate"] == 0.0
    assert result["avg_latency"] == 1.0


def test_pipeline_health_some_failing(history_path):
    h = RunHistory(str(history_path))
    h.record(_entry("pipe_b", healthy=True))
    h.record(_entry("pipe_b", healthy=False, latency=2.0))
    result = _pipeline_health("pipe_b", h, window=10)
    assert result["status"] == "failing"
    assert result["error_rate"] == 0.5


def test_format_text_healthy():
    rows = [{"pipeline": "p", "status": "healthy", "error_rate": 0.0, "avg_latency": 0.1, "entries": 3}]
    out = _format_text(rows)
    assert "✓" in out
    assert "healthy" in out


def test_format_text_unknown():
    rows = [{"pipeline": "p", "status": "unknown", "entries": 0}]
    out = _format_text(rows)
    assert "unknown" in out


def _args(history_file, pipeline=None, window=10, fmt="text", exit_code=False):
    return Namespace(
        config="pipewatch.yaml",
        history_file=str(history_file),
        pipeline=pipeline,
        window=window,
        format=fmt,
        exit_code=exit_code,
    )


def test_run_health_cmd_json_output(history_path, capsys):
    from pipewatch.config import AppConfig, PipelineConfig
    cfg = AppConfig(pipelines=[PipelineConfig(name="pipe_a")])
    h = RunHistory(str(history_path))
    h.record(_entry("pipe_a", healthy=True))
    with patch("pipewatch.commands.health_cmd.load_config", return_value=cfg), \
         patch("pipewatch.commands.health_cmd.RunHistory", return_value=h):
        rc = run_health_cmd(_args(history_path, fmt="json"))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data[0]["pipeline"] == "pipe_a"
    assert rc == 0


def test_run_health_cmd_exit_code_on_failure(history_path, capsys):
    from pipewatch.config import AppConfig, PipelineConfig
    cfg = AppConfig(pipelines=[PipelineConfig(name="pipe_b")])
    h = RunHistory(str(history_path))
    h.record(_entry("pipe_b", healthy=False))
    with patch("pipewatch.commands.health_cmd.load_config", return_value=cfg), \
         patch("pipewatch.commands.health_cmd.RunHistory", return_value=h):
        rc = run_health_cmd(_args(history_path, exit_code=True))
    assert rc == 1
