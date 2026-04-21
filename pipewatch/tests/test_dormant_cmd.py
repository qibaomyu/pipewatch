"""Tests for dormant_cmd."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.dormant_cmd import _pipeline_dormant, run_dormant_cmd
from pipewatch.history import HistoryEntry


NOW = datetime.now(tz=timezone.utc).timestamp()


def _entry(pipeline: str, offset_hours: float, healthy: bool = True) -> HistoryEntry:
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=NOW - offset_hours * 3600,
        healthy=healthy,
        error_rate=0.0,
        latency=1.0,
        alerts=[],
    )


def _args(**kwargs) -> argparse.Namespace:
    defaults = dict(
        config="pipewatch.yaml",
        history_file=".pipewatch_history.json",
        hours=24.0,
        pipeline=None,
        only_dormant=False,
        json=False,
        exit_code=False,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _patch(entries_map: dict):
    """Return a context manager patching RunHistory and load_config."""
    mock_history = MagicMock()
    mock_history.get.side_effect = lambda name: entries_map.get(name, [])

    mock_cfg = MagicMock()
    mock_cfg.pipelines = [MagicMock(name=n) for n in entries_map]
    for p, n in zip(mock_cfg.pipelines, entries_map):
        p.name = n

    return mock_history, mock_cfg


def test_pipeline_dormant_no_entries():
    mock_history = MagicMock()
    mock_history.get.return_value = []
    result = _pipeline_dormant("pipe_a", mock_history, hours=24)
    assert result["dormant"] is True
    assert result["last_run"] is None
    assert result["runs_in_window"] == 0


def test_pipeline_dormant_recent_run():
    mock_history = MagicMock()
    mock_history.get.return_value = [_entry("pipe_a", offset_hours=1)]
    result = _pipeline_dormant("pipe_a", mock_history, hours=24)
    assert result["dormant"] is False
    assert result["runs_in_window"] == 1
    assert result["last_run"] is not None


def test_pipeline_dormant_old_run_outside_window():
    mock_history = MagicMock()
    mock_history.get.return_value = [_entry("pipe_a", offset_hours=48)]
    result = _pipeline_dormant("pipe_a", mock_history, hours=24)
    assert result["dormant"] is True
    assert result["runs_in_window"] == 0
    assert result["last_run"] is not None  # has a historical run, just outside window


def test_run_dormant_cmd_text_output(capsys):
    mock_history = MagicMock()
    mock_history.get.return_value = []
    mock_cfg = MagicMock()
    mock_cfg.pipelines = [MagicMock()]
    mock_cfg.pipelines[0].name = "pipe_a"

    with patch("pipewatch.commands.dormant_cmd.load_config", return_value=mock_cfg), \
         patch("pipewatch.commands.dormant_cmd.RunHistory", return_value=mock_history):
        code = run_dormant_cmd(_args())

    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "DORMANT" in out
    assert code == 0


def test_run_dormant_cmd_json_output(capsys):
    mock_history = MagicMock()
    mock_history.get.return_value = [_entry("pipe_b", offset_hours=2)]
    mock_cfg = MagicMock()
    mock_cfg.pipelines = [MagicMock()]
    mock_cfg.pipelines[0].name = "pipe_b"

    with patch("pipewatch.commands.dormant_cmd.load_config", return_value=mock_cfg), \
         patch("pipewatch.commands.dormant_cmd.RunHistory", return_value=mock_history):
        code = run_dormant_cmd(_args(json=True))

    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_b"
    assert data[0]["dormant"] is False
    assert code == 0


def test_run_dormant_cmd_exit_code_when_dormant():
    mock_history = MagicMock()
    mock_history.get.return_value = []
    mock_cfg = MagicMock()
    mock_cfg.pipelines = [MagicMock()]
    mock_cfg.pipelines[0].name = "pipe_c"

    with patch("pipewatch.commands.dormant_cmd.load_config", return_value=mock_cfg), \
         patch("pipewatch.commands.dormant_cmd.RunHistory", return_value=mock_history):
        code = run_dormant_cmd(_args(exit_code=True))

    assert code == 1


def test_run_dormant_cmd_missing_config(capsys):
    with patch("pipewatch.commands.dormant_cmd.load_config", return_value=None):
        code = run_dormant_cmd(_args())
    assert code == 2
