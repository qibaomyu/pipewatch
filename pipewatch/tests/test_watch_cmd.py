"""Tests for watch_cmd."""
from __future__ import annotations

import types
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.watch_cmd import run_watch_cmd
from pipewatch.runner import RunResult
from pipewatch.monitor import PipelineStatus


def _args(**kwargs):
    defaults = dict(
        config="pipewatch.yaml",
        pipeline=None,
        interval=0.0,
        count=1,
        format="text",
        exit_code=False,
    )
    defaults.update(kwargs)
    return types.SimpleNamespace(**defaults)


def _make_result(healthy: bool) -> RunResult:
    status = PipelineStatus(name="pipe", healthy=healthy, error_rate=0.0, latency_p99=0.0)
    return RunResult(status=status, alerts=[])


@patch("pipewatch.commands.watch_cmd.time.sleep")
@patch("pipewatch.commands.watch_cmd.format_results", return_value="ok")
@patch("pipewatch.commands.watch_cmd.PipelineRunner")
@patch("pipewatch.commands.watch_cmd.load_config")
def test_watch_runs_count_iterations(mock_cfg, mock_runner_cls, mock_fmt, mock_sleep):
    cfg = MagicMock()
    cfg.pipelines = [MagicMock(name="pipe")]
    mock_cfg.return_value = cfg

    runner = MagicMock()
    runner.run.return_value = _make_result(True)
    mock_runner_cls.return_value = runner

    rc = run_watch_cmd(_args(count=3))

    assert rc == 0
    assert runner.run.call_count == 3
    assert mock_sleep.call_count == 2  # sleep between iterations, not after last


@patch("pipewatch.commands.watch_cmd.load_config", side_effect=FileNotFoundError)
def test_watch_missing_config_returns_2(mock_cfg):
    rc = run_watch_cmd(_args())
    assert rc == 2


@patch("pipewatch.commands.watch_cmd.time.sleep")
@patch("pipewatch.commands.watch_cmd.format_results", return_value="ok")
@patch("pipewatch.commands.watch_cmd.PipelineRunner")
@patch("pipewatch.commands.watch_cmd.load_config")
def test_watch_exit_code_unhealthy(mock_cfg, mock_runner_cls, mock_fmt, mock_sleep):
    cfg = MagicMock()
    cfg.pipelines = [MagicMock()]
    mock_cfg.return_value = cfg

    runner = MagicMock()
    runner.run.return_value = _make_result(False)
    mock_runner_cls.return_value = runner

    rc = run_watch_cmd(_args(count=1, exit_code=True))
    assert rc == 1


@patch("pipewatch.commands.watch_cmd.load_config")
def test_watch_unknown_pipeline_returns_2(mock_cfg):
    cfg = MagicMock()
    p = MagicMock()
    p.name = "real_pipe"
    cfg.pipelines = [p]
    mock_cfg.return_value = cfg

    rc = run_watch_cmd(_args(pipeline=["ghost"], count=1))
    assert rc == 2
