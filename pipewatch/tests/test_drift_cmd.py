"""Tests for drift_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from unittest.mock import patch

import pytest

from pipewatch.commands.drift_cmd import _format_text, _pipeline_drift, run_drift_cmd
from pipewatch.config import AppConfig, PipelineConfig


def _make_cfg(name="pipe", max_error_rate=0.05, max_latency_seconds=30.0, min_throughput=10):
    pipeline = PipelineConfig(
        name=name,
        max_error_rate=max_error_rate,
        max_latency_seconds=max_latency_seconds,
        min_throughput=min_throughput,
    )
    return AppConfig(pipelines=[pipeline])


def _args(**kwargs):
    defaults = {
        "config": "pipewatch.yaml",
        "baseline_file": ".pipewatch_baselines.json",
        "pipeline": None,
        "json": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(cfg, baseline):
    return (
        patch("pipewatch.commands.drift_cmd.load_config", return_value=cfg),
        patch("pipewatch.commands.drift_cmd.get_baseline", return_value=baseline),
    )


def test_pipeline_drift_no_baseline():
    cfg = _make_cfg()
    with patch("pipewatch.commands.drift_cmd.get_baseline", return_value=None):
        result = _pipeline_drift("pipe", cfg, ".baselines.json")
    assert result["error"] == "no baseline recorded"


def test_pipeline_drift_not_in_config():
    cfg = _make_cfg()
    result = _pipeline_drift("missing", cfg, ".baselines.json")
    assert result["error"] == "not found in config"


def test_pipeline_drift_no_changes():
    cfg = _make_cfg(max_error_rate=0.05, max_latency_seconds=30.0, min_throughput=10)
    baseline = {"max_error_rate": 0.05, "max_latency_seconds": 30.0, "min_throughput": 10}
    with patch("pipewatch.commands.drift_cmd.get_baseline", return_value=baseline):
        result = _pipeline_drift("pipe", cfg, ".baselines.json")
    assert result["drifts"] == []


def test_pipeline_drift_detects_changes():
    cfg = _make_cfg(max_error_rate=0.10)
    baseline = {"max_error_rate": 0.05, "max_latency_seconds": 30.0, "min_throughput": 10}
    with patch("pipewatch.commands.drift_cmd.get_baseline", return_value=baseline):
        result = _pipeline_drift("pipe", cfg, ".baselines.json")
    assert len(result["drifts"]) == 1
    assert result["drifts"][0]["field"] == "max_error_rate"
    assert result["drifts"][0]["baseline"] == 0.05
    assert result["drifts"][0]["current"] == 0.10


def test_format_text_no_drift():
    results = [{"pipeline": "pipe", "drifts": []}]
    out = _format_text(results)
    assert "no drift" in out


def test_format_text_with_drift():
    results = [{"pipeline": "pipe", "drifts": [{"field": "max_error_rate", "baseline": 0.05, "current": 0.1}]}]
    out = _format_text(results)
    assert "max_error_rate" in out
    assert "0.05" in out
    assert "0.1" in out


def test_run_drift_cmd_json(capsys):
    cfg = _make_cfg(max_error_rate=0.10)
    baseline = {"max_error_rate": 0.05}
    p1, p2 = _patch(cfg, baseline)
    with p1, p2:
        rc = run_drift_cmd(_args(json=True))
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert isinstance(data, list)
    assert rc == 0


def test_run_drift_cmd_exit_code_on_drift():
    cfg = _make_cfg(max_error_rate=0.10)
    baseline = {"max_error_rate": 0.05}
    p1, p2 = _patch(cfg, baseline)
    with p1, p2:
        rc = run_drift_cmd(_args(exit_code=True))
    assert rc == 1


def test_run_drift_cmd_missing_config():
    with patch("pipewatch.commands.drift_cmd.load_config", side_effect=FileNotFoundError):
        rc = run_drift_cmd(_args())
    assert rc == 2
