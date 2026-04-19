"""Tests for sla_cmd."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

from pipewatch.commands.sla_cmd import _pipeline_sla, run_sla_cmd


def _entry(pipeline, error_rate, latency, hours_ago=1):
    e = MagicMock()
    e.pipeline = pipeline
    e.error_rate = error_rate
    e.latency = latency
    e.timestamp = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return e


def _args(**kwargs):
    defaults = dict(
        pipeline=None,
        hours=24,
        max_error_rate=0.05,
        max_latency=5.0,
        json=False,
        exit_code=False,
        history_file=".pipewatch_history.json",
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _history(*entries):
    h = MagicMock()
    h.entries = list(entries)
    return h


def test_pipeline_sla_no_entries():
    h = _history()
    result = _pipeline_sla(h, pipeline=None, hours=24, max_error_rate=0.05, max_latency=5.0)
    assert result == []


def test_pipeline_sla_all_compliant():
    h = _history(
        _entry("pipe_a", 0.01, 1.0),
        _entry("pipe_a", 0.02, 2.0),
    )
    result = _pipeline_sla(h, pipeline=None, hours=24, max_error_rate=0.05, max_latency=5.0)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"
    assert result[0]["breaches"] == 0
    assert result[0]["compliance_pct"] == 100.0


def test_pipeline_sla_with_breach():
    h = _history(
        _entry("pipe_b", 0.10, 1.0),
        _entry("pipe_b", 0.01, 1.0),
        _entry("pipe_b", 0.01, 1.0),
        _entry("pipe_b", 0.01, 1.0),
    )
    result = _pipeline_sla(h, pipeline=None, hours=24, max_error_rate=0.05, max_latency=5.0)
    assert result[0]["breaches"] == 1
    assert result[0]["compliance_pct"] == 75.0


def test_pipeline_sla_filters_by_pipeline():
    h = _history(
        _entry("pipe_a", 0.01, 1.0),
        _entry("pipe_b", 0.01, 1.0),
    )
    result = _pipeline_sla(h, pipeline="pipe_a", hours=24, max_error_rate=0.05, max_latency=5.0)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"


def test_pipeline_sla_latency_breach():
    h = _history(
        _entry("pipe_c", 0.01, 10.0),
    )
    result = _pipeline_sla(h, pipeline=None, hours=24, max_error_rate=0.05, max_latency=5.0)
    assert result[0]["breaches"] == 1


def test_run_sla_cmd_exit_code_failure(capsys):
    h = _history(_entry("pipe_x", 0.20, 1.0))
    code = run_sla_cmd(_args(exit_code=True), history=h)
    assert code == 1


def test_run_sla_cmd_exit_code_success(capsys):
    h = _history(_entry("pipe_y", 0.01, 1.0))
    code = run_sla_cmd(_args(exit_code=True), history=h)
    assert code == 0


def test_run_sla_cmd_json_output(capsys):
    import json
    h = _history(_entry("pipe_z", 0.01, 1.0))
    run_sla_cmd(_args(json=True), history=h)
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data[0]["pipeline"] == "pipe_z"


def test_run_sla_cmd_no_data(capsys):
    h = _history()
    code = run_sla_cmd(_args(), history=h)
    assert code == 0
    assert "No SLA data" in capsys.readouterr().out
