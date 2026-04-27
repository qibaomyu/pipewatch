"""Tests for throughput_rate_cmd."""
from __future__ import annotations

import datetime
import json
from argparse import Namespace
from unittest.mock import patch

import pytest

from pipewatch.commands.throughput_rate_cmd import (
    _pipeline_throughput_rate,
    run_throughput_rate_cmd,
    register_throughput_rate_subcommand,
)
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool, hours_ago: float = 1.0) -> HistoryEntry:
    ts = datetime.datetime.utcnow() - datetime.timedelta(hours=hours_ago)
    return HistoryEntry(pipeline=pipeline, timestamp=ts, healthy=healthy, alerts=[], latency=0.1, error_rate=0.0)


def _args(**kwargs):
    defaults = dict(hours=24, pipeline=None, json=False, history_file=".pipewatch_history.json")
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.throughput_rate_cmd.RunHistory.all", return_value=entries)


def test_pipeline_throughput_rate_no_entries():
    result = _pipeline_throughput_rate([], None, 24)
    assert result == []


def test_pipeline_throughput_rate_single_pipeline():
    entries = [_entry("pipe_a", True) for _ in range(6)]
    result = _pipeline_throughput_rate(entries, None, 24)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"
    assert result[0]["runs"] == 6
    assert result[0]["rate"] == pytest.approx(6 / 24, rel=1e-3)


def test_pipeline_throughput_rate_multiple_pipelines():
    entries = [_entry("pipe_a", True)] * 3 + [_entry("pipe_b", False)] * 9
    result = _pipeline_throughput_rate(entries, None, 24)
    names = [r["pipeline"] for r in result]
    assert "pipe_a" in names
    assert "pipe_b" in names
    b = next(r for r in result if r["pipeline"] == "pipe_b")
    assert b["runs"] == 9


def test_pipeline_throughput_rate_filter_by_pipeline():
    entries = [_entry("pipe_a", True)] * 4 + [_entry("pipe_b", True)] * 10
    result = _pipeline_throughput_rate(entries, "pipe_a", 24)
    assert len(result) == 1
    assert result[0]["pipeline"] == "pipe_a"
    assert result[0]["runs"] == 4


def test_pipeline_throughput_rate_excludes_old_entries():
    old = _entry("pipe_a", True, hours_ago=48)
    recent = _entry("pipe_a", True, hours_ago=1)
    result = _pipeline_throughput_rate([old, recent], None, 24)
    assert result[0]["runs"] == 1


def test_run_throughput_rate_cmd_no_data(capsys):
    with _patch([]):
        code = run_throughput_rate_cmd(_args())
    assert code == 0
    assert "No data" in capsys.readouterr().out


def test_run_throughput_rate_cmd_text_output(capsys):
    entries = [_entry("pipe_a", True)] * 12
    with _patch(entries):
        code = run_throughput_rate_cmd(_args())
    assert code == 0
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "12" in out


def test_run_throughput_rate_cmd_json_output(capsys):
    entries = [_entry("pipe_x", False)] * 5
    with _patch(entries):
        code = run_throughput_rate_cmd(_args(json=True))
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert data[0]["pipeline"] == "pipe_x"
    assert data[0]["runs"] == 5


def test_register_throughput_rate_subcommand():
    import argparse
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    register_throughput_rate_subcommand(sub)
    args = parser.parse_args(["throughput-rate", "--hours", "12", "--json"])
    assert args.hours == 12
    assert args.json is True
