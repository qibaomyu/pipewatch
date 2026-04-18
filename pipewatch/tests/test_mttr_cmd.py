"""Tests for the MTTR command."""
import json
import argparse
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.commands.mttr_cmd import _pipeline_mttr, _format_text, run_mttr_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool, offset_seconds: int = 0) -> HistoryEntry:
    ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds)
    return HistoryEntry(pipeline=pipeline, timestamp=ts, healthy=healthy, error_rate=0.0, latency=0.0)


def _args(pipeline=None, json_out=False, history_file=".pipewatch_history.json"):
    ns = argparse.Namespace()
    ns.pipeline = pipeline
    ns.json = json_out
    ns.history_file = history_file
    return ns


def test_pipeline_mttr_no_recovery():
    entries = [_entry("p", False, 0), _entry("p", False, 60)]
    assert _pipeline_mttr(entries) is None


def test_pipeline_mttr_single_recovery():
    entries = [_entry("p", False, 0), _entry("p", True, 120)]
    assert _pipeline_mttr(entries) == pytest.approx(120.0)


def test_pipeline_mttr_multiple_recoveries():
    entries = [
        _entry("p", False, 0),
        _entry("p", True, 60),
        _entry("p", False, 120),
        _entry("p", True, 300),
    ]
    # recoveries: 60s and 180s -> mean = 120s
    assert _pipeline_mttr(entries) == pytest.approx(120.0)


def test_pipeline_mttr_empty():
    assert _pipeline_mttr([]) is None


def test_format_text_no_data():
    out = _format_text({"pipe": None})
    assert "no recovery data" in out


def test_format_text_with_value():
    out = _format_text({"pipe": 95.5})
    assert "95.5s" in out


def _patch(entries):
    return patch("pipewatch.commands.mttr_cmd.RunHistory.all", return_value=entries)


def test_run_mttr_text_output(capsys):
    entries = [_entry("etl", False, 0), _entry("etl", True, 200)]
    with _patch(entries):
        code = run_mttr_cmd(_args())
    assert code == 0
    captured = capsys.readouterr().out
    assert "etl" in captured
    assert "200.0s" in captured


def test_run_mttr_json_output(capsys):
    entries = [_entry("etl", False, 0), _entry("etl", True, 300)]
    with _patch(entries):
        code = run_mttr_cmd(_args(json_out=True))
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert "etl" in data
    assert data["etl"] == pytest.approx(300.0)


def test_run_mttr_pipeline_filter(capsys):
    entries = [
        _entry("etl", False, 0), _entry("etl", True, 100),
        _entry("other", False, 0), _entry("other", True, 500),
    ]
    with _patch(entries):
        run_mttr_cmd(_args(pipeline="etl", json_out=True))
    data = json.loads(capsys.readouterr().out)
    assert "etl" in data
    assert "other" not in data
