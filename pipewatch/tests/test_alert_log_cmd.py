"""Tests for the alert-log command."""
from __future__ import annotations

import json
import pytest
from argparse import Namespace
from unittest.mock import patch, MagicMock

from pipewatch.commands.alert_log_cmd import run_alert_log_cmd, _collect_alerts
from pipewatch.history import HistoryEntry


def _entry(pipeline="pipe-a", alerts=None, timestamp="2024-01-01T00:00:00"):
    e = MagicMock(spec=HistoryEntry)
    e.pipeline = pipeline
    e.timestamp = timestamp
    e.alerts = alerts or []
    return e


def _args(**kwargs):
    defaults = {"history_file": ".pipewatch_history.json", "pipeline": None, "json": False}
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.alert_log_cmd.RunHistory", return_value=MagicMock(all=MagicMock(return_value=entries)))


def test_collect_alerts_all_pipelines():
    alerts = [{"level": "CRITICAL", "message": "error rate high"}]
    entries = [_entry("pipe-a", alerts), _entry("pipe-b", alerts)]
    rows = _collect_alerts(entries, None)
    assert len(rows) == 2


def test_collect_alerts_filtered():
    alerts = [{"level": "WARNING", "message": "latency"}]
    entries = [_entry("pipe-a", alerts), _entry("pipe-b", alerts)]
    rows = _collect_alerts(entries, "pipe-a")
    assert len(rows) == 1
    assert rows[0]["pipeline"] == "pipe-a"


def test_empty_history_prints_message(capsys):
    with _patch([]):
        code = run_alert_log_cmd(_args())
    assert code == 0
    assert "No history" in capsys.readouterr().out


def test_no_alerts_prints_message(capsys):
    with _patch([_entry("pipe-a", [])]):
        code = run_alert_log_cmd(_args())
    assert code == 0
    assert "No alerts" in capsys.readouterr().out


def test_text_output_shows_alerts(capsys):
    alerts = [{"level": "CRITICAL", "message": "boom"}]
    with _patch([_entry("pipe-a", alerts)]):
        code = run_alert_log_cmd(_args())
    assert code == 0
    out = capsys.readouterr().out
    assert "pipe-a" in out
    assert "CRITICAL" in out
    assert "boom" in out


def test_json_output(capsys):
    alerts = [{"level": "WARNING", "message": "slow"}]
    with _patch([_entry("pipe-a", alerts, "2024-06-01T12:00:00")]):
        code = run_alert_log_cmd(_args(json=True))
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert data[0]["pipeline"] == "pipe-a"
    assert data[0]["level"] == "WARNING"
