"""Tests for export_cmd."""

import json
import csv
import io
import pytest
from argparse import Namespace
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from pipewatch.commands.export_cmd import run_export_cmd
from pipewatch.history import HistoryEntry


def _make_entry(pipeline="pipe1", healthy=True, error_rate=0.01, latency=1.2, alert_count=0):
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        healthy=healthy,
        error_rate=error_rate,
        latency=latency,
        alert_count=alert_count,
    )


def _args(fmt="csv", pipeline=None, output=None, history_file="history.json"):
    return Namespace(format=fmt, pipeline=pipeline, output=output, history_file=history_file)


def _patch(entries):
    return patch("pipewatch.commands.export_cmd.RunHistory", return_value=MagicMock(get=MagicMock(return_value=entries)))


def test_export_json_stdout(capsys):
    entries = [_make_entry()]
    with _patch(entries):
        code = run_export_cmd(_args(fmt="json"))
    assert code == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data[0]["pipeline"] == "pipe1"
    assert data[0]["healthy"] is True


def test_export_csv_stdout(capsys):
    entries = [_make_entry(healthy=False, alert_count=2)]
    with _patch(entries):
        code = run_export_cmd(_args(fmt="csv"))
    assert code == 0
    out = capsys.readouterr().out
    reader = csv.DictReader(io.StringIO(out))
    rows = list(reader)
    assert rows[0]["pipeline"] == "pipe1"
    assert rows[0]["alert_count"] == "2"


def test_empty_history_returns_zero(capsys):
    with _patch([]):
        code = run_export_cmd(_args())
    assert code == 0
    err = capsys.readouterr().err
    assert "No history" in err


def test_export_json_to_file(tmp_path):
    out_file = tmp_path / "out.json"
    entries = [_make_entry(), _make_entry(pipeline="pipe2")]
    with _patch(entries):
        code = run_export_cmd(_args(fmt="json", output=str(out_file)))
    assert code == 0
    data = json.loads(out_file.read_text())
    assert len(data) == 2


def test_export_csv_to_file(tmp_path):
    out_file = tmp_path / "out.csv"
    entries = [_make_entry()]
    with _patch(entries):
        code = run_export_cmd(_args(fmt="csv", output=str(out_file)))
    assert code == 0
    content = out_file.read_text()
    assert "pipeline" in content
    assert "pipe1" in content
