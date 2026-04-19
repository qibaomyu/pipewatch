"""Tests for noise_cmd."""
from __future__ import annotations

import json
import time
from argparse import Namespace
from unittest.mock import patch

import pytest

from pipewatch.commands.noise_cmd import _collect_noise, run_noise_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, alerts: list, age_seconds: int = 0) -> HistoryEntry:
    return HistoryEntry(
        pipeline=pipeline,
        timestamp=time.time() - age_seconds,
        healthy=len(alerts) == 0,
        alerts=alerts,
        error_rate=0.0,
        latency=0.0,
    )


def _args(**kwargs):
    defaults = dict(hours=24, pipeline=None, json=False, history_file=".pipewatch_history.json")
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.noise_cmd.RunHistory.all", return_value=entries)


def test_collect_noise_counts_alerts():
    entries = [
        _entry("pipe_a", ["err1", "err2"]),
        _entry("pipe_b", ["err1"]),
        _entry("pipe_a", ["err3"]),
    ]
    results = _collect_noise(entries)
    assert results[0] == ("pipe_a", 3)
    assert results[1] == ("pipe_b", 1)


def test_collect_noise_filters_by_pipeline():
    entries = [
        _entry("pipe_a", ["err1"]),
        _entry("pipe_b", ["err1", "err2"]),
    ]
    results = _collect_noise(entries, pipeline="pipe_a")
    assert len(results) == 1
    assert results[0][0] == "pipe_a"


def test_collect_noise_respects_hours():
    entries = [
        _entry("pipe_a", ["err1"], age_seconds=100),
        _entry("pipe_a", ["err2"], age_seconds=999999),
    ]
    results = _collect_noise(entries, hours=1)
    assert results[0][1] == 1


def test_run_noise_cmd_no_data(capsys):
    with _patch([]):
        code = run_noise_cmd(_args())
    assert code == 0
    assert "No alert data" in capsys.readouterr().out


def test_run_noise_cmd_text_output(capsys):
    entries = [_entry("pipe_a", ["e1", "e2"]), _entry("pipe_b", ["e1"])]
    with _patch(entries):
        code = run_noise_cmd(_args())
    assert code == 0
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "pipe_b" in out


def test_run_noise_cmd_json_output(capsys):
    entries = [_entry("pipe_a", ["e1"]), _entry("pipe_b", ["e1", "e2"])]
    with _patch(entries):
        code = run_noise_cmd(_args(json=True))
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    names = [d["pipeline"] for d in data]
    assert "pipe_b" in names
    assert "pipe_a" in names
    top = data[0]
    assert top["alert_count"] == 2
