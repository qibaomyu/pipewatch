import json
import pytest
from argparse import Namespace
from unittest.mock import patch
from datetime import datetime, timezone

from pipewatch.commands.flap_cmd import _flap_score, _pipeline_flap, run_flap_cmd
from pipewatch.history import HistoryEntry


def _entry(pipeline: str, healthy: bool, ts: float) -> HistoryEntry:
    e = HistoryEntry(pipeline=pipeline, timestamp=ts, healthy=healthy, error_rate=0.0, latency_p99=0.0, alerts=[])
    return e


def _args(**kwargs):
    defaults = dict(pipeline=None, threshold=3, history_file="x.json", json=False, exit_code=False)
    defaults.update(kwargs)
    return Namespace(**defaults)


def _patch(entries):
    return patch("pipewatch.commands.flap_cmd.RunHistory.all", return_value=entries)


# --- unit tests for helpers ---

def test_flap_score_no_transitions():
    entries = [_entry("p", True, i) for i in range(5)]
    assert _flap_score(entries) == 0


def test_flap_score_counts_transitions():
    states = [True, False, True, False, True]
    entries = [_entry("p", s, i) for i, s in enumerate(states)]
    assert _flap_score(entries) == 4


def test_flap_score_single_entry():
    assert _flap_score([_entry("p", True, 0)]) == 0


def test_pipeline_flap_flagged():
    states = [True, False, True, False]
    entries = [_entry("p", s, i) for i, s in enumerate(states)]
    result = _pipeline_flap("p", entries, threshold=3)
    assert result["flapping"] is True
    assert result["transitions"] == 3


def test_pipeline_flap_not_flagged():
    entries = [_entry("p", True, i) for i in range(4)]
    result = _pipeline_flap("p", entries, threshold=3)
    assert result["flapping"] is False


# --- integration via run_flap_cmd ---

def test_run_flap_cmd_text_output(capsys):
    entries = [_entry("pipe_a", s, i) for i, s in enumerate([True, False, True, False])]
    with _patch(entries):
        rc = run_flap_cmd(_args())
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert rc == 0


def test_run_flap_cmd_json_output(capsys):
    entries = [_entry("pipe_a", s, i) for i, s in enumerate([True, False, True, False])]
    with _patch(entries):
        rc = run_flap_cmd(_args(json=True))
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"


def test_run_flap_cmd_exit_code_when_flapping():
    entries = [_entry("pipe_a", s, i) for i, s in enumerate([True, False, True, False])]
    with _patch(entries):
        rc = run_flap_cmd(_args(exit_code=True))
    assert rc == 1


def test_run_flap_cmd_no_history(capsys):
    with _patch([]):
        rc = run_flap_cmd(_args())
    assert rc == 0
    assert "No history" in capsys.readouterr().out


def test_run_flap_cmd_pipeline_filter(capsys):
    entries = [
        *[_entry("a", s, i) for i, s in enumerate([True, False, True, False])],
        *[_entry("b", True, i) for i in range(4)],
    ]
    with _patch(entries):
        rc = run_flap_cmd(_args(pipeline="b", json=True))
    data = json.loads(capsys.readouterr().out)
    assert all(r["pipeline"] == "b" for r in data)
