"""Tests for the baseline command."""
import json
import types
import pytest

from pipewatch.commands.baseline_cmd import (
    set_baseline,
    get_baseline,
    run_baseline_cmd,
    DEFAULT_BASELINE_FILE,
)


@pytest.fixture()
def baseline_file(tmp_path):
    return str(tmp_path / "baselines.json")


def _args(baseline_file, action, pipeline=None, error_rate=0.0, latency=0.0):
    return types.SimpleNamespace(
        baseline_action=action,
        pipeline=pipeline,
        error_rate=error_rate,
        latency=latency,
        baseline_file=baseline_file,
    )


def test_set_baseline_creates_entry(baseline_file):
    entry = set_baseline("etl", 0.05, 1.2, baseline_file)
    assert entry["error_rate"] == 0.05
    assert entry["latency"] == 1.2
    assert "recorded_at" in entry


def test_set_baseline_persists(baseline_file):
    set_baseline("etl", 0.01, 0.5, baseline_file)
    with open(baseline_file) as f:
        data = json.load(f)
    assert "etl" in data


def test_get_baseline_returns_none_for_unknown(baseline_file):
    assert get_baseline("missing", baseline_file) is None


def test_get_baseline_returns_entry(baseline_file):
    set_baseline("etl", 0.02, 0.8, baseline_file)
    entry = get_baseline("etl", baseline_file)
    assert entry is not None
    assert entry["error_rate"] == 0.02


def test_run_set_prints_confirmation(baseline_file, capsys):
    args = _args(baseline_file, "set", pipeline="etl", error_rate=0.03, latency=1.5)
    rc = run_baseline_cmd(args)
    out = capsys.readouterr().out
    assert rc == 0
    assert "etl" in out
    assert "Baseline saved" in out


def test_run_show_empty(baseline_file, capsys):
    args = _args(baseline_file, "show")
    rc = run_baseline_cmd(args)
    out = capsys.readouterr().out
    assert rc == 0
    assert "No baselines" in out


def test_run_show_lists_pipelines(baseline_file, capsys):
    set_baseline("etl", 0.01, 0.4, baseline_file)
    set_baseline("ingest", 0.02, 0.9, baseline_file)
    args = _args(baseline_file, "show")
    rc = run_baseline_cmd(args)
    out = capsys.readouterr().out
    assert rc == 0
    assert "etl" in out
    assert "ingest" in out


def test_run_show_filtered(baseline_file, capsys):
    set_baseline("etl", 0.01, 0.4, baseline_file)
    set_baseline("ingest", 0.02, 0.9, baseline_file)
    args = _args(baseline_file, "show", pipeline="etl")
    rc = run_baseline_cmd(args)
    out = capsys.readouterr().out
    assert "etl" in out
    assert "ingest" not in out
