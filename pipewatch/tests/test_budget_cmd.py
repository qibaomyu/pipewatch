"""Tests for budget_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pipewatch.commands.budget_cmd import (
    _format_text,
    _pipeline_budget,
    run_budget_cmd,
)


def _entry(pipeline: str, error_count: int, age_secs: int = 60):
    ts = datetime.now(timezone.utc).timestamp() - age_secs
    return SimpleNamespace(pipeline=pipeline, error_count=error_count, timestamp=ts)


def _args(**kwargs):
    defaults = dict(
        hours=24, limit=100, pipeline=None, json=False,
        exit_code=False, history_file="fake.json",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.budget_cmd.RunHistory",
        return_value=SimpleNamespace(all=lambda: entries),
    )


# ── unit: _pipeline_budget ──────────────────────────────────────────────────

def test_pipeline_budget_no_entries():
    result = _pipeline_budget([], "pipe_a", 24, 100)
    assert result["runs"] == 0
    assert result["errors"] == 0
    assert result["remaining"] == 100
    assert result["breached"] is False


def test_pipeline_budget_under_limit():
    entries = [_entry("pipe_a", 10), _entry("pipe_a", 20)]
    result = _pipeline_budget(entries, "pipe_a", 24, 100)
    assert result["errors"] == 30
    assert result["remaining"] == 70
    assert result["breached"] is False
    assert result["pct_used"] == 30.0


def test_pipeline_budget_breached():
    entries = [_entry("pipe_a", 60), _entry("pipe_a", 50)]
    result = _pipeline_budget(entries, "pipe_a", 24, 100)
    assert result["errors"] == 110
    assert result["remaining"] == 0
    assert result["breached"] is True


def test_pipeline_budget_ignores_old_entries():
    old = _entry("pipe_a", 999, age_secs=90000)  # >24 h old
    recent = _entry("pipe_a", 5)
    result = _pipeline_budget([old, recent], "pipe_a", 24, 100)
    assert result["errors"] == 5


def test_pipeline_budget_filters_by_pipeline():
    entries = [_entry("pipe_a", 10), _entry("pipe_b", 50)]
    result = _pipeline_budget(entries, "pipe_a", 24, 100)
    assert result["errors"] == 10


# ── unit: _format_text ──────────────────────────────────────────────────────

def test_format_text_no_rows():
    assert _format_text([]) == "No data."


def test_format_text_ok():
    row = {"pipeline": "p", "errors": 5, "limit": 100,
           "pct_used": 5.0, "remaining": 95, "breached": False}
    out = _format_text([row])
    assert "p" in out and "ok" in out and "5/100" in out


def test_format_text_breached():
    row = {"pipeline": "p", "errors": 110, "limit": 100,
           "pct_used": 110.0, "remaining": 0, "breached": True}
    out = _format_text([row])
    assert "BREACHED" in out


# ── integration: run_budget_cmd ─────────────────────────────────────────────

def test_run_budget_cmd_text_output(capsys):
    entries = [_entry("pipe_a", 10)]
    with _patch(entries):
        rc = run_budget_cmd(_args())
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert rc == 0


def test_run_budget_cmd_json_output(capsys):
    entries = [_entry("pipe_a", 10)]
    with _patch(entries):
        rc = run_budget_cmd(_args(json=True))
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"
    assert rc == 0


def test_run_budget_cmd_exit_code_on_breach():
    entries = [_entry("pipe_a", 200)]
    with _patch(entries):
        rc = run_budget_cmd(_args(exit_code=True, limit=100))
    assert rc == 1


def test_run_budget_cmd_no_exit_code_when_ok():
    entries = [_entry("pipe_a", 10)]
    with _patch(entries):
        rc = run_budget_cmd(_args(exit_code=True, limit=100))
    assert rc == 0
