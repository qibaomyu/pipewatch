"""Tests for pipewatch/commands/incident_cmd.py."""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipewatch.commands.incident_cmd import (
    get_open_incidents,
    open_incident,
    resolve_incident,
    run_incident_cmd,
)


@pytest.fixture()
def incident_file(tmp_path: Path) -> str:
    return str(tmp_path / "incidents.json")


def test_open_incident_creates_entry(incident_file: str) -> None:
    entry = open_incident("pipe_a", "Something broke", path=incident_file)
    assert entry["id"] == 1
    assert entry["message"] == "Something broke"
    assert entry["resolved_at"] is None


def test_open_incident_persists(incident_file: str) -> None:
    open_incident("pipe_a", "First", path=incident_file)
    open_incident("pipe_a", "Second", path=incident_file)
    data = json.loads(Path(incident_file).read_text())
    assert len(data["pipe_a"]) == 2
    assert data["pipe_a"][1]["id"] == 2


def test_open_incident_multiple_pipelines(incident_file: str) -> None:
    open_incident("pipe_a", "err", path=incident_file)
    open_incident("pipe_b", "err", path=incident_file)
    data = json.loads(Path(incident_file).read_text())
    assert "pipe_a" in data
    assert "pipe_b" in data


def test_resolve_incident_returns_true(incident_file: str) -> None:
    open_incident("pipe_a", "err", path=incident_file)
    ok = resolve_incident("pipe_a", 1, path=incident_file)
    assert ok is True
    data = json.loads(Path(incident_file).read_text())
    assert data["pipe_a"][0]["resolved_at"] is not None


def test_resolve_incident_unknown_returns_false(incident_file: str) -> None:
    ok = resolve_incident("pipe_a", 99, path=incident_file)
    assert ok is False


def test_resolve_already_resolved_returns_false(incident_file: str) -> None:
    open_incident("pipe_a", "err", path=incident_file)
    resolve_incident("pipe_a", 1, path=incident_file)
    ok = resolve_incident("pipe_a", 1, path=incident_file)
    assert ok is False


def test_get_open_incidents_all(incident_file: str) -> None:
    open_incident("pipe_a", "err1", path=incident_file)
    open_incident("pipe_b", "err2", path=incident_file)
    resolve_incident("pipe_a", 1, path=incident_file)
    results = get_open_incidents(None, path=incident_file)
    assert len(results) == 1
    assert results[0]["pipeline"] == "pipe_b"


def test_get_open_incidents_filtered(incident_file: str) -> None:
    open_incident("pipe_a", "err", path=incident_file)
    open_incident("pipe_b", "err", path=incident_file)
    results = get_open_incidents("pipe_a", path=incident_file)
    assert all(r["pipeline"] == "pipe_a" for r in results)


def _args(**kwargs):
    defaults = {"incident_file": ".pipewatch_incidents.json", "json": False}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_run_incident_open(incident_file: str, capsys) -> None:
    args = _args(incident_action="open", pipeline="pipe_a", message="boom",
                 incident_file=incident_file)
    rc = run_incident_cmd(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "#1" in out


def test_run_incident_resolve(incident_file: str, capsys) -> None:
    open_incident("pipe_a", "err", path=incident_file)
    args = _args(incident_action="resolve", pipeline="pipe_a", incident_id=1,
                 incident_file=incident_file)
    rc = run_incident_cmd(args)
    assert rc == 0


def test_run_incident_resolve_missing(incident_file: str) -> None:
    args = _args(incident_action="resolve", pipeline="pipe_a", incident_id=99,
                 incident_file=incident_file)
    rc = run_incident_cmd(args)
    assert rc == 2


def test_run_incident_list_empty(incident_file: str, capsys) -> None:
    args = _args(incident_action="list", pipeline=None, incident_file=incident_file)
    rc = run_incident_cmd(args)
    assert rc == 0
    assert "No open incidents" in capsys.readouterr().out


def test_run_incident_list_json(incident_file: str, capsys) -> None:
    open_incident("pipe_a", "err", path=incident_file)
    args = _args(incident_action="list", pipeline=None, incident_file=incident_file,
                 json=True)
    rc = run_incident_cmd(args)
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert isinstance(payload, list)
    assert payload[0]["pipeline"] == "pipe_a"
