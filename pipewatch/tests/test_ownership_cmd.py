"""Tests for pipewatch.commands.ownership_cmd."""
from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest

from pipewatch.commands.ownership_cmd import (
    get_owner,
    list_owners,
    run_ownership_cmd,
    set_owner,
)


@pytest.fixture()
def ownership_file(tmp_path: Path) -> str:
    return str(tmp_path / "owners.json")


def _args(action: str, ownership_file: str, **kwargs) -> Namespace:
    return Namespace(ownership_action=action, ownership_file=ownership_file, **kwargs)


# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------

def test_set_owner_creates_entry(ownership_file):
    set_owner("etl", "alice", "data-eng", ownership_file)
    data = json.loads(Path(ownership_file).read_text())
    assert data["etl"]["owner"] == "alice"
    assert data["etl"]["team"] == "data-eng"


def test_set_owner_overwrites(ownership_file):
    set_owner("etl", "alice", "", ownership_file)
    set_owner("etl", "bob", "ops", ownership_file)
    assert get_owner("etl", ownership_file)["owner"] == "bob"


def test_get_owner_returns_none_for_unknown(ownership_file):
    assert get_owner("missing", ownership_file) is None


def test_list_owners_empty(ownership_file):
    assert list_owners(ownership_file) == []


def test_list_owners_sorted(ownership_file):
    set_owner("zzz", "carol", "", ownership_file)
    set_owner("aaa", "dave", "team-a", ownership_file)
    rows = list_owners(ownership_file)
    assert rows[0]["pipeline"] == "aaa"
    assert rows[1]["pipeline"] == "zzz"


# ---------------------------------------------------------------------------
# Integration tests via run_ownership_cmd
# ---------------------------------------------------------------------------

def test_run_set_prints_confirmation(ownership_file, capsys):
    args = _args("set", ownership_file, pipeline="etl", owner="alice", team="")
    rc = run_ownership_cmd(args)
    assert rc == 0
    assert "alice" in capsys.readouterr().out


def test_run_get_prints_owner(ownership_file, capsys):
    set_owner("etl", "alice", "data-eng", ownership_file)
    args = _args("get", ownership_file, pipeline="etl")
    rc = run_ownership_cmd(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "alice" in out
    assert "data-eng" in out


def test_run_get_missing_returns_1(ownership_file, capsys):
    args = _args("get", ownership_file, pipeline="ghost")
    rc = run_ownership_cmd(args)
    assert rc == 1


def test_run_list_prints_all(ownership_file, capsys):
    set_owner("etl", "alice", "", ownership_file)
    set_owner("ml", "bob", "ml-team", ownership_file)
    args = _args("list", ownership_file)
    rc = run_ownership_cmd(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "etl" in out
    assert "ml" in out


def test_run_list_empty_prints_message(ownership_file, capsys):
    args = _args("list", ownership_file)
    run_ownership_cmd(args)
    assert "No ownership" in capsys.readouterr().out
