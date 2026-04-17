"""Tests for pipewatch.history module."""

import json
import os
import pytest

from pipewatch.history import HistoryEntry, RunHistory


@pytest.fixture
def history_file(tmp_path):
    return str(tmp_path / "test_history.json")


@pytest.fixture
def history(history_file):
    return RunHistory(path=history_file)


def _entry(pipeline="pipe1", healthy=True, error_rate=0.0, latency=1.0, alert_count=0):
    return HistoryEntry(
        pipeline=pipeline,
        timestamp="2024-01-01T00:00:00",
        healthy=healthy,
        error_rate=error_rate,
        latency=latency,
        alert_count=alert_count,
    )


def test_record_and_retrieve(history):
    e = _entry()
    history.record(e)
    entries = history.get("pipe1")
    assert len(entries) == 1
    assert entries[0].pipeline == "pipe1"


def test_persists_to_disk(history_file):
    h = RunHistory(path=history_file)
    h.record(_entry(pipeline="pipe1"))
    h2 = RunHistory(path=history_file)
    assert len(h2.get("pipe1")) == 1


def test_get_all_pipelines(history):
    history.record(_entry(pipeline="a"))
    history.record(_entry(pipeline="b"))
    assert len(history.get()) == 2


def test_get_filters_by_pipeline(history):
    history.record(_entry(pipeline="a"))
    history.record(_entry(pipeline="b"))
    assert len(history.get("a")) == 1


def test_last_returns_most_recent(history):
    history.record(_entry(pipeline="p", error_rate=0.1))
    history.record(_entry(pipeline="p", error_rate=0.5))
    last = history.last("p")
    assert last is not None
    assert last.error_rate == 0.5


def test_last_returns_none_for_unknown(history):
    assert history.last("nonexistent") is None


def test_clear_specific_pipeline(history):
    history.record(_entry(pipeline="a"))
    history.record(_entry(pipeline="b"))
    history.clear("a")
    assert history.get("a") == []
    assert len(history.get("b")) == 1


def test_clear_all(history):
    history.record(_entry(pipeline="a"))
    history.record(_entry(pipeline="b"))
    history.clear()
    assert history.get() == []


def test_invalid_history_file_starts_empty(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("not valid json")
    h = RunHistory(path=str(p))
    assert h.get() == []
