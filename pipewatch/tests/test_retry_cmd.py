"""Tests for pipewatch/commands/retry_cmd.py."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.retry_cmd import _pipeline_retry, run_retry_cmd
from pipewatch.history import HistoryEntry


NOW = datetime.now(tz=timezone.utc).timestamp()


def _entry(pipeline: str, healthy: bool, offset_s: float = 0) -> HistoryEntry:
    e = MagicMock(spec=HistoryEntry)
    e.pipeline = pipeline
    e.healthy = healthy
    e.timestamp = NOW - offset_s
    return e


def _args(**kwargs) -> argparse.Namespace:
    defaults = dict(
        hours=24.0,
        max_retries=3,
        pipeline=None,
        json=False,
        exit_code=False,
        history_file=".pipewatch_history.json",
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _patch(entries):
    return patch(
        "pipewatch.commands.retry_cmd.RunHistory",
        return_value=MagicMock(entries=entries, load=MagicMock()),
    )


# ---------------------------------------------------------------------------
# unit tests for _pipeline_retry
# ---------------------------------------------------------------------------

class TestPipelineRetry:
    def test_no_consecutive_failures(self):
        entries = [
            _entry("pipe_a", healthy=True, offset_s=100),
            _entry("pipe_a", healthy=True, offset_s=50),
        ]
        history = MagicMock(entries=entries)
        rows = _pipeline_retry(history, pipeline=None, hours=24, max_retries=3)
        assert rows[0]["consecutive_failures"] == 0
        assert rows[0]["exceeds_threshold"] is False

    def test_exceeds_threshold(self):
        entries = [
            _entry("pipe_b", healthy=False, offset_s=300),
            _entry("pipe_b", healthy=False, offset_s=200),
            _entry("pipe_b", healthy=False, offset_s=100),
            _entry("pipe_b", healthy=False, offset_s=10),
        ]
        history = MagicMock(entries=entries)
        rows = _pipeline_retry(history, pipeline=None, hours=24, max_retries=3)
        assert rows[0]["consecutive_failures"] == 4
        assert rows[0]["exceeds_threshold"] is True

    def test_recovery_resets_count(self):
        entries = [
            _entry("pipe_c", healthy=False, offset_s=300),
            _entry("pipe_c", healthy=True, offset_s=200),  # recovery
            _entry("pipe_c", healthy=False, offset_s=100),
            _entry("pipe_c", healthy=False, offset_s=10),
        ]
        history = MagicMock(entries=entries)
        rows = _pipeline_retry(history, pipeline=None, hours=24, max_retries=3)
        assert rows[0]["consecutive_failures"] == 2
        assert rows[0]["exceeds_threshold"] is False

    def test_pipeline_filter(self):
        entries = [
            _entry("pipe_a", healthy=False, offset_s=10),
            _entry("pipe_b", healthy=False, offset_s=10),
        ]
        history = MagicMock(entries=entries)
        rows = _pipeline_retry(history, pipeline="pipe_a", hours=24, max_retries=3)
        assert len(rows) == 1
        assert rows[0]["pipeline"] == "pipe_a"


# ---------------------------------------------------------------------------
# integration-style tests for run_retry_cmd
# ---------------------------------------------------------------------------

class TestRunRetryCmd:
    def test_empty_history_prints_message(self, capsys):
        with _patch([]):
            rc = run_retry_cmd(_args())
        assert rc == 0
        assert "No pipeline history" in capsys.readouterr().out

    def test_text_output(self, capsys):
        entries = [_entry("pipe_x", healthy=False, offset_s=10)]
        with _patch(entries):
            rc = run_retry_cmd(_args())
        assert rc == 0
        out = capsys.readouterr().out
        assert "pipe_x" in out

    def test_json_output(self, capsys):
        entries = [_entry("pipe_y", healthy=False, offset_s=10)]
        with _patch(entries):
            rc = run_retry_cmd(_args(json=True))
        data = json.loads(capsys.readouterr().out)
        assert isinstance(data, list)
        assert data[0]["pipeline"] == "pipe_y"

    def test_exit_code_when_exceeds(self):
        entries = [
            _entry("pipe_z", healthy=False, offset_s=400),
            _entry("pipe_z", healthy=False, offset_s=300),
            _entry("pipe_z", healthy=False, offset_s=200),
            _entry("pipe_z", healthy=False, offset_s=10),
        ]
        with _patch(entries):
            rc = run_retry_cmd(_args(exit_code=True, max_retries=3))
        assert rc == 1

    def test_exit_code_zero_when_healthy(self):
        entries = [_entry("pipe_ok", healthy=True, offset_s=10)]
        with _patch(entries):
            rc = run_retry_cmd(_args(exit_code=True, max_retries=3))
        assert rc == 0
