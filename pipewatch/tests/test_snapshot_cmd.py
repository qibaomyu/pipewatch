"""Tests for the snapshot command."""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.commands.snapshot_cmd import run_snapshot_cmd
from pipewatch.monitor import PipelineStatus
from pipewatch.runner import RunResult


def _make_result(name: str, healthy: bool, alerts=None) -> RunResult:
    status = PipelineStatus(healthy=healthy, error_rate=0.0, latency_p99=0.0)
    return RunResult(pipeline_name=name, status=status, alerts=alerts or [])


@pytest.fixture()
def _patch_collect():
    """Patch _collect_results to return controlled data."""
    results = [
        _make_result("ingest", healthy=True),
        _make_result("transform", healthy=False),
    ]
    with patch(
        "pipewatch.commands.snapshot_cmd._collect_results", return_value=results
    ) as mock:
        yield mock, results


def test_snapshot_text_output(_patch_collect):
    out = io.StringIO()
    code = run_snapshot_cmd("pipewatch.yaml", output_format="text", out=out)
    assert code == 0
    text = out.getvalue()
    assert "Snapshot @" in text
    assert "[OK] ingest" in text
    assert "[FAIL] transform" in text


def test_snapshot_json_output(_patch_collect):
    out = io.StringIO()
    code = run_snapshot_cmd("pipewatch.yaml", output_format="json", out=out)
    assert code == 0
    data = json.loads(out.getvalue())
    assert "snapshot_at" in data
    names = [p["pipeline"] for p in data["pipelines"]]
    assert "ingest" in names
    assert "transform" in names


def test_snapshot_healthy_flag_in_json(_patch_collect):
    out = io.StringIO()
    run_snapshot_cmd("pipewatch.yaml", output_format="json", out=out)
    data = json.loads(out.getvalue())
    by_name = {p["pipeline"]: p for p in data["pipelines"]}
    assert by_name["ingest"]["healthy"] is True
    assert by_name["transform"]["healthy"] is False


def test_snapshot_missing_config_returns_2():
    with patch(
        "pipewatch.commands.snapshot_cmd._collect_results",
        side_effect=FileNotFoundError("no file"),
    ):
        out = io.StringIO()
        code = run_snapshot_cmd("missing.yaml", out=out)
    assert code == 2
    assert "error:" in out.getvalue()


def test_snapshot_filters_pipelines(_patch_collect):
    mock_collect, _ = _patch_collect
    out = io.StringIO()
    run_snapshot_cmd("pipewatch.yaml", pipelines=["ingest"], out=out)
    mock_collect.assert_called_once_with("pipewatch.yaml", ["ingest"])
