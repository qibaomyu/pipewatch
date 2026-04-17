"""Tests for output formatters."""
import json
import pytest

from pipewatch.formatter import format_text, format_json, format_results
from pipewatch.runner import RunResult
from pipewatch.alerts import Alert, AlertLevel


def _make_result(name: str, healthy: bool, alerts=None) -> RunResult:
    return RunResult(pipeline_name=name, healthy=healthy, alerts=alerts or [])


def _alert(level: AlertLevel, msg: str) -> Alert:
    return Alert(pipeline="p", level=level, message=msg)


def test_format_text_healthy():
    result = _make_result("etl", True)
    output = format_text([result])
    assert "[OK] etl" in output


def test_format_text_failing_with_alert():
    alert = _alert(AlertLevel.CRITICAL, "error rate too high")
    result = _make_result("etl", False, [alert])
    output = format_text([result])
    assert "[FAIL] etl" in output
    assert "error rate too high" in output
    assert "✖" in output


def test_format_text_warning_symbol():
    alert = _alert(AlertLevel.WARNING, "latency elevated")
    result = _make_result("etl", False, [alert])
    output = format_text([result])
    assert "⚠" in output


def test_format_text_empty():
    output = format_text([])
    assert "No pipelines" in output


def test_format_json_structure():
    alert = _alert(AlertLevel.WARNING, "slow")
    result = _make_result("pipe1", False, [alert])
    raw = format_json([result])
    data = json.loads(raw)
    assert len(data) == 1
    assert data[0]["pipeline"] == "pipe1"
    assert data[0]["healthy"] is False
    assert data[0]["alerts"][0]["level"] == AlertLevel.WARNING.value


def test_format_results_delegates_json():
    result = _make_result("p", True)
    out = format_results([result], fmt="json")
    assert json.loads(out)[0]["pipeline"] == "p"


def test_format_results_delegates_text():
    result = _make_result("p", True)
    out = format_results([result], fmt="text")
    assert "[OK] p" in out


def test_format_results_default_is_text():
    result = _make_result("p", True)
    assert format_results([result]) == format_text([result])
