"""Tests for alert dispatching."""

import logging
import pytest

from pipewatch.alerts import Alert, AlertDispatcher, AlertLevel, build_alert


@pytest.fixture
def dispatcher():
    return AlertDispatcher(channels=["log"])


def test_alert_str_representation():
    alert = Alert(
        pipeline_name="etl_daily",
        level=AlertLevel.CRITICAL,
        message="error_rate exceeded threshold",
        metric="error_rate",
        value=0.15,
        threshold=0.05,
    )
    assert "etl_daily" in str(alert)
    assert "CRITICAL" in str(alert)
    assert "0.15" in str(alert)


def test_dispatch_adds_to_history(dispatcher):
    alert = build_alert("pipe1", "error_rate", 0.2, 0.1, AlertLevel.WARNING)
    dispatcher.dispatch(alert)
    assert len(dispatcher.alert_history) == 1
    assert dispatcher.alert_history[0].pipeline_name == "pipe1"


def test_dispatch_multiple_alerts(dispatcher):
    for i in range(3):
        alert = build_alert(f"pipe{i}", "latency_p99", 500.0, 200.0, AlertLevel.WARNING)
        dispatcher.dispatch(alert)
    assert len(dispatcher.alert_history) == 3


def test_clear_history(dispatcher):
    dispatcher.dispatch(build_alert("pipe1", "error_rate", 0.3, 0.1, AlertLevel.CRITICAL))
    dispatcher.clear_history()
    assert dispatcher.alert_history == []


def test_critical_alert_logs_critical(dispatcher, caplog):
    alert = build_alert("pipe1", "error_rate", 0.5, 0.1, AlertLevel.CRITICAL)
    with caplog.at_level(logging.CRITICAL, logger="pipewatch.alerts"):
        dispatcher.dispatch(alert)
    assert any(r.levelname == "CRITICAL" for r in caplog.records)


def test_warning_alert_logs_warning(dispatcher, caplog):
    alert = build_alert("pipe1", "latency_p99", 300.0, 200.0, AlertLevel.WARNING)
    with caplog.at_level(logging.WARNING, logger="pipewatch.alerts"):
        dispatcher.dispatch(alert)
    assert any(r.levelname == "WARNING" for r in caplog.records)


def test_build_alert_fields():
    alert = build_alert("my_pipe", "error_rate", 0.08, 0.05, AlertLevel.WARNING)
    assert alert.pipeline_name == "my_pipe"
    assert alert.metric == "error_rate"
    assert alert.value == pytest.approx(0.08)
    assert alert.threshold == pytestn    assert alert.level == AlertLevel.WARNING


def test_unknown_channel_logs_warning(caplog):
    dispatcher = AlertDispatcher(channels=["slack"])
    alert = build_alert("pipe1", "error_rate", 0.2, 0.1, AlertLevel.WARNING)
    with caplog.at_level(logging.WARNING, logger="pipewatch.alerts"):
        dispatcher.dispatch(alert)
    assert any("Unknown alert channel" in r.message for r in caplog.records)
