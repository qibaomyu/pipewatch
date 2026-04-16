"""Tests for pipeline monitoring and threshold evaluation."""

import pytest

from pipewatch.config import PipelineConfig
from pipewatch.monitor import PipelineMonitor, PipelineStatus, evaluate_pipeline


@pytest.fixture
def pipeline_config():
    return PipelineConfig(
        name="orders",
        error_rate_threshold=0.05,
        latency_threshold_seconds=30.0,
    )


@pytest.fixture
def monitor(pipeline_config):
    return PipelineMonitor(pipelines=[pipeline_config])


def test_evaluate_healthy_pipeline(pipeline_config):
    status = PipelineStatus(name="orders", error_rate=0.01, latency_seconds=10.0)
    alerts = evaluate_pipeline(pipeline_config, status)
    assert alerts == []


def test_evaluate_high_error_rate(pipeline_config):
    status = PipelineStatus(name="orders", error_rate=0.10, latency_seconds=10.0)
    alerts = evaluate_pipeline(pipeline_config, status)
    assert len(alerts) == 1
    assert "Error rate" in alerts[0]
    assert "orders" in alerts[0]


def test_evaluate_high_latency(pipeline_config):
    status = PipelineStatus(name="orders", error_rate=0.01, latency_seconds=60.0)
    alerts = evaluate_pipeline(pipeline_config, status)
    assert len(alerts) == 1
    assert "Latency" in alerts[0]


def test_evaluate_multiple_violations(pipeline_config):
    status = PipelineStatus(name="orders", error_rate=0.20, latency_seconds=90.0)
    alerts = evaluate_pipeline(pipeline_config, status)
    assert len(alerts) == 2


def test_no_latency_threshold():
    config = PipelineConfig(name="events", error_rate_threshold=0.05)
    status = PipelineStatus(name="events", error_rate=0.01, latency_seconds=999.0)
    alerts = evaluate_pipeline(config, status)
    assert alerts == []


def test_monitor_check_populates_alerts(monitor):
    status = PipelineStatus(name="orders", error_rate=0.10, latency_seconds=5.0)
    result = monitor.check(status)
    assert not result.healthy
    assert len(result.alerts) == 1


def test_monitor_unknown_pipeline_raises(monitor):
    status = PipelineStatus(name="unknown", error_rate=0.0, latency_seconds=0.0)
    with pytest.raises(ValueError, match="Unknown pipeline"):
        monitor.check(status)


def test_monitor_unhealthy_filters_correctly(monitor):
    statuses = [
        PipelineStatus(name="orders", error_rate=0.01, latency_seconds=5.0),
        PipelineStatus(name="orders", error_rate=0.50, latency_seconds=5.0),
    ]
    results = monitor.check_all(statuses)
    bad = monitor.unhealthy(results)
    assert len(bad) == 1
    assert bad[0].error_rate == 0.50
