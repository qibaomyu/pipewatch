"""Tests for PipelineRunner and RunResult."""
import pytest
from unittest.mock import MagicMock, patch

from pipewatch.runner import PipelineRunner, RunResult
from pipewatch.config import PipelineConfig
from pipewatch.alerts import Alert, AlertLevel
from pipewatch.monitor import PipelineStatus


@pytest.fixture
def pipeline_cfg():
    return PipelineConfig(
        name="orders",
        error_rate_threshold=0.05,
        latency_threshold_ms=500,
        min_throughput=10,
    )


@pytest.fixture
def runner(pipeline_cfg):
    return PipelineRunner(pipelines=[pipeline_cfg])


def _status(error_rate=0.01, latency_ms=100, throughput=50) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name="orders",
        error_rate=error_rate,
        latency_ms=latency_ms,
        throughput=throughput,
    )


def test_run_returns_run_result(runner):
    status = _status()
    with patch.object(runner.monitor, "check", return_value=(True, [])):
        result = runner.run("orders", status)
    assert isinstance(result, RunResult)
    assert result.pipeline_name == "orders"


def test_run_healthy_no_alerts(runner):
    status = _status()
    with patch.object(runner.monitor, "check", return_value=(True, [])):
        result = runner.run("orders", status)
    assert result.healthy is True
    assert result.alerts == []


def test_run_unhealthy_dispatches_alerts(runner):
    alert = Alert(pipeline="orders", level=AlertLevel.CRITICAL, message="bad")
    status = _status(error_rate=0.9)
    with patch.object(runner.monitor, "check", return_value=(False, [alert])):
        result = runner.run("orders", status)
    assert result.healthy is False
    assert len(result.alerts) == 1


def test_run_unknown_pipeline_raises(runner):
    with pytest.raises(KeyError):
        runner.run("nonexistent", _status())
