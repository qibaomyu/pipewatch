"""Pipeline runner: ties together monitoring and alert dispatching."""

from dataclasses import dataclass
from typing import Optional

from pipewatch.alerts import AlertDispatcher, AlertLevel, build_alert
from pipewatch.config import AppConfig
from pipewatch.monitor import PipelineMonitor, PipelineStatus


@dataclass
class RunResult:
    pipeline_name: str
    status: PipelineStatus
    alerts_dispatched: int


class PipelineRunner:
    """Evaluates all configured pipelines and dispatches alerts."""

    def __init__(self, config: AppConfig, dispatcher: Optional[AlertDispatcher] = None):
        self.config = config
        self.dispatcher = dispatcher or AlertDispatcher(
            channels=getattr(config, "alert_channels", ["log"])
        )
        self.monitor = PipelineMonitor(config)

    def run(self, metrics: dict[str, dict]) -> list[RunResult]:
        """
        Evaluate pipelines against provided metrics.

        metrics: {pipeline_name: {"error_rate": float, "latency_p99": float}}
        """
        results = []
        for pipeline_cfg in self.config.pipelines:
            name = pipeline_cfg.name
            pipeline_metrics = metrics.get(name, {})
            status = self.monitor.evaluate(name, pipeline_metrics)
            alerts_sent = self._dispatch_alerts(name, pipeline_cfg, pipeline_metrics, status)
            results.append(RunResult(pipeline_name=name, status=status, alerts_dispatched=alerts_sent))
        return results

    def _dispatch_alerts(self, name, cfg, metrics, status) -> int:
        if status == PipelineStatus.HEALTHY:
            return 0

        count = 0
        error_rate = metrics.get("error_rate")
        latency = metrics.get("latency_p99")

        if error_rate is not None and cfg.error_rate_threshold is not None:
            if error_rate > cfg.error_rate_threshold:
                level = AlertLevel.CRITICAL if error_rate > cfg.error_rate_threshold * 2 else AlertLevel.WARNING
                self.dispatcher.dispatch(build_alert(name, "error_rate", error_rate, cfg.error_rate_threshold, level))
                count += 1

        if latency is not None and cfg.latency_p99_threshold is not None:
            if latency > cfg.latency_p99_threshold:
                level = AlertLevel.CRITICAL if latency > cfg.latency_p99_threshold * 2 else AlertLevel.WARNING
                self.dispatcher.dispatch(build_alert(name, "latency_p99", latency, cfg.latency_p99_threshold, level))
                count += 1

        return count
