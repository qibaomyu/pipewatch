"""Pipeline monitoring and threshold evaluation."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pipewatch.config import PipelineConfig


@dataclass
class PipelineStatus:
    name: str
    error_rate: float
    latency_seconds: float
    last_checked: datetime = field(default_factory=datetime.utcnow)
    alerts: list[str] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return len(self.alerts) == 0


def evaluate_pipeline(config: PipelineConfig, status: PipelineStatus) -> list[str]:
    """Evaluate a pipeline status against its config thresholds.

    Returns a list of alert messages (empty if healthy).
    """
    alerts = []

    if status.error_rate > config.error_rate_threshold:
        alerts.append(
            f"[{config.name}] Error rate {status.error_rate:.2%} exceeds "
            f"threshold {config.error_rate_threshold:.2%}"
        )

    if config.latency_threshold_seconds is not None:
        if status.latency_seconds > config.latency_threshold_seconds:
            alerts.append(
                f"[{config.name}] Latency {status.latency_seconds:.1f}s exceeds "
                f"threshold {config.latency_threshold_seconds:.1f}s"
            )

    return alerts


class PipelineMonitor:
    """Monitors multiple pipelines and aggregates alerts."""

    def __init__(self, pipelines: list[PipelineConfig]):
        self.pipelines = {p.name: p for p in pipelines}

    def check(self, status: PipelineStatus) -> PipelineStatus:
        """Check a single pipeline status and populate its alerts."""
        config = self.pipelines.get(status.name)
        if config is None:
            raise ValueError(f"Unknown pipeline: {status.name}")
        status.alerts = evaluate_pipeline(config, status)
        return status

    def check_all(self, statuses: list[PipelineStatus]) -> list[PipelineStatus]:
        """Check all provided statuses and return results."""
        return [self.check(s) for s in statuses]

    def unhealthy(self, statuses: list[PipelineStatus]) -> list[PipelineStatus]:
        """Return only statuses that have triggered alerts."""
        return [s for s in statuses if not s.healthy]
