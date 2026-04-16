"""Alert dispatching for pipeline threshold violations."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class AlertLevel(str, Enum):
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Alert:
    pipeline_name: str
    level: AlertLevel
    message: str
    metric: str
    value: float
    threshold: float

    def __str__(self) -> str:
        return (
            f"[{self.level.upper()}] Pipeline '{self.pipeline_name}': "
            f"{self.message} ({self.metric}={self.value:.2f}, threshold={self.threshold:.2f})"
        )


class AlertDispatcher:
    """Dispatches alerts to configured channels."""

    def __init__(self, channels: Optional[list] = None):
        self.channels = channels or ["log"]
        self._alert_history: list[Alert] = []

    def dispatch(self, alert: Alert) -> None:
        self._alert_history.append(alert)
        for channel in self.channels:
            if channel == "log":
                self._dispatch_log(alert)
            else:
                logger.warning("Unknown alert channel: %s", channel)

    def _dispatch_log(self, alert: Alert) -> None:
        if alert.level == AlertLevel.CRITICAL:
            logger.critical(str(alert))
        else:
            logger.warning(str(alert))

    @property
    def alert_history(self) -> list[Alert]:
        return list(self._alert_history)

    def clear_history(self) -> None:
        self._alert_history.clear()


def build_alert(pipeline_name: str, metric: str, value: float, threshold: float, level: AlertLevel) -> Alert:
    message = f"{metric} exceeded threshold"
    return Alert(
        pipeline_name=pipeline_name,
        level=level,
        message=message,
        metric=metric,
        value=value,
        threshold=threshold,
    )
