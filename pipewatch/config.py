"""Configuration loader for pipewatch."""

import os
import yaml
from dataclasses import dataclass, field
from typing import Dict, Any, Optional


DEFAULT_CONFIG_PATH = "pipewatch.yaml"


@dataclass
class PipelineConfig:
    name: str
    threshold_failures: int = 3
    window_seconds: int = 300
    alert_channels: list = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AppConfig:
    pipelines: Dict[str, PipelineConfig] = field(default_factory=dict)
    log_level: str = "INFO"
    state_file: str = ".pipewatch_state.json"


def load_config(path: Optional[str] = None) -> AppConfig:
    """Load configuration from a YAML file."""
    config_path = path or os.environ.get("PIPEWATCH_CONFIG", DEFAULT_CONFIG_PATH)

    if not os.path.exists(config_path):
        return AppConfig()

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f) or {}

    pipelines = {}
    for name, opts in raw.get("pipelines", {}).items():
        pipelines[name] = PipelineConfig(
            name=name,
            threshold_failures=opts.get("threshold_failures", 3),
            window_seconds=opts.get("window_seconds", 300),
            alert_channels=opts.get("alert_channels", []),
            tags=opts.get("tags", {}),
        )

    return AppConfig(
        pipelines=pipelines,
        log_level=raw.get("log_level", "INFO"),
        state_file=raw.get("state_file", ".pipewatch_state.json"),
    )
