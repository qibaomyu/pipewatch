"""Tests for pipewatch configuration loader."""

import os
import textwrap
import pytest
from unittest.mock import patch

from pipewatch.config import load_config, AppConfig, PipelineConfig


YAML_CONTENT = textwrap.dedent("""\
    log_level: DEBUG
    state_file: /tmp/state.json
    pipelines:
      etl_daily:
        threshold_failures: 5
        window_seconds: 600
        alert_channels:
          - slack
          - email
        tags:
          team: data
      ml_training:
        threshold_failures: 2
        window_seconds: 120
""")


@pytest.fixture
def config_file(tmp_path):
    cfg = tmp_path / "pipewatch.yaml"
    cfg.write_text(YAML_CONTENT)
    return str(cfg)


def test_load_config_parses_pipelines(config_file):
    config = load_config(config_file)
    assert isinstance(config, AppConfig)
    assert "etl_daily" in config.pipelines
    assert "ml_training" in config.pipelines


def test_pipeline_thresholds(config_file):
    config = load_config(config_file)
    etl = config.pipelines["etl_daily"]
    assert etl.threshold_failures == 5
    assert etl.window_seconds == 600
    assert etl.alert_channels == ["slack", "email"]
    assert etl.tags == {"team": "data"}


def test_pipeline_defaults(config_file):
    config = load_config(config_file)
    ml = config.pipelines["ml_training"]
    assert ml.alert_channels == []
    assert ml.tags == {}


def test_app_level_settings(config_file):
    config = load_config(config_file)
    assert config.log_level == "DEBUG"
    assert config.state_file == "/tmp/state.json"


def test_missing_config_returns_defaults():
    config = load_config("/nonexistent/path.yaml")
    assert isinstance(config, AppConfig)
    assert config.pipelines == {}
    assert config.log_level == "INFO"


def test_env_var_config_path(config_file):
    with patch.dict(os.environ, {"PIPEWATCH_CONFIG": config_file}):
        config = load_config()
    assert "etl_daily" in config.pipelines


def test_malformed_yaml_returns_defaults(tmp_path):
    """A config file with invalid YAML should fall back to default AppConfig."""
    bad_cfg = tmp_path / "bad.yaml"
    bad_cfg.write_text("pipelines: [this: is: not: valid\n  yaml: -")
    config = load_config(str(bad_cfg))
    assert isinstance(config, AppConfig)
    assert config.pipelines == {}
    assert config.log_level == "INFO"
