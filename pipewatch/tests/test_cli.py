"""Tests for the CLI layer."""
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from pipewatch.cli import build_parser, run_cli
from pipewatch.runner import RunResult


@pytest.fixture
def config_path(tmp_path):
    cfg = tmp_path / "pipewatch.yaml"
    cfg.write_text(
        "app:\n  check_interval: 60\npipelines:\n  - name: orders\n    error_rate_threshold: 0.05\n"
    )
    return cfg


def test_build_parser_defaults():
    parser = build_parser()
    args = parser.parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.pipeline is None
    assert args.dry_run is False
    assert args.verbose is False


def test_missing_config_returns_exit_code_2():
    code = run_cli(["-c", "nonexistent.yaml"])
    assert code == 2


def test_unknown_pipeline_returns_exit_code_2(config_path):
    code = run_cli(["-c", str(config_path), "--pipeline", "ghost"])
    assert code == 2


def _make_result(healthy, summary="ok"):
    r = MagicMock(spec=RunResult)
    r.healthy = healthy
    r.summary = summary
    return r


def test_healthy_pipeline_returns_0(config_path):
    with patch("pipewatch.cli.PipelineRunner") as MockRunner:
        instance = MockRunner.return_value
        instance.run.return_value = _make_result(healthy=True, summary="all good")
        code = run_cli(["-c", str(config_path)])
    assert code == 0


def test_failing_pipeline_returns_1(config_path):
    with patch("pipewatch.cli.PipelineRunner") as MockRunner:
        instance = MockRunner.return_value
        instance.run.return_value = _make_result(healthy=False, summary="error rate high")
        code = run_cli(["-c", str(config_path)])
    assert code == 1


def test_single_pipeline_filter(config_path):
    with patch("pipewatch.cli.PipelineRunner") as MockRunner:
        instance = MockRunner.return_value
        instance.run.return_value = _make_result(healthy=True)
        code = run_cli(["-c", str(config_path), "--pipeline", "orders"])
        assert instance.run.call_count == 1
    assert code == 0


def test_dry_run_passes_flag_to_dispatcher(config_path):
    with patch("pipewatch.cli.AlertDispatcher") as MockDispatcher, \
         patch("pipewatch.cli.PipelineRunner") as MockRunner:
        MockRunner.return_value.run.return_value = _make_result(healthy=True)
        run_cli(["-c", str(config_path), "--dry-run"])
        MockDispatcher.assert_called_once_with(dry_run=True)


def test_verbose_flag_parsed(config_path):
    """Ensure --verbose is forwarded through argument parsing."""
    with patch("pipewatch.cli.PipelineRunner") as MockRunner:
        MockRunner.return_value.run.return_value = _make_result(healthy=True)
        parser = build_parser()
        args = parser.parse_args(["-c", str(config_path), "--verbose"])
        assert args.verbose is True
