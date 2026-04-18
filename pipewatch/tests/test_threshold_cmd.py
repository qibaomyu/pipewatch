"""Tests for the threshold command."""
import json
from argparse import Namespace
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.commands.threshold_cmd import run_threshold_cmd
from pipewatch.config import PipelineConfig, AppConfig


def _make_cfg(*pipelines):
    cfg = MagicMock(spec=AppConfig)
    cfg.pipelines = list(pipelines)
    return cfg


def _pipeline(name, error_rate=0.05, latency=30.0, throughput=10):
    p = MagicMock(spec=PipelineConfig)
    p.name = name
    p.max_error_rate = error_rate
    p.max_latency_seconds = latency
    p.min_throughput = throughput
    return p


def _args(config="pipewatch.yaml", pipeline=None, as_json=False):
    return Namespace(config=config, pipeline=pipeline, json=as_json)


@pytest.fixture
def patch_load():
    def _patch(cfg):
        return patch("pipewatch.commands.threshold_cmd.load_config", return_value=cfg)
    return _patch


def test_missing_config_returns_exit_code_2(tmp_path):
    args = _args(config=str(tmp_path / "missing.yaml"))
    with patch("pipewatch.commands.threshold_cmd.load_config", side_effect=FileNotFoundError):
        assert run_threshold_cmd(args) == 2


def test_unknown_pipeline_returns_exit_code_2(patch_load, capsys):
    cfg = _make_cfg(_pipeline("pipe_a"))
    args = _args(pipeline="nonexistent")
    with patch_load(cfg):
        code = run_threshold_cmd(args)
    assert code == 2
    assert "Unknown pipeline" in capsys.readouterr().out


def test_text_output_shows_all_pipelines(patch_load, capsys):
    cfg = _make_cfg(_pipeline("alpha"), _pipeline("beta", error_rate=0.1))
    args = _args()
    with patch_load(cfg):
        code = run_threshold_cmd(args)
    assert code == 0
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out


def test_json_output(patch_load, capsys):
    cfg = _make_cfg(_pipeline("pipe_a", error_rate=0.02, latency=60.0, throughput=5))
    args = _args(as_json=True)
    with patch_load(cfg):
        code = run_threshold_cmd(args)
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert data[0]["pipeline"] == "pipe_a"
    assert data[0]["max_error_rate"] == 0.02
    assert data[0]["max_latency_seconds"] == 60.0
    assert data[0]["min_throughput"] == 5


def test_filter_by_pipeline(patch_load, capsys):
    cfg = _make_cfg(_pipeline("alpha"), _pipeline("beta"))
    args = _args(pipeline="alpha", as_json=True)
    with patch_load(cfg):
        code = run_threshold_cmd(args)
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert len(data) == 1
    assert data[0]["pipeline"] == "alpha"


def test_empty_pipelines_prints_message(patch_load, capsys):
    cfg = _make_cfg()
    args = _args()
    with patch_load(cfg):
        code = run_threshold_cmd(args)
    assert code == 0
    assert "No pipelines configured" in capsys.readouterr().out
