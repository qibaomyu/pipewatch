"""Tests for pipewatch/commands/tag_cmd.py"""
import json
import pytest
from pipewatch.commands.tag_cmd import (
    add_tag,
    remove_tag,
    get_tags,
    pipelines_with_tag,
    run_tag_cmd,
)


@pytest.fixture
def tags_file(tmp_path):
    return str(tmp_path / "tags.json")


def test_add_tag_creates_entry(tags_file):
    add_tag("etl", "critical", tags_file)
    assert "critical" in get_tags("etl", tags_file)


def test_add_tag_no_duplicates(tags_file):
    add_tag("etl", "critical", tags_file)
    add_tag("etl", "critical", tags_file)
    assert get_tags("etl", tags_file).count("critical") == 1


def test_remove_tag_returns_true(tags_file):
    add_tag("etl", "critical", tags_file)
    result = remove_tag("etl", "critical", tags_file)
    assert result is True
    assert get_tags("etl", tags_file) == []


def test_remove_tag_unknown_returns_false(tags_file):
    assert remove_tag("etl", "nope", tags_file) is False


def test_remove_tag_cleans_up_empty_pipeline(tags_file):
    add_tag("etl", "critical", tags_file)
    remove_tag("etl", "critical", tags_file)
    data = json.loads(open(tags_file).read())
    assert "etl" not in data


def test_get_tags_unknown_pipeline(tags_file):
    assert get_tags("ghost", tags_file) == []


def test_pipelines_with_tag(tags_file):
    add_tag("etl", "critical", tags_file)
    add_tag("loader", "critical", tags_file)
    add_tag("other", "low", tags_file)
    result = pipelines_with_tag("critical", tags_file)
    assert set(result) == {"etl", "loader"}


def _args(action, pipeline=None, tag=None, tags_file=""):
    class A:
        pass
    a = A()
    a.tag_action = action
    a.pipeline = pipeline
    a.tag = tag
    a.tags_file = tags_file
    return a


def test_run_tag_cmd_add(tags_file, capsys):
    rc = run_tag_cmd(_args("add", "etl", "critical", tags_file))
    assert rc == 0
    assert "critical" in capsys.readouterr().out


def test_run_tag_cmd_remove_missing(tags_file, capsys):
    rc = run_tag_cmd(_args("remove", "etl", "nope", tags_file))
    assert rc == 2


def test_run_tag_cmd_list(tags_file, capsys):
    add_tag("etl", "critical", tags_file)
    run_tag_cmd(_args("list", "etl", tags_file=tags_file))
    assert "critical" in capsys.readouterr().out


def test_run_tag_cmd_find(tags_file, capsys):
    add_tag("etl", "critical", tags_file)
    run_tag_cmd(_args("find", tag="critical", tags_file=tags_file))
    assert "etl" in capsys.readouterr().out
