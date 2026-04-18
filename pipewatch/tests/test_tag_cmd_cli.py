"""CLI-level tests for the 'tag' subcommand registration."""
from pipewatch.commands.tag_cmd_register import register_tag_subcommand
import argparse


def _parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    register_tag_subcommand(sub)
    return p


def test_tag_subcommand_add_defaults():
    p = _parser()
    args = p.parse_args(["tag", "add", "--pipeline", "etl", "--tag", "critical"])
    assert args.tag_action == "add"
    assert args.pipeline == "etl"
    assert args.tag == "critical"
    assert args.tags_file == "pipewatch_tags.json"


def test_tag_subcommand_remove():
    p = _parser()
    args = p.parse_args(["tag", "remove", "--pipeline", "etl", "--tag", "critical"])
    assert args.tag_action == "remove"


def test_tag_subcommand_list():
    p = _parser()
    args = p.parse_args(["tag", "list", "--pipeline", "etl"])
    assert args.tag_action == "list"
    assert args.pipeline == "etl"


def test_tag_subcommand_find_custom_file():
    p = _parser()
    args = p.parse_args(["tag", "find", "--tag", "critical", "--tags-file", "my_tags.json"])
    assert args.tag_action == "find"
    assert args.tags_file == "my_tags.json"
