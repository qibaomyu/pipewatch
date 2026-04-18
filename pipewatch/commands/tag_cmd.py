"""Tag pipelines with arbitrary labels for grouping and filtering."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

_DEFAULT_TAGS_FILE = "pipewatch_tags.json"


def _load_tags(tags_file: str) -> Dict[str, List[str]]:
    path = Path(tags_file)
    if not path.exists():
        return {}
    with path.open() as fh:
        return json.load(fh)


def _save_tags(tags: Dict[str, List[str]], tags_file: str) -> None:
    with Path(tags_file).open("w") as fh:
        json.dump(tags, fh, indent=2)


def add_tag(pipeline: str, tag: str, tags_file: str = _DEFAULT_TAGS_FILE) -> None:
    tags = _load_tags(tags_file)
    existing = tags.get(pipeline, [])
    if tag not in existing:
        existing.append(tag)
    tags[pipeline] = existing
    _save_tags(tags, tags_file)


def remove_tag(pipeline: str, tag: str, tags_file: str = _DEFAULT_TAGS_FILE) -> bool:
    tags = _load_tags(tags_file)
    if pipeline not in tags or tag not in tags[pipeline]:
        return False
    tags[pipeline].remove(tag)
    if not tags[pipeline]:
        del tags[pipeline]
    _save_tags(tags, tags_file)
    return True


def get_tags(pipeline: str, tags_file: str = _DEFAULT_TAGS_FILE) -> List[str]:
    return _load_tags(tags_file).get(pipeline, [])


def pipelines_with_tag(tag: str, tags_file: str = _DEFAULT_TAGS_FILE) -> List[str]:
    return [p for p, tags in _load_tags(tags_file).items() if tag in tags]


def run_tag_cmd(args) -> int:
    tags_file = getattr(args, "tags_file", _DEFAULT_TAGS_FILE)
    if args.tag_action == "add":
        add_tag(args.pipeline, args.tag, tags_file)
        print(f"Tagged '{args.pipeline}' with '{args.tag}'.")
    elif args.tag_action == "remove":
        removed = remove_tag(args.pipeline, args.tag, tags_file)
        if not removed:
            print(f"Tag '{args.tag}' not found on '{args.pipeline}'.")
            return 2
        print(f"Removed tag '{args.tag}' from '{args.pipeline}'.")
    elif args.tag_action == "list":
        tags = get_tags(args.pipeline, tags_file)
        if not tags:
            print(f"No tags for '{args.pipeline}'.")
        else:
            print(", ".join(tags))
    elif args.tag_action == "find":
        pipelines = pipelines_with_tag(args.tag, tags_file)
        if not pipelines:
            print(f"No pipelines tagged '{args.tag}'.")
        else:
            print("\n".join(pipelines))
    return 0
