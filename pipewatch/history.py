"""Persistent run history for pipewatch."""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class HistoryEntry:
    pipeline: str
    timestamp: str
    status: str
    alerts: List[Dict] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> "HistoryEntry":
        return cls(
            pipeline=d["pipeline"],
            timestamp=d["timestamp"],
            status=d["status"],
            alerts=d.get("alerts", []),
        )

    def to_dict(self) -> dict:
        return asdict(self)


class RunHistory:
    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._data: Dict[str, List[dict]] = {}
        if self._path.exists():
            self._load()

    def _load(self) -> None:
        with self._path.open() as fh:
            self._data = json.load(fh)

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w") as fh:
            json.dump(self._data, fh, indent=2)

    def record(self, entry: HistoryEntry) -> None:
        self._data.setdefault(entry.pipeline, []).append(entry.to_dict())
        self._save()

    def get(self, pipeline: str, limit: Optional[int] = None) -> List[HistoryEntry]:
        raw = self._data.get(pipeline, [])
        if limit:
            raw = raw[-limit:]
        return [HistoryEntry.from_dict(r) for r in raw]

    def pipelines(self) -> List[str]:
        return list(self._data.keys())

    def prune(self, pipeline: str, keep: int) -> int:
        entries = self._data.get(pipeline, [])
        removed = max(0, len(entries) - keep)
        self._data[pipeline] = entries[-keep:] if keep else []
        self._save()
        return removed

    def clear(self, pipeline: Optional[str] = None) -> None:
        if pipeline:
            self._data.pop(pipeline, None)
        else:
            self._data = {}
        self._save()
