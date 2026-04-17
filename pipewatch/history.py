"""Persistent run history for pipeline monitoring."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class HistoryEntry:
    pipeline: str
    timestamp: str
    healthy: bool
    error_rate: float
    latency: float
    alert_count: int

    @staticmethod
    def from_dict(d: dict) -> "HistoryEntry":
        return HistoryEntry(**d)

    def to_dict(self) -> dict:
        return asdict(self)


class RunHistory:
    def __init__(self, path: str = ".pipewatch_history.json"):
        self.path = path
        self._entries: List[HistoryEntry] = []
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.path):
            try:
                with open(self.path) as f:
                    raw = json.load(f)
                self._entries = [HistoryEntry.from_dict(e) for e in raw]
            except (json.JSONDecodeError, KeyError, TypeError):
                self._entries = []

    def save(self) -> None:
        with open(self.path, "w") as f:
            json.dump([e.to_dict() for e in self._entries], f, indent=2)

    def record(self, entry: HistoryEntry) -> None:
        self._entries.append(entry)
        self.save()

    def get(self, pipeline: Optional[str] = None) -> List[HistoryEntry]:
        if pipeline is None:
            return list(self._entries)
        return [e for e in self._entries if e.pipeline == pipeline]

    def clear(self, pipeline: Optional[str] = None) -> None:
        if pipeline is None:
            self._entries = []
        else:
            self._entries = [e for e in self._entries if e.pipeline != pipeline]
        self.save()

    def last(self, pipeline: str) -> Optional[HistoryEntry]:
        entries = self.get(pipeline)
        return entries[-1] if entries else None
