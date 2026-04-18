"""Persistent run history storage for pipewatch."""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional


@dataclass
class HistoryEntry:
    pipeline: str
    timestamp: str
    healthy: bool
    alert_count: int
    alerts: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> "HistoryEntry":
        return cls(
            pipeline=data["pipeline"],
            timestamp=data["timestamp"],
            healthy=data["healthy"],
            alert_count=data["alert_count"],
            alerts=data.get("alerts", []),
        )

    def to_dict(self) -> dict:
        return asdict(self)


class RunHistory:
    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._entries: List[HistoryEntry] = []
        if self._path.exists():
            self._load()

    def _load(self) -> None:
        raw = json.loads(self._path.read_text())
        self._entries = [HistoryEntry.from_dict(e) for e in raw]

    def _save(self) -> None:
        self._path.write_text(json.dumps([e.to_dict() for e in self._entries], indent=2))

    def record(self, entry: HistoryEntry) -> None:
        self._entries.append(entry)
        self._save()

    def get(self, pipeline: Optional[str] = None) -> List[HistoryEntry]:
        if pipeline:
            return [e for e in self._entries if e.pipeline == pipeline]
        return list(self._entries)

    def prune(self, days: int, pipeline: Optional[str] = None, dry_run: bool = False) -> int:
        """Remove entries older than *days*. Returns count removed."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        removed = 0
        kept: List[HistoryEntry] = []
        for entry in self._entries:
            ts = datetime.fromisoformat(entry.timestamp)
            in_scope = (pipeline is None) or (entry.pipeline == pipeline)
            if in_scope and ts < cutoff:
                removed += 1
            else:
                kept.append(entry)
        if not dry_run:
            self._entries = kept
            self._save()
        return removed

    def clear(self) -> None:
        self._entries = []
        self._save()
