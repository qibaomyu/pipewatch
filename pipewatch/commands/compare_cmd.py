"""Compare pipeline metrics across two time windows."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from pipewatch.history import RunHistory, HistoryEntry


def _window_entries(
    entries: List[HistoryEntry], hours_ago_start: float, hours_ago_end: float
) -> List[HistoryEntry]:
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(hours=hours_ago_start)
    end = now - timedelta(hours=hours_ago_end)
    return [e for e in entries if start <= e.timestamp <= end]


def _window_stats(entries: List[HistoryEntry]) -> dict:
    if not entries:
        return {"count": 0, "error_rate": None, "avg_latency": None, "failures": 0}
    error_rates = [e.error_rate for e in entries if e.error_rate is not None]
    latencies = [e.latency for e in entries if e.latency is not None]
    failures = sum(1 for e in entries if not e.healthy)
    return {
        "count": len(entries),
        "error_rate": round(sum(error_rates) / len(error_rates), 4) if error_rates else None,
        "avg_latency": round(sum(latencies) / len(latencies), 4) if latencies else None,
        "failures": failures,
    }


def _pipeline_compare(pipeline: str, entries: List[HistoryEntry], window: int) -> dict:
    pipe_entries = [e for e in entries if e.pipeline == pipeline]
    current = _window_stats(_window_entries(pipe_entries, window, 0))
    previous = _window_stats(_window_entries(pipe_entries, window * 2, window))
    return {"pipeline": pipeline, "current": current, "previous": previous}


def run_compare_cmd(args, history: Optional[RunHistory] = None) -> int:
    h = history or RunHistory(path=args.history_file)
    entries = h.all()
    pipelines = (
        [args.pipeline]
        if args.pipeline
        else sorted({e.pipeline for e in entries})
    )
    if not pipelines:
        print("No history available.")
        return 0

    results = [_pipeline_compare(p, entries, args.window) for p in pipelines]

    if args.format == "json":
        print(json.dumps(results, indent=2, default=str))
    else:
        for r in results:
            cur, prev = r["current"], r["previous"]
            print(f"Pipeline: {r['pipeline']}")
            print(f"  Current  ({args.window}h): runs={cur['count']}  failures={cur['failures']}  "
                  f"error_rate={cur['error_rate']}  avg_latency={cur['avg_latency']}")
            print(f"  Previous ({args.window}h): runs={prev['count']}  failures={prev['failures']}  "
                  f"error_rate={prev['error_rate']}  avg_latency={prev['avg_latency']}")
    return 0
