"""Export pipeline run history to CSV or JSON format."""

import json
import csv
import io
import sys
from datetime import datetime

from pipewatch.history import RunHistory


def _entry_to_dict(entry):
    return {
        "pipeline": entry.pipeline,
        "timestamp": entry.timestamp.isoformat(),
        "healthy": entry.healthy,
        "error_rate": entry.error_rate,
        "latency": entry.latency,
        "alert_count": entry.alert_count,
    }


def run_export_cmd(args):
    history = RunHistory(args.history_file)
    entries = history.get(pipeline=getattr(args, "pipeline", None))

    if not entries:
        print("No history entries found.", file=sys.stderr)
        return 0

    rows = [_entry_to_dict(e) for e in entries]

    if args.format == "json":
        out = json.dumps(rows, indent=2)
        if args.output:
            with open(args.output, "w") as f:
                f.write(out)
        else:
            print(out)
    else:
        fieldnames = ["pipeline", "timestamp", "healthy", "error_rate", "latency", "alert_count"]
        if args.output:
            f = open(args.output, "w", newline="")
            close = True
        else:
            f = sys.stdout
            close = False
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        if close:
            f.close()

    return 0
