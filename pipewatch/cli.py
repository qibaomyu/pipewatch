"""CLI entry-point for pipewatch."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.commands.history_cmd import run_history_cmd
from pipewatch.commands.silence_cmd import run_silence_cmd
from pipewatch.commands.snapshot_cmd import run_snapshot_cmd
from pipewatch.commands.prune_cmd import run_prune_cmd
from pipewatch.commands.summary_cmd import run_summary_cmd
from pipewatch.commands.export_cmd import run_export_cmd
from pipewatch.commands.report_cmd import run_report_cmd
from pipewatch.commands.threshold_cmd import run_threshold_cmd
from pipewatch.commands.trend_cmd import run_trend_cmd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor and alert on data pipeline failures.",
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--history-file", default="pipewatch_history.json", dest="history_file"
    )

    sub = parser.add_subparsers(dest="subcommand")

    # snapshot
    snap = sub.add_parser("snapshot", help="Run a snapshot check.")
    snap.add_argument("--pipeline", default=None)
    snap.add_argument("--format", choices=["text", "json"], default="text", dest="fmt")
    snap.add_argument("--exit-code", action="store_true", dest="exit_code")

    # history
    hist = sub.add_parser("history", help="Show run history.")
    hist.add_argument("--pipeline", default=None)
    hist.add_argument("--limit", type=int, default=20)

    # silence
    sil = sub.add_parser("silence", help="Silence a pipeline.")
    sil.add_argument("pipeline")
    sil.add_argument("--minutes", type=int, default=60)

    # prune
    prune = sub.add_parser("prune", help="Prune old history entries.")
    prune.add_argument("--days", type=int, default=30)

    # summary
    summ = sub.add_parser("summary", help="Summarise pipeline health.")
    summ.add_argument("--pipeline", default=None)
    summ.add_argument("--format", choices=["text", "json"], default="text", dest="fmt")

    # export
    exp = sub.add_parser("export", help="Export history to JSON or CSV.")
    exp.add_argument("--pipeline", default=None)
    exp.add_argument("--format", choices=["json", "csv"], default="json", dest="fmt")
    exp.add_argument("--output", default=None)

    # report
    rep = sub.add_parser("report", help="Generate a health report.")
    rep.add_argument("--hours", type=int, default=24)
    rep.add_argument("--format", choices=["text", "json"], default="text", dest="fmt")
    rep.add_argument("--exit-code", action="store_true", dest="exit_code")

    # threshold
    thr = sub.add_parser("threshold", help="Show configured thresholds.")
    thr.add_argument("--pipeline", default=None)
    thr.add_argument("--format", choices=["text", "json"], default="text", dest="fmt")

    # trend
    trend = sub.add_parser("trend", help="Show error-rate and latency trends.")
    trend.add_argument("--pipeline", default=None)
    trend.add_argument("--buckets", type=int, default=5)

    return parser


def run_cli(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.subcommand is None:
        parser.print_help()
        return 0

    dispatch = {
        "snapshot": run_snapshot_cmd,
        "history": run_history_cmd,
        "silence": run_silence_cmd,
        "prune": run_prune_cmd,
        "summary": run_summary_cmd,
        "export": run_export_cmd,
        "report": run_report_cmd,
        "threshold": run_threshold_cmd,
        "trend": run_trend_cmd,
    }

    handler = dispatch.get(args.subcommand)
    if handler is None:
        parser.print_help()
        return 2

    return handler(args)


def main():
    sys.exit(run_cli())
