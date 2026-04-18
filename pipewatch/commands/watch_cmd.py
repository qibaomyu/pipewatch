"""watch_cmd: continuously poll pipelines and alert on threshold breaches."""
from __future__ import annotations

import time
from argparse import Namespace
from typing import List

from pipewatch.config import load_config
from pipewatch.runner import PipelineRunner
from pipewatch.alerts import AlertDispatcher
from pipewatch.formatter import format_results


def run_watch_cmd(args: Namespace) -> int:
    """Poll all (or selected) pipelines every *interval* seconds."""
    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"Config file not found: {args.config}")
        return 2

    pipelines = cfg.pipelines
    if args.pipeline:
        pipelines = [p for p in pipelines if p.name in args.pipeline]
        if not pipelines:
            print(f"No pipelines matched: {args.pipeline}")
            return 2

    dispatcher = AlertDispatcher()
    runner = PipelineRunner(dispatcher)
    iterations = 0

    try:
        while True:
            results = [runner.run(p) for p in pipelines]
            output = format_results(results, fmt=args.format)
            print(output, flush=True)

            iterations += 1
            if args.count and iterations >= args.count:
                break

            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass

    if args.exit_code:
        return 1 if any(not r.status.healthy for r in results) else 0
    return 0


def register_watch_subcommand(subparsers) -> None:
    p = subparsers.add_parser("watch", help="Continuously poll pipelines")
    p.add_argument("--config", default="pipewatch.yaml")
    p.add_argument("--pipeline", nargs="+", metavar="NAME")
    p.add_argument("--interval", type=float, default=30.0,
                   help="Seconds between polls (default: 30)")
    p.add_argument("--count", type=int, default=0,
                   help="Stop after N iterations (0 = run forever)")
    p.add_argument("--format", choices=["text", "json"], default="text")
    p.add_argument("--exit-code", action="store_true",
                   help="Exit 1 if any pipeline is unhealthy")
    p.set_defaults(func=run_watch_cmd)
