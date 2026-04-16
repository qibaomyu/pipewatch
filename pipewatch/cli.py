"""CLI entry point for pipewatch."""
import sys
import argparse
from pathlib import Path

from pipewatch.config import load_config
from pipewatch.runner import PipelineRunner
from pipewatch.alerts import AlertDispatcher


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor and alert on data pipeline failures.",
    )
    parser.add_argument(
        "-c", "--config",
        default="pipewatch.yaml",
        help="Path to configuration file (default: pipewatch.yaml)",
    )
    parser.add_argument(
        "--pipeline",
        metavar="NAME",
        help="Run checks for a single named pipeline only",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Evaluate pipelines but do not dispatch alerts",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print status for every pipeline, not just failures",
    )
    return parser


def run_cli(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"[pipewatch] Config file not found: {config_path}", file=sys.stderr)
        return 2

    app_config = load_config(config_path)
    dispatcher = AlertDispatcher(dry_run=args.dry_run)
    runner = PipelineRunner(dispatcher=dispatcher)

    pipelines = app_config.pipelines
    if args.pipeline:
        pipelines = [p for p in pipelines if p.name == args.pipeline]
        if not pipelines:
            print(f"[pipewatch] No pipeline named '{args.pipeline}' found.", file=sys.stderr)
            return 2

    exit_code = 0
    for pipeline in pipelines:
        result = runner.run(pipeline)
        if args.verbose or not result.healthy:
            status = "OK" if result.healthy else "FAIL"
            print(f"[{status}] {pipeline.name}: {result.summary}")
        if not result.healthy:
            exit_code = 1

    return exit_code


def main():
    sys.exit(run_cli())


if __name__ == "__main__":
    main()
