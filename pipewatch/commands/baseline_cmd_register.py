"""Register baseline subcommand with the CLI argument parser."""
from pipewatch.commands.baseline_cmd import DEFAULT_BASELINE_FILE, run_baseline_cmd


def register_baseline_subcommand(subparsers) -> None:
    p = subparsers.add_parser("baseline", help="Manage pipeline performance baselines")
    p.add_argument(
        "baseline_action",
        choices=["set", "show"],
        help="Action to perform",
    )
    p.add_argument("--pipeline", default=None, help="Pipeline name")
    p.add_argument(
        "--error-rate",
        dest="error_rate",
        type=float,
        default=0.0,
        help="Error rate to record (0.0–1.0)",
    )
    p.add_argument(
        "--latency",
        type=float,
        default=0.0,
        help="Latency in seconds to record",
    )
    p.add_argument(
        "--baseline-file",
        dest="baseline_file",
        default=DEFAULT_BASELINE_FILE,
        help="Path to baseline storage file",
    )
    p.set_defaults(func=run_baseline_cmd)
