"""Command-line entry point."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from cdc_reconciliation.generator import CDCConfig, generate_cdc_dataset
from cdc_reconciliation.processor import CDCProcessor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cdc-reconcile",
        description="Run the synthetic CDC ordering and reconciliation demo.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    generate = commands.add_parser("generate")
    generate.add_argument("--output", type=Path, required=True)
    generate.add_argument("--seed", type=int, default=20260728)
    generate.add_argument("--entities", type=int, default=24)

    run = commands.add_parser("run")
    run.add_argument("--events", type=Path, required=True)
    run.add_argument("--source-snapshot", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)

    demo = commands.add_parser("demo")
    demo.add_argument("--workspace", type=Path, required=True)
    demo.add_argument("--seed", type=int, default=20260728)
    demo.add_argument("--entities", type=int, default=24)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "generate":
        result = generate_cdc_dataset(
            args.output,
            CDCConfig(seed=args.seed, entities=args.entities),
        )
    elif args.command == "run":
        result = CDCProcessor(args.output).run(args.events, args.source_snapshot)
    else:
        input_dir = args.workspace / "input"
        output_dir = args.workspace / "warehouse"
        generate_cdc_dataset(
            input_dir,
            CDCConfig(seed=args.seed, entities=args.entities),
        )
        result = CDCProcessor(output_dir).run(
            input_dir / "events.jsonl",
            input_dir / "source_snapshot.csv",
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

