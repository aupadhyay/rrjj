#!/usr/bin/env python3
"""Prepare and run a pinned real-repository workload."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path


def run(command: list[str], cwd: Path | None = None) -> float:
    started = time.perf_counter()
    subprocess.run(command, cwd=cwd, check=True)
    return time.perf_counter() - started


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("setup", "mutate"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--repository")
    parser.add_argument("--revision")
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="command after --, run inside the checkout during mutate",
    )
    args = parser.parse_args()

    if args.phase == "setup":
        if not args.repository or not args.revision:
            parser.error("setup requires --repository and --revision")
        if args.root.exists():
            parser.error(f"setup destination already exists: {args.root}")
        seconds = run(
            [
                "git",
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                args.repository,
                str(args.root),
            ]
        )
        seconds += run(["git", "checkout", "--detach", args.revision], args.root)
        print(
            json.dumps(
                {
                    "phase": "setup",
                    "repository": args.repository,
                    "revision": subprocess.check_output(
                        ["git", "rev-parse", "HEAD"], cwd=args.root, text=True
                    ).strip(),
                    "seconds": seconds,
                },
                sort_keys=True,
            )
        )
        return

    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("mutate requires a command after --")
    seconds = run(command, args.root)
    print(
        json.dumps(
            {
                "phase": "mutate",
                "command": command,
                "logical_mutations": None,
                "seconds": seconds,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
