#!/usr/bin/env python3
"""Deterministic fixed-working-set filesystem workload."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path


def payload(index: int, size: int) -> bytes:
    prefix = f"{index:020d}:".encode()
    repeats = (size + len(prefix) - 1) // len(prefix)
    return (prefix * repeats)[:size]


def initialize(root: Path, working_set: int, file_bytes: int) -> dict[str, object]:
    started = time.perf_counter()
    root.mkdir(parents=True, exist_ok=True)
    for index in range(working_set):
        (root / f"file-{index:08d}.dat").write_bytes(payload(index, file_bytes))
    return {
        "phase": "setup",
        "working_set": working_set,
        "file_bytes": file_bytes,
        "files_created": working_set,
        "seconds": time.perf_counter() - started,
    }


def mutate(
    root: Path,
    mutations: int,
    working_set: int,
    file_bytes: int,
    burst_size: int,
    burst_pause_ms: float,
) -> dict[str, object]:
    started = time.perf_counter()
    for index in range(mutations):
        slot = index % working_set
        (root / f"file-{slot:08d}.dat").write_bytes(
            payload(working_set + index, file_bytes)
        )
        if burst_pause_ms and (index + 1) % burst_size == 0:
            time.sleep(burst_pause_ms / 1000)
    return {
        "phase": "mutate",
        "logical_mutations": mutations,
        "working_set": working_set,
        "file_bytes": file_bytes,
        "burst_size": burst_size,
        "burst_pause_ms": burst_pause_ms,
        "seconds": time.perf_counter() - started,
        "pid": os.getpid(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("setup", "mutate"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--mutations", type=int, default=1_000_000)
    parser.add_argument("--working-set", type=int, default=10_000)
    parser.add_argument("--file-bytes", type=int, default=64)
    parser.add_argument("--burst-size", type=int, default=10_000)
    parser.add_argument("--burst-pause-ms", type=float, default=0)
    args = parser.parse_args()
    if min(args.mutations, args.working_set, args.file_bytes, args.burst_size) < 1:
        parser.error("counts and file size must be positive")
    if args.burst_pause_ms < 0:
        parser.error("--burst-pause-ms must be non-negative")
    return args


def main() -> None:
    args = parse_args()
    result = (
        initialize(args.root, args.working_set, args.file_bytes)
        if args.phase == "setup"
        else mutate(
            args.root,
            args.mutations,
            args.working_set,
            args.file_bytes,
            args.burst_size,
            args.burst_pause_ms,
        )
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
