#!/usr/bin/env python3
"""Validate a rrjj event-latency benchmark result without performance bounds."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def require_object(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be an object")
    return value


def require_number(value: Any, context: str) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{context} must be a number")


def validate(result: dict[str, Any]) -> None:
    if result.get("schema") != 1 or result.get("benchmark") != "rrjj_event_latency":
        raise ValueError("not a latency result schema 1")
    if result.get("failures") != []:
        raise ValueError("latency result contains failures")
    samples = result.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ValueError("latency result must contain samples")
    modes = set()
    paths = set()
    for index, raw_sample in enumerate(samples):
        sample = require_object(raw_sample, f"sample {index}")
        if sample.get("status") != "ok":
            raise ValueError(f"sample {index} did not complete")
        mode = sample.get("mode")
        if mode not in {"isolated", "burst", "continuous"}:
            raise ValueError(f"sample {index} has invalid mode")
        modes.add(mode)
        path = sample.get("path")
        if not isinstance(path, str) or not path or path in paths:
            raise ValueError(f"sample {index} path is missing or duplicated")
        paths.add(path)
        events = require_object(sample.get("events"), f"sample {index} events")
        touched = require_object(events.get("touched_paths"), "touched_paths")
        snapshot = require_object(events.get("snapshot"), "snapshot")
        if touched.get("seq", -1) >= snapshot.get("seq", -1):
            raise ValueError(f"sample {index} snapshot does not follow touched_paths")
        latencies = require_object(sample.get("latency_ms"), "latency_ms")
        for name, value in latencies.items():
            require_number(value, f"sample {index} latency {name}")
        monotonic_completion = latencies.get(
            "edit_completion_to_snapshot_receipt_monotonic"
        )
        require_number(monotonic_completion, "snapshot completion latency")
        if monotonic_completion < 0:
            raise ValueError("monotonic snapshot completion latency is negative")
    if modes != {"isolated", "burst", "continuous"}:
        raise ValueError(f"latency result is missing modes: {sorted(modes)}")
    summary = require_object(result.get("summary"), "summary")
    all_summary = require_object(summary.get("all"), "summary.all")
    if all_summary.get("successful") != len(samples) or all_summary.get("failed") != 0:
        raise ValueError("summary counts do not match samples")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=Path)
    args = parser.parse_args()
    try:
        value = json.loads(args.result.read_text())
        validate(require_object(value, "result"))
    except (OSError, json.JSONDecodeError, ValueError) as error:
        raise SystemExit(f"invalid latency benchmark result: {error}") from error
    print(f"validated latency benchmark result: {args.result}")


if __name__ == "__main__":
    main()
