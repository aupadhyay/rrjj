#!/usr/bin/env python3
"""Validate the semantic contract of a paired benchmark result."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def require_object(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be an object")
    return value


def require_non_negative_number(value: Any, context: str) -> None:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or value < 0
    ):
        raise ValueError(f"{context} must be a non-negative number")


def validate(result: dict[str, Any], expected_mutations: int | None = None) -> None:
    if result.get("schema") != 1:
        raise ValueError("result schema must be 1")
    recording_off = require_object(result.get("recording_off"), "recording_off")
    recording_on = require_object(result.get("recording_on"), "recording_on")
    off_workload = require_object(recording_off.get("workload"), "off workload")
    on_workload = require_object(recording_on.get("workload"), "on workload")
    off_mutations = off_workload.get("logical_mutations")
    on_mutations = on_workload.get("logical_mutations")
    if off_mutations != on_mutations:
        raise ValueError("paired workloads reported different mutation counts")
    if expected_mutations is not None and off_mutations != expected_mutations:
        raise ValueError(
            f"expected {expected_mutations} mutations, received {off_mutations!r}"
        )
    for field in (
        "workload_seconds",
        "baseline_setup_seconds",
    ):
        require_non_negative_number(recording_off.get(field), f"recording_off.{field}")
        require_non_negative_number(recording_on.get(field), f"recording_on.{field}")
    for field in (
        "recorder_baseline_seconds",
        "flush_time_to_durable_seconds",
        "cold_session_open_and_index_seconds",
        "warm_index_op_to_tree_lookup_ns",
        "materialization_seconds",
        "events",
        "materialized_files",
    ):
        require_non_negative_number(recording_on.get(field), f"recording_on.{field}")
    if recording_on["events"] < 1:
        raise ValueError("recording_on.events must contain the durable timeline")
    semantics = require_object(result.get("semantics"), "semantics")
    if semantics.get("baseline_excluded_from_overhead") is not True:
        raise ValueError("baseline must be excluded from incremental overhead")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=Path)
    parser.add_argument("--expected-mutations", type=int)
    args = parser.parse_args()
    try:
        value = json.loads(args.result.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"cannot read benchmark result {args.result}: {error}") from error
    try:
        validate(require_object(value, "result"), args.expected_mutations)
    except ValueError as error:
        raise SystemExit(f"invalid benchmark result: {error}") from error
    print(f"validated benchmark result: {args.result}")


if __name__ == "__main__":
    main()
