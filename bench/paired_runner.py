#!/usr/bin/env python3
"""Run the same workload with rrjj recording off and on."""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]


class BenchmarkDataError(RuntimeError):
    """A benchmark artifact does not satisfy the expected data contract."""


def run_json(command: list[str], **kwargs: Any) -> tuple[Any, float]:
    started = time.perf_counter()
    result = subprocess.run(command, check=True, text=True, capture_output=True, **kwargs)
    seconds = time.perf_counter() - started
    try:
        return json.loads(result.stdout), seconds
    except json.JSONDecodeError as error:
        raise RuntimeError(f"non-JSON output from {command!r}: {result.stdout}") from error


def require_object(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BenchmarkDataError(f"{context} must be a JSON object")
    return value


def read_json_object(path: Path, context: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except FileNotFoundError as error:
        raise BenchmarkDataError(f"missing {context}: {path}") from error
    except json.JSONDecodeError as error:
        raise BenchmarkDataError(
            f"malformed {context} at {path}:{error.lineno}:{error.colno}: {error.msg}"
        ) from error
    return require_object(value, context)


def workload_commands(
    args: argparse.Namespace, root: Path
) -> tuple[list[str], list[str]]:
    if args.workload == "synthetic":
        common = [
            sys.executable,
            str(REPO / "bench/workloads/synthetic.py"),
            "--root",
            str(root),
            "--working-set",
            str(args.working_set),
            "--file-bytes",
            str(args.file_bytes),
            "--burst-size",
            str(args.burst_size),
            "--burst-pause-ms",
            str(args.burst_pause_ms),
        ]
        return [common[0], common[1], "setup", *common[2:]], [
            common[0],
            common[1],
            "mutate",
            *common[2:],
            "--mutations",
            str(args.mutations),
        ]
    setup = [
        sys.executable,
        str(REPO / "bench/workloads/real.py"),
        "setup",
        "--root",
        str(root),
        "--repository",
        args.repository,
        "--revision",
        args.revision,
    ]
    mutate = [
        sys.executable,
        str(REPO / "bench/workloads/real.py"),
        "mutate",
        "--root",
        str(root),
        "--",
        *args.command,
    ]
    return setup, mutate


def wait_for_daemon(binary: Path, socket: Path, process: subprocess.Popen[str]) -> float:
    started = time.perf_counter()
    while time.perf_counter() - started < 120:
        if process.poll() is not None:
            raise RuntimeError(f"rrjj daemon exited with status {process.returncode}")
        result = subprocess.run(
            [str(binary), "status", "--socket", str(socket)],
            text=True,
            capture_output=True,
        )
        if result.returncode == 0:
            return time.perf_counter() - started
        time.sleep(0.02)
    raise TimeoutError("rrjj daemon did not become ready within 120 seconds")


def summarize_events(
    session: Path,
    expected_session_id: str | None = None,
    expected_durable_seq: int | None = None,
    events_object: str = "events/000000.ndjson",
) -> dict[str, Any]:
    event_path = session / events_object
    try:
        lines = event_path.read_text().splitlines()
    except FileNotFoundError as error:
        raise BenchmarkDataError(f"missing durable event stream: {event_path}") from error
    events: list[dict[str, Any]] = []
    session_id: str | None = None
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            event = require_object(
                json.loads(line), f"event at {event_path}:{line_number}"
            )
        except json.JSONDecodeError as error:
            raise BenchmarkDataError(
                f"malformed event at {event_path}:{line_number}: {error.msg}"
            ) from error
        sequence = event.get("seq")
        expected_sequence = len(events)
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence != expected_sequence
        ):
            raise BenchmarkDataError(
                f"invalid event sequence at {event_path}:{line_number}: "
                f"expected {expected_sequence}, received {sequence!r}"
            )
        current_session_id = event.get("session_id")
        if not isinstance(current_session_id, str) or not current_session_id:
            raise BenchmarkDataError(
                f"missing event session_id at {event_path}:{line_number}"
            )
        if session_id is not None and current_session_id != session_id:
            raise BenchmarkDataError(
                f"event session changed at {event_path}:{line_number}"
            )
        event_type = event.get("type")
        if not isinstance(event_type, str) or not event_type:
            raise BenchmarkDataError(
                f"missing event type at {event_path}:{line_number}"
            )
        require_object(event.get("data"), f"event data at {event_path}:{line_number}")
        session_id = current_session_id
        events.append(event)
    if not events:
        raise BenchmarkDataError(f"durable event stream is empty: {event_path}")
    if expected_session_id is not None and session_id != expected_session_id:
        raise BenchmarkDataError(
            f"event session {session_id!r} does not match manifest "
            f"session {expected_session_id!r}"
        )
    if expected_durable_seq is not None and events[-1]["seq"] != expected_durable_seq:
        raise BenchmarkDataError(
            f"durable event stream ends at {events[-1]['seq']}, "
            f"manifest advertises {expected_durable_seq}"
        )
    touched = [
        require_object(event["data"], "touched_paths event data")
        for event in events
        if event["type"] == "touched_paths"
    ]
    touched_paths: list[dict[str, Any]] = []
    for window in touched:
        raw_events = window.get("raw_events", 0)
        if (
            not isinstance(raw_events, int)
            or isinstance(raw_events, bool)
            or raw_events < 0
        ):
            raise BenchmarkDataError("touched_paths data.raw_events must be unsigned")
        items = window.get("paths", [])
        if not isinstance(items, list):
            raise BenchmarkDataError("touched_paths data.paths must be an array")
        for item in items:
            path_entry = require_object(item, "touched_paths path entry")
            if not isinstance(path_entry.get("path"), str):
                raise BenchmarkDataError("touched_paths path must be a string")
            operations = path_entry.get("operations", [])
            if not isinstance(operations, list) or not all(
                isinstance(operation, str) for operation in operations
            ):
                raise BenchmarkDataError(
                    "touched_paths path operations must be a string array"
                )
            touched_paths.append(path_entry)
    paths = {item["path"] for item in touched_paths}
    operations = sorted(
        {
            operation
            for item in touched_paths
            for operation in item.get("operations", [])
        }
    )
    return {
        "events": len(events),
        "snapshot_count": sum(event["type"] == "snapshot" for event in events),
        "touched_path_windows": len(touched),
        "distinct_touched_paths": len(paths),
        "raw_watcher_events": sum(window.get("raw_events", 0) for window in touched),
        "observed_touch_operations": operations,
        "overflow_events": sum(event["type"] == "overflow" for event in events),
    }


def reader_metrics(binary: Path, session: Path, last_op: str, work: Path) -> dict[str, Any]:
    index, cold_seconds = run_json([str(binary), "index", str(session)])
    if not isinstance(index, list):
        raise BenchmarkDataError("rrjj index output must be a JSON array")
    entries = [
        require_object(entry, f"rrjj index entry {position}")
        for position, entry in enumerate(index)
    ]
    op_to_tree = {
        entry["op"]: entry["tree"]
        for entry in entries
        if isinstance(entry.get("op"), str) and isinstance(entry.get("tree"), str)
    }
    durable_tree = op_to_tree.get(last_op)
    if durable_tree is None:
        raise BenchmarkDataError(
            f"durable operation {last_op!r} is missing from rrjj index output"
        )
    lookup_iterations = 100_000
    started = time.perf_counter_ns()
    for _ in range(lookup_iterations):
        if op_to_tree.get(last_op) != durable_tree:
            raise AssertionError("operation lookup returned no tree")
    lookup_ns = (time.perf_counter_ns() - started) / lookup_iterations
    destination = work / "materialized"
    _, materialize_seconds = run_json(
        [str(binary), "materialize", str(session), last_op, str(destination)]
    )
    return {
        "cold_session_open_and_index_seconds": cold_seconds,
        "warm_index_op_to_tree_lookup_ns": lookup_ns,
        "warm_lookup_iterations": lookup_iterations,
        "materialization_seconds": materialize_seconds,
        "materialized_files": sum(path.is_file() for path in destination.rglob("*")),
    }


def run_off(args: argparse.Namespace, root: Path) -> dict[str, Any]:
    setup, mutate = workload_commands(args, root)
    setup_value, setup_seconds = run_json(setup)
    mutation_value, workload_seconds = run_json(mutate)
    return {
        "baseline_setup_seconds": setup_seconds,
        "baseline": require_object(setup_value, "baseline setup output"),
        "workload_seconds": workload_seconds,
        "workload": require_object(mutation_value, "workload output"),
    }


def run_on(args: argparse.Namespace, binary: Path, work: Path) -> dict[str, Any]:
    root = work / "root"
    shadow = work / "shadow"
    session = work / "session"
    spool = work / "spool.ndjson"
    socket = work / "rrjj.sock"
    work.mkdir(parents=True)
    shadow.mkdir()
    setup, mutate = workload_commands(args, root)
    setup_value, setup_seconds = run_json(setup)
    setup_result = require_object(setup_value, "recording-on setup output")
    log_path = work / "daemon.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [
                str(binary),
                "daemon",
                "--root",
                str(root),
                "--shadow",
                str(shadow),
                "--events",
                str(spool),
                "--session-dir",
                str(session),
                "--socket",
                str(socket),
                "--session-id",
                "paired-benchmark",
                "--quiescence-ms",
                str(args.quiescence_ms),
                "--max-delay-ms",
                str(args.max_delay_ms),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        terminated_by_runner = False
        try:
            recorder_baseline_seconds = wait_for_daemon(binary, socket, process)
            mutation_value, workload_seconds = run_json(mutate)
            mutation_result = require_object(
                mutation_value, "recording-on workload output"
            )
            time.sleep(args.settle_ms / 1000)
            _, flush_seconds = run_json(
                [str(binary), "flush", "--socket", str(socket)]
            )
            manifest = read_json_object(
                session / "manifest.json", "durable session manifest"
            )
            durable_op = manifest.get("durable_op")
            if not isinstance(durable_op, str) or not durable_op:
                raise BenchmarkDataError(
                    "durable session manifest has no non-empty durable_op"
                )
            manifest_session_id = manifest.get("session_id")
            if not isinstance(manifest_session_id, str) or not manifest_session_id:
                raise BenchmarkDataError(
                    "durable session manifest has no non-empty session_id"
                )
            durable_seq = manifest.get("durable_seq")
            if (
                not isinstance(durable_seq, int)
                or isinstance(durable_seq, bool)
                or durable_seq < 0
            ):
                raise BenchmarkDataError(
                    "durable session manifest has no unsigned durable_seq"
                )
            events_object = manifest.get("events_object")
            if not isinstance(events_object, str) or not events_object:
                raise BenchmarkDataError(
                    "durable session manifest has no non-empty events_object"
                )
            metrics = reader_metrics(binary, session, durable_op, work)
            event_metrics = summarize_events(
                session, manifest_session_id, durable_seq, events_object
            )
        finally:
            if process.poll() is None:
                terminated_by_runner = True
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
    if not terminated_by_runner and process.returncode != 0:
        raise RuntimeError(f"rrjj daemon failed; see {log_path}")
    return {
        "baseline_setup_seconds": setup_seconds,
        "baseline": setup_result,
        "recorder_baseline_seconds": recorder_baseline_seconds,
        "workload_seconds": workload_seconds,
        "workload": mutation_result,
        "flush_time_to_durable_seconds": flush_seconds,
        **event_metrics,
        **metrics,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=REPO / "target/release/rrjj")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--keep", type=Path)
    parser.add_argument("--quiescence-ms", type=int, default=100)
    parser.add_argument("--max-delay-ms", type=int, default=1_000)
    parser.add_argument("--settle-ms", type=int, default=250)
    subparsers = parser.add_subparsers(dest="workload", required=True)
    synthetic = subparsers.add_parser("synthetic")
    synthetic.add_argument("--mutations", type=int, default=10_000)
    synthetic.add_argument("--working-set", type=int, default=1_000)
    synthetic.add_argument("--file-bytes", type=int, default=64)
    synthetic.add_argument("--burst-size", type=int, default=10_000)
    synthetic.add_argument("--burst-pause-ms", type=float, default=0)
    real = subparsers.add_parser("real")
    real.add_argument("--repository", required=True)
    real.add_argument("--revision", required=True)
    real.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.workload == "real" and not args.command:
        parser.error("real workload requires a command after --")
    return args


def main() -> None:
    args = parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(
            f"rrjj binary not found at {binary}; run "
            "'cargo build --release --locked -p rrjj' first"
        )
    temporary = tempfile.TemporaryDirectory(prefix="rrjj-bench-") if not args.keep else None
    work = args.keep.resolve() if args.keep else Path(temporary.name)
    work.mkdir(parents=True, exist_ok=True)
    try:
        off = run_off(args, work / "off")
        on = run_on(args, binary, work / "on")
        overhead = on["workload_seconds"] - off["workload_seconds"]
        result = {
            "schema": 1,
            "workload": args.workload,
            "host": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "python": platform.python_version(),
            },
            "recording_off": off,
            "recording_on": on,
            "incremental_overhead_seconds": overhead,
            "incremental_overhead_percent": (
                overhead / off["workload_seconds"] * 100
                if off["workload_seconds"]
                else None
            ),
            "semantics": {
                "mutation_count": "workload-reported logical filesystem operations",
                "snapshots": "jj checkpoints, not every intermediate byte version",
                "touched_paths": "aggregated watcher audit context, not ground truth",
                "baseline_excluded_from_overhead": True,
            },
        }
        encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
        print(encoded, end="")
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(encoded)
    finally:
        if temporary:
            temporary.cleanup()


if __name__ == "__main__":
    main()
