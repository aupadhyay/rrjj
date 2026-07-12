#!/usr/bin/env python3
"""Measure end-to-end latency through rrjj's native watcher and SSE pipeline."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import platform
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, BinaryIO

REPO = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 1


class LatencyError(RuntimeError):
    """The benchmark cannot produce a valid latency sample."""


class SequenceError(LatencyError):
    """The live SSE stream is discontinuous or incompatible."""


def parse_rfc3339_ns(value: str) -> int:
    """Convert an RFC3339 timestamp to Unix nanoseconds without float rounding."""
    if not isinstance(value, str):
        raise ValueError("RFC3339 timestamp must be a string")
    text = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as error:
        raise ValueError(f"invalid RFC3339 timestamp: {value!r}") from error
    if parsed.tzinfo is None:
        raise ValueError("RFC3339 timestamp must include an offset")
    utc = parsed.astimezone(dt.timezone.utc)
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    seconds = (utc.replace(microsecond=0) - epoch).days * 86_400
    seconds += (utc.replace(microsecond=0) - epoch).seconds
    fraction = text.split("T", 1)[-1]
    fraction = fraction.split("+", 1)[0].split("-", 1)[0]
    digits = fraction.split(".", 1)[1] if "." in fraction else ""
    digits = "".join(character for character in digits if character.isdigit())
    nanoseconds = int((digits + "000000000")[:9]) if digits else 0
    return seconds * 1_000_000_000 + nanoseconds


def parse_sse(stream: BinaryIO):
    """Yield SSE messages from a binary line stream."""
    fields: dict[str, Any] = {"data": []}
    while True:
        raw = stream.readline()
        if raw == b"":
            if fields["data"]:
                yield _finish_sse(fields)
            return
        line = raw.decode("utf-8").rstrip("\r\n")
        if not line:
            if fields["data"]:
                yield _finish_sse(fields)
            fields = {"data": []}
            continue
        if line.startswith(":"):
            continue
        name, separator, value = line.partition(":")
        if separator and value.startswith(" "):
            value = value[1:]
        if name == "data":
            fields["data"].append(value)
        elif name in {"event", "id", "retry"}:
            fields[name] = value


def _finish_sse(fields: dict[str, Any]) -> dict[str, Any]:
    return {
        "event": fields.get("event", "message"),
        "id": fields.get("id"),
        "data": "\n".join(fields["data"]),
    }


def percentile(values: list[float], percentage: float) -> float | None:
    """Return a linearly interpolated percentile."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percentage / 100
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def statistics(values: list[float]) -> dict[str, Any]:
    return {
        "count": len(values),
        "min": min(values) if values else None,
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": max(values) if values else None,
    }


def binary_display_value(binary: Path) -> str:
    """Return a stable, non-identifying binary value for retained results."""
    return binary.name


def host_metadata() -> dict[str, Any]:
    """Describe the runtime without recording a machine hostname."""
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "clock_info": {
            "monotonic": vars(time.get_clock_info("perf_counter")),
            "wall": vars(time.get_clock_info("time")),
        },
    }


def check_sequence(
    event: dict[str, Any],
    state: dict[str, Any],
) -> None:
    sequence = event.get("seq")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        raise SequenceError(f"invalid SSE sequence: {sequence!r}")
    if event.get("v") != 0:
        raise SequenceError(f"unsupported rrjj event schema: {event.get('v')!r}")
    session = event.get("session_id")
    if not isinstance(session, str) or not session:
        raise SequenceError("SSE event has no session_id")
    if state.get("session") not in (None, session):
        raise SequenceError("SSE session changed")
    expected = state.get("next_seq")
    if expected is not None and sequence != expected:
        raise SequenceError(f"SSE sequence gap: expected {expected}, received {sequence}")
    state["session"] = session
    state["next_seq"] = sequence + 1


class SseCollector:
    def __init__(self, url: str):
        self.url = url
        self.opened = threading.Event()
        self.stopped = threading.Event()
        self.condition = threading.Condition()
        self.records: list[dict[str, Any]] = []
        self.failure: str | None = None
        self._response: Any = None
        self._thread = threading.Thread(target=self._run, name="rrjj-sse", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        state: dict[str, Any] = {}
        try:
            request = urllib.request.Request(
                self.url, headers={"Accept": "text/event-stream"}
            )
            self._response = urllib.request.urlopen(request, timeout=30)
            self.opened.set()
            for message in parse_sse(self._response):
                received_monotonic_ns = time.perf_counter_ns()
                received_wall_ns = time.time_ns()
                if message["event"] == "overflow":
                    raise SequenceError(f"SSE overflow: {message['data']}")
                if message["event"] != "event":
                    continue
                try:
                    event = json.loads(message["data"])
                except json.JSONDecodeError as error:
                    raise SequenceError(f"invalid SSE event JSON: {error}") from error
                if not isinstance(event, dict):
                    raise SequenceError("SSE event data is not an object")
                check_sequence(event, state)
                with self.condition:
                    self.records.append(
                        {
                            "event": event,
                            "received_monotonic_ns": received_monotonic_ns,
                            "received_wall_ns": received_wall_ns,
                        }
                    )
                    self.condition.notify_all()
        except Exception as error:
            if not self.stopped.is_set():
                self.failure = f"{type(error).__name__}: {error}"
                self.opened.set()
                with self.condition:
                    self.condition.notify_all()

    def close(self) -> None:
        self.stopped.set()
        if self._response is not None:
            self._response.close()
        self._thread.join(timeout=2)


def event_has_path(event: dict[str, Any], event_type: str, path: str) -> bool:
    if event.get("type") != event_type:
        return False
    data = event.get("data")
    if not isinstance(data, dict):
        return False
    key = "paths" if event_type == "touched_paths" else "changes"
    entries = data.get(key)
    return isinstance(entries, list) and any(
        isinstance(entry, dict) and entry.get("path") == path for entry in entries
    )


def find_match(
    records: list[dict[str, Any]], path: str
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    touched = next(
        (
            record
            for record in records
            if event_has_path(record["event"], "touched_paths", path)
        ),
        None,
    )
    if touched is None:
        return None
    touched_seq = touched["event"]["seq"]
    snapshot = next(
        (
            record
            for record in records
            if record["event"]["seq"] > touched_seq
            and record["event"].get("type") == "snapshot"
        ),
        None,
    )
    return (touched, snapshot) if snapshot is not None else None


def wait_for_match(
    collector: SseCollector,
    process: subprocess.Popen[str],
    path: str,
    timeout_seconds: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    deadline = time.monotonic() + timeout_seconds
    with collector.condition:
        while True:
            matched = find_match(collector.records, path)
            if matched is not None:
                return matched
            if collector.failure:
                raise LatencyError(collector.failure)
            status = process.poll()
            if status is not None:
                raise LatencyError(f"rrjj daemon exited with status {status}")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"timed out waiting for SSE events matching {path}")
            collector.condition.wait(min(remaining, 0.05))


def available_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_ready(
    health_url: str,
    process: subprocess.Popen[str],
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise LatencyError(f"rrjj daemon exited with status {process.returncode}")
        try:
            with urllib.request.urlopen(health_url, timeout=0.5) as response:
                value = json.load(response)
            if isinstance(value, dict):
                return value
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
            last_error = str(error)
        time.sleep(0.02)
    raise TimeoutError(f"daemon readiness timed out: {last_error}")


def write_sample(root: Path, mode: str, identifier: str) -> dict[str, Any]:
    relative = f"{mode}/{identifier}.txt"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    start_monotonic_ns = time.perf_counter_ns()
    start_wall_ns = time.time_ns()
    path.write_text(f"{identifier}\n")
    completion_monotonic_ns = time.perf_counter_ns()
    completion_wall_ns = time.time_ns()
    return {
        "sample_id": identifier,
        "mode": mode,
        "path": relative,
        "edit": {
            "start_monotonic_ns": start_monotonic_ns,
            "completion_monotonic_ns": completion_monotonic_ns,
            "start_wall_unix_ns": start_wall_ns,
            "completion_wall_unix_ns": completion_wall_ns,
            "operation_duration_ms": (completion_monotonic_ns - start_monotonic_ns)
            / 1_000_000,
        },
    }


def complete_sample(
    sample: dict[str, Any],
    touched: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    touched_event = touched["event"]
    snapshot_event = snapshot["event"]
    watcher_wall_ns = parse_rfc3339_ns(
        touched_event["data"]["window_started_at"]
    )
    touched_creation_wall_ns = parse_rfc3339_ns(touched_event["ts"])
    snapshot_creation_wall_ns = parse_rfc3339_ns(snapshot_event["ts"])
    edit = sample["edit"]
    sample["events"] = {
        "watcher_first_observed_rfc3339": touched_event["data"][
            "window_started_at"
        ],
        "touched_paths": {
            "seq": touched_event["seq"],
            "created_rfc3339": touched_event["ts"],
            "sse_receipt_monotonic_ns": touched["received_monotonic_ns"],
            "sse_receipt_wall_unix_ns": touched["received_wall_ns"],
        },
        "snapshot": {
            "seq": snapshot_event["seq"],
            "created_rfc3339": snapshot_event["ts"],
            "sse_receipt_monotonic_ns": snapshot["received_monotonic_ns"],
            "sse_receipt_wall_unix_ns": snapshot["received_wall_ns"],
        },
    }
    sample["latency_ms"] = {
        "watcher_detection_wall_estimate": (
            watcher_wall_ns - edit["start_wall_unix_ns"]
        )
        / 1_000_000,
        "debounce_projector_daemon_wall": (
            touched_creation_wall_ns - watcher_wall_ns
        )
        / 1_000_000,
        "touched_sse_delivery_wall_estimate": (
            touched["received_wall_ns"] - touched_creation_wall_ns
        )
        / 1_000_000,
        "edit_completion_to_touched_receipt_monotonic": (
            touched["received_monotonic_ns"] - edit["completion_monotonic_ns"]
        )
        / 1_000_000,
        "touched_receipt_to_snapshot_receipt_monotonic": (
            snapshot["received_monotonic_ns"] - touched["received_monotonic_ns"]
        )
        / 1_000_000,
        "snapshot_creation_to_receipt_wall_estimate": (
            snapshot["received_wall_ns"] - snapshot_creation_wall_ns
        )
        / 1_000_000,
        "edit_completion_to_snapshot_receipt_monotonic": (
            snapshot["received_monotonic_ns"] - edit["completion_monotonic_ns"]
        )
        / 1_000_000,
        "edit_start_to_snapshot_receipt_monotonic": (
            snapshot["received_monotonic_ns"] - edit["start_monotonic_ns"]
        )
        / 1_000_000,
    }
    sample["status"] = "ok"
    return sample


def mark_failure(sample: dict[str, Any], error: Exception) -> dict[str, Any]:
    sample["status"] = "failed"
    sample["failure"] = {"kind": type(error).__name__, "message": str(error)}
    return sample


def await_samples(
    samples: list[dict[str, Any]],
    collector: SseCollector,
    process: subprocess.Popen[str],
    timeout_seconds: float,
) -> None:
    for sample in samples:
        try:
            touched, snapshot = wait_for_match(
                collector, process, sample["path"], timeout_seconds
            )
            complete_sample(sample, touched, snapshot)
        except Exception as error:
            mark_failure(sample, error)


def summarize(samples: list[dict[str, Any]]) -> dict[str, Any]:
    names = sorted(
        {
            name
            for sample in samples
            if sample.get("status") == "ok"
            for name in sample["latency_ms"]
        }
    )
    return {
        "samples": len(samples),
        "successful": sum(sample.get("status") == "ok" for sample in samples),
        "failed": sum(sample.get("status") != "ok" for sample in samples),
        "latency_ms": {
            name: statistics(
                [
                    sample["latency_ms"][name]
                    for sample in samples
                    if sample.get("status") == "ok"
                    and name in sample["latency_ms"]
                ]
            )
            for name in names
        },
    }


def scope_watcher_estimates(samples: list[dict[str, Any]]) -> None:
    """Keep one watcher-detection estimate per aggregated watcher window."""
    windows: dict[int, list[dict[str, Any]]] = {}
    for sample in samples:
        if sample.get("status") != "ok":
            continue
        sequence = sample["events"]["touched_paths"]["seq"]
        windows.setdefault(sequence, []).append(sample)
    for window_samples in windows.values():
        representative = min(
            window_samples, key=lambda sample: sample["edit"]["start_wall_unix_ns"]
        )
        for sample in window_samples:
            sample["events"]["watcher_detection_window_representative"] = (
                sample is representative
            )
            if sample is not representative:
                sample["latency_ms"].pop("watcher_detection_wall_estimate", None)


def run(args: argparse.Namespace, work: Path) -> dict[str, Any]:
    root = work / "root"
    shadow = work / "shadow"
    session = work / "session"
    spool = work / "events.ndjson"
    control = work / "rrjj.sock"
    log_path = work / "daemon.log"
    root.mkdir(parents=True)
    for mode in ("isolated", "burst", "continuous"):
        (root / mode).mkdir()
    shadow.mkdir()
    port = available_port()
    session_id = f"latency-{uuid.uuid4()}"
    command = [
        str(args.binary),
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
        str(control),
        "--http",
        f"127.0.0.1:{port}",
        "--session-id",
        session_id,
        "--quiescence-ms",
        str(args.quiescence_ms),
        "--max-delay-ms",
        str(args.max_delay_ms),
    ]
    samples: list[dict[str, Any]] = []
    global_failures: list[dict[str, str]] = []
    collector = SseCollector(f"http://127.0.0.1:{port}/events")
    with log_path.open("w") as log:
        process = subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT, text=True
        )
        try:
            readiness = wait_ready(
                f"http://127.0.0.1:{port}/health", process, args.timeout
            )
            collector.start()
            if not collector.opened.wait(args.timeout):
                raise TimeoutError("SSE subscription did not become established")
            if collector.failure:
                raise LatencyError(collector.failure)

            for index in range(args.iterations):
                sample = write_sample(root, "isolated", f"{index:04}-{uuid.uuid4()}")
                samples.append(sample)
                await_samples([sample], collector, process, args.timeout)

            for iteration in range(args.iterations):
                burst = [
                    write_sample(
                        root,
                        "burst",
                        f"{iteration:04}-{index:04}-{uuid.uuid4()}",
                    )
                    for index in range(args.burst_size)
                ]
                samples.extend(burst)
                await_samples(burst, collector, process, args.timeout)

            continuous: list[dict[str, Any]] = []
            continuous_started = time.monotonic()
            index = 0
            while time.monotonic() - continuous_started < args.continuous_duration:
                continuous.append(
                    write_sample(root, "continuous", f"{index:06}-{uuid.uuid4()}")
                )
                index += 1
                due = continuous_started + index * args.continuous_interval_ms / 1000
                time.sleep(max(0, due - time.monotonic()))
            samples.extend(continuous)
            await_samples(continuous, collector, process, args.timeout)
            scope_watcher_estimates(samples)
        except Exception as error:
            global_failures.append(
                {"kind": type(error).__name__, "message": str(error)}
            )
            readiness = None
        finally:
            collector.close()
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            if process.returncode not in (0, -signal.SIGINT):
                global_failures.append(
                    {
                        "kind": "DaemonExit",
                        "message": f"daemon exited with status {process.returncode}; see {log_path}",
                    }
                )
    return {
        "schema": SCHEMA_VERSION,
        "benchmark": "rrjj_event_latency",
        "host": host_metadata(),
        "config": {
            "binary": binary_display_value(args.binary),
            "iterations": args.iterations,
            "quiescence_ms": args.quiescence_ms,
            "max_delay_ms": args.max_delay_ms,
            "timeout_seconds": args.timeout,
            "burst_size": args.burst_size,
            "continuous_duration_seconds": args.continuous_duration,
            "continuous_interval_ms": args.continuous_interval_ms,
            "session_id": session_id,
            "http_address": f"127.0.0.1:{port}",
        },
        "readiness": readiness,
        "units": {
            "latency": "milliseconds",
            "raw_monotonic": "nanoseconds from a process-local unspecified origin",
            "raw_wall": "Unix nanoseconds UTC",
        },
        "semantics": {
            "pipeline": "filesystem edit -> native watcher -> debounce/projector -> durable NDJSON acceptance -> SSE receipt -> matching snapshot SSE receipt",
            "matching": "unique relative path selects its touched_paths window; the first subsequent snapshot is that coordinator window's complete checkpoint (its diff need not repeat every audit path)",
            "client_intervals": "perf_counter monotonic clock; never subtracted directly from wall timestamps",
            "daemon_intervals": "differences between daemon RFC3339 wall timestamps",
            "wall_estimates": "same-host daemon/client wall-clock estimates; susceptible to clock adjustment and timestamp quantization",
            "watcher_detection": "window_started_at is shared by every path in an aggregated window; watcher-detection latency is summarized only for the earliest edit in each window, identified by watcher_detection_window_representative",
            "timestamp_precision": "rrjj emits RFC3339 timestamps rounded/truncated to milliseconds; wall-derived estimates have approximately 1 ms quantization and may be slightly negative",
            "snapshot_completion": "SSE client receipt of the matching complete jj tree checkpoint, not durable session flush",
            "sse_overflow_or_gap": "fatal for the run because live timing cannot be reconstructed reliably",
        },
        "samples": samples,
        "summary": {
            "all": summarize(samples),
            "by_mode": {
                mode: summarize(
                    [sample for sample in samples if sample["mode"] == mode]
                )
                for mode in ("isolated", "burst", "continuous")
            },
        },
        "failures": global_failures
        + [
            sample["failure"]
            | {"sample_id": sample["sample_id"], "path": sample["path"]}
            for sample in samples
            if sample.get("status") == "failed"
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--binary", type=Path, default=REPO / "target/release/rrjj")
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--quiescence-ms", type=int, default=1_500)
    parser.add_argument("--max-delay-ms", type=int, default=10_000)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--burst-size", type=int, default=8)
    parser.add_argument("--continuous-duration", type=float, default=11)
    parser.add_argument("--continuous-interval-ms", type=float, default=100)
    args = parser.parse_args()
    for name in ("iterations", "quiescence_ms", "max_delay_ms", "burst_size"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be greater than zero")
    if args.timeout <= 0 or args.continuous_duration <= 0:
        parser.error("--timeout and --continuous-duration must be greater than zero")
    if args.continuous_interval_ms <= 0:
        parser.error("--continuous-interval-ms must be greater than zero")
    if args.continuous_duration * 1000 <= args.max_delay_ms:
        parser.error("--continuous-duration must cross --max-delay-ms")
    args.binary = args.binary.resolve()
    return args


def main() -> None:
    args = parse_args()
    if not args.binary.is_file():
        raise SystemExit(
            f"rrjj binary not found at {args.binary}; run "
            "'cargo build --release --locked -p rrjj' first"
        )
    with tempfile.TemporaryDirectory(prefix="rrjj-latency-") as temporary:
        result = run(args, Path(temporary))
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    print(encoded, end="")
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    if result["failures"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
