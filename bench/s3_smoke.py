"""Helpers for the Modal-only real S3 end-to-end smoke.

This module deliberately has no Modal or AWS SDK imports so its validation and
artifact handling can be unit tested locally.
"""

from __future__ import annotations

import hashlib
import json
import signal
import subprocess
import tempfile
import time
import uuid
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from bench.paired_runner import BenchmarkDataError, read_json_object, summarize_events
from bench.workloads.synthetic import initialize, mutate

DEFAULT_S3_PREFIX = "rrjj/modal"


class S3Client(Protocol):
    def get_paginator(self, operation_name: str) -> Any: ...

    def download_file(self, bucket: str, key: str, filename: str) -> None: ...


def normalize_prefix(prefix: str) -> str:
    normalized = prefix.strip().strip("/")
    parts = normalized.split("/")
    if not normalized or any(part in ("", ".", "..") for part in parts):
        raise ValueError("S3 prefix must contain only non-empty path segments")
    return normalized


def make_session_id() -> str:
    return f"s3-smoke-{uuid.uuid4()}"


def exact_session_prefix(prefix: str, session_id: str) -> str:
    if not session_id or "/" in session_id or session_id in (".", ".."):
        raise ValueError("session ID must be one non-empty path segment")
    return f"{normalize_prefix(prefix)}/{session_id}"


def hash_tree(root: Path) -> dict[str, str]:
    if not root.is_dir():
        raise BenchmarkDataError(f"missing tree to hash: {root}")
    result: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise BenchmarkDataError(f"tree contains unsupported symlink: {path}")
        if path.is_file():
            relative = path.relative_to(root).as_posix()
            result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def download_session(
    client: S3Client, bucket: str, session_prefix: str, destination: Path
) -> dict[str, int]:
    exact_prefix = normalize_prefix(session_prefix) + "/"
    objects: list[tuple[str, int]] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=exact_prefix):
        for item in page.get("Contents", []):
            key = item.get("Key")
            size = item.get("Size")
            if not isinstance(key, str) or not key.startswith(exact_prefix):
                raise BenchmarkDataError("S3 listing returned a key outside the session prefix")
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                raise BenchmarkDataError(f"S3 object has invalid size: {key}")
            relative = key[len(exact_prefix) :]
            parts = PurePosixPath(relative).parts
            if not relative or relative.endswith("/") or any(
                part in ("", ".", "..") for part in parts
            ) or PurePosixPath(*parts).as_posix() != relative:
                raise BenchmarkDataError(f"unsafe or non-file S3 object key: {key}")
            objects.append((key, size))
    if not objects:
        raise BenchmarkDataError(f"no objects found at s3://{bucket}/{exact_prefix}")

    destination.mkdir(parents=True, exist_ok=False)
    seen: set[str] = set()
    for key, _ in sorted(objects):
        relative = key[len(exact_prefix) :]
        if relative in seen:
            raise BenchmarkDataError(f"duplicate S3 object in listing: {key}")
        seen.add(relative)
        local_path = destination.joinpath(*PurePosixPath(relative).parts)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket, key, str(local_path))
    return {
        "object_count": len(objects),
        "object_bytes": sum(size for _, size in objects),
    }


def validate_downloaded_session(
    binary: Path,
    session: Path,
    expected_session_id: str,
    expected_hashes: dict[str, str],
    work: Path,
    timeout_seconds: float = 120,
) -> dict[str, Any]:
    manifest = read_json_object(session / "manifest.json", "S3 session manifest")
    if manifest.get("session_id") != expected_session_id:
        raise BenchmarkDataError("manifest session ID does not match requested S3 session")
    durable_seq = manifest.get("durable_seq")
    if (
        not isinstance(durable_seq, int)
        or isinstance(durable_seq, bool)
        or durable_seq < 0
    ):
        raise BenchmarkDataError("S3 manifest has no unsigned durable_seq")
    durable_op = manifest.get("durable_op")
    if not isinstance(durable_op, str) or not durable_op:
        raise BenchmarkDataError("S3 manifest has no non-empty durable_op")
    events_object = manifest.get("events_object")
    if not isinstance(events_object, str) or not events_object:
        raise BenchmarkDataError("S3 manifest has no non-empty events_object")
    if not (session / "store").is_dir() or not any(
        path.is_file() for path in (session / "store").rglob("*")
    ):
        raise BenchmarkDataError("downloaded S3 session has no store objects")

    event_metrics = summarize_events(
        session,
        expected_session_id=expected_session_id,
        expected_durable_seq=durable_seq,
        events_object=events_object,
    )
    index_started = time.perf_counter()
    index_result = subprocess.run(
        [str(binary), "index", str(session)],
        check=True,
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
    )
    index_seconds = time.perf_counter() - index_started
    try:
        index = json.loads(index_result.stdout)
    except json.JSONDecodeError as error:
        raise BenchmarkDataError("rrjj index returned malformed JSON") from error
    if not isinstance(index, list) or not any(
        isinstance(entry, dict) and entry.get("op") == durable_op for entry in index
    ):
        raise BenchmarkDataError("durable operation is missing from rrjj index")

    restored = work / "restored"
    materialize_started = time.perf_counter()
    subprocess.run(
        [str(binary), "materialize", str(session), durable_op, str(restored)],
        check=True,
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
    )
    materialize_seconds = time.perf_counter() - materialize_started
    restored_hashes = hash_tree(restored)
    if restored_hashes != expected_hashes:
        missing = sorted(expected_hashes.keys() - restored_hashes.keys())
        extra = sorted(restored_hashes.keys() - expected_hashes.keys())
        changed = sorted(
            path
            for path in expected_hashes.keys() & restored_hashes.keys()
            if expected_hashes[path] != restored_hashes[path]
        )
        raise BenchmarkDataError(
            "materialization mismatch "
            f"(missing={missing[:10]}, extra={extra[:10]}, changed={changed[:10]})"
        )
    return {
        "durable_watermark": {"seq": durable_seq, "op": durable_op},
        "event_count": event_metrics["events"],
        "snapshot_count": event_metrics["snapshot_count"],
        "touched_path_windows": event_metrics["touched_path_windows"],
        "distinct_touched_paths": event_metrics["distinct_touched_paths"],
        "index_seconds": index_seconds,
        "materialize_seconds": materialize_seconds,
        "restored_file_count": len(restored_hashes),
        "verification": {"ok": True, "method": "sha256_tree"},
    }


def _run_control(
    binary: Path, socket: Path, process: subprocess.Popen[str], command: list[str], timeout: float
) -> tuple[dict[str, Any], float]:
    if process.poll() is not None:
        raise RuntimeError(f"rrjj daemon exited with status {process.returncode}")
    started = time.perf_counter()
    result = subprocess.run(
        [str(binary), *command, "--socket", str(socket)],
        check=True,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    elapsed = time.perf_counter() - started
    if process.poll() is not None:
        raise RuntimeError(f"rrjj daemon exited with status {process.returncode}")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"rrjj {command[0]} returned malformed JSON") from error
    if not isinstance(value, dict):
        raise RuntimeError(f"rrjj {command[0]} did not return a JSON object")
    return value, elapsed


def _wait_for_daemon(
    binary: Path,
    socket: Path,
    process: subprocess.Popen[str],
    timeout: float,
) -> float:
    started = time.perf_counter()
    while time.perf_counter() - started < timeout:
        if process.poll() is not None:
            raise RuntimeError(f"rrjj daemon exited with status {process.returncode}")
        try:
            result = subprocess.run(
                [str(binary), "status", "--socket", str(socket)],
                text=True,
                capture_output=True,
                timeout=min(5, timeout),
            )
        except subprocess.TimeoutExpired:
            continue
        if result.returncode == 0:
            return time.perf_counter() - started
        time.sleep(0.02)
    raise TimeoutError(f"rrjj daemon did not become ready within {timeout} seconds")


def _stop_daemon(process: subprocess.Popen[str], timeout: float) -> float:
    started = time.perf_counter()
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
    try:
        returncode = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        process.kill()
        process.wait()
        raise TimeoutError(f"rrjj daemon did not stop within {timeout} seconds") from error
    if returncode != 0:
        raise RuntimeError(f"rrjj daemon exited with status {returncode}")
    return time.perf_counter() - started


def run_s3_smoke(
    client: S3Client,
    *,
    bucket: str,
    region: str,
    prefix: str = DEFAULT_S3_PREFIX,
    mutations: int = 32,
    working_set: int = 8,
    file_bytes: int = 64,
    quiescence_ms: int = 50,
    max_delay_ms: int = 250,
    timeout_seconds: float = 120,
    binary: Path = Path("/src/target/release/rrjj"),
) -> dict[str, Any]:
    if not bucket or not region:
        raise ValueError("bucket and region are required")
    if min(mutations, working_set, file_bytes, quiescence_ms, max_delay_ms) < 1:
        raise ValueError("smoke counts, file size, and watcher timings must be positive")
    if timeout_seconds <= 0:
        raise ValueError("timeout must be positive")

    session_id = make_session_id()
    base_prefix = normalize_prefix(prefix)
    session_prefix = exact_session_prefix(base_prefix, session_id)
    total_started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="rrjj-s3-smoke-") as temporary:
        work = Path(temporary)
        root = work / "root"
        shadow = work / "shadow"
        spool = work / "spool.ndjson"
        socket = work / "rrjj.sock"
        shadow.mkdir()

        setup_started = time.perf_counter()
        initialize(root, working_set, file_bytes)
        setup_seconds = time.perf_counter() - setup_started
        log_path = work / "daemon.log"
        with log_path.open("w") as daemon_log:
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
                    "--socket",
                    str(socket),
                    "--session-id",
                    session_id,
                    "--s3-bucket",
                    bucket,
                    "--s3-prefix",
                    base_prefix,
                    "--s3-region",
                    region,
                    "--quiescence-ms",
                    str(quiescence_ms),
                    "--max-delay-ms",
                    str(max_delay_ms),
                ],
                stdout=daemon_log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            cleanly_stopped = False
            try:
                ready_seconds = _wait_for_daemon(
                    binary, socket, process, timeout_seconds
                )
                workload_started = time.perf_counter()
                mutate(root, mutations, working_set, file_bytes, mutations, 0)
                workload_seconds = time.perf_counter() - workload_started
                expected_hashes = hash_tree(root)
                _, mark_seconds = _run_control(
                    binary,
                    socket,
                    process,
                    ["mark", "modal_s3_smoke", "--meta", '{"phase":"mutated"}'],
                    timeout_seconds,
                )
                _, flush_seconds = _run_control(
                    binary, socket, process, ["flush"], timeout_seconds
                )
                shutdown_seconds = _stop_daemon(process, timeout_seconds)
                cleanly_stopped = True
            finally:
                if not cleanly_stopped and process.poll() is None:
                    try:
                        _stop_daemon(process, timeout_seconds)
                    except Exception:
                        process.kill()
                        process.wait()

        download_started = time.perf_counter()
        session = work / "downloaded-session"
        object_metrics = download_session(client, bucket, session_prefix, session)
        download_seconds = time.perf_counter() - download_started
        verification = validate_downloaded_session(
            binary,
            session,
            session_id,
            expected_hashes,
            work,
            timeout_seconds,
        )
        return {
            "mode": "s3_smoke",
            "bucket": bucket,
            "prefix": session_prefix,
            "session_id": session_id,
            **object_metrics,
            "timings": {
                "baseline_setup_seconds": setup_seconds,
                "daemon_ready_seconds": ready_seconds,
                "workload_seconds": workload_seconds,
                "mark_seconds": mark_seconds,
                "explicit_flush_seconds": flush_seconds,
                "clean_shutdown_seconds": shutdown_seconds,
                "download_seconds": download_seconds,
                "index_seconds": verification.pop("index_seconds"),
                "materialize_seconds": verification.pop("materialize_seconds"),
                "total_seconds": time.perf_counter() - total_started,
            },
            **verification,
        }
