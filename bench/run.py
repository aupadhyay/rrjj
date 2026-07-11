#!/usr/bin/env python3
"""Modal entrypoint for benchmarks, the S3 smoke, and a temporary live demo.

Install and authenticate first:
  python -m pip install modal
  modal setup
Then run:
  modal run bench/run.py --mode paired --mutations 10000 --working-set 1000
  modal run bench/run.py --mode s3-smoke
  modal serve bench/run.py
"""

from __future__ import annotations

import atexit
import json
import os
import signal
import subprocess
import threading
import time
import uuid
from pathlib import Path

try:
    import modal
except ImportError as error:
    raise SystemExit(
        "Modal is optional and only used by bench/run.py. "
        "Install it with 'python -m pip install modal', then run 'modal setup'."
    ) from error

app = modal.App("rrjj-benchmark")
MODAL_S3_SECRET = os.environ.get("RRJJ_MODAL_S3_SECRET", "rrjj-s3")
DEFAULT_S3_PREFIX = os.environ.get("RRJJ_MODAL_S3_PREFIX", "rrjj/modal")
LIVE_PREFIX = os.environ.get("RRJJ_MODAL_LIVE_PREFIX", "rrjj/modal-live")
image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install(
        "build-essential",
        "ca-certificates",
        "clang",
        "cmake",
        "curl",
        "git",
        "libssl-dev",
        "pkg-config",
    )
    .run_commands(
        "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs "
        "| sh -s -- -y --profile minimal --default-toolchain 1.96.0"
    )
    .env({"PATH": "/root/.cargo/bin:/usr/local/bin:/usr/bin:/bin"})
    .add_local_file("Cargo.toml", remote_path="/src/Cargo.toml", copy=True)
    .add_local_file("Cargo.lock", remote_path="/src/Cargo.lock", copy=True)
    .add_local_file(
        "rust-toolchain.toml",
        remote_path="/src/rust-toolchain.toml",
        copy=True,
    )
    .add_local_dir("crates", remote_path="/src/crates", copy=True)
    .add_local_dir(
        "ui/scrubber",
        remote_path="/src/ui/scrubber",
        copy=True,
    )
    .add_local_dir(
        "bench",
        remote_path="/src/bench",
        copy=True,
        ignore=["jj-scale/target/**", "results/**", "__pycache__/**"],
    )
    .workdir("/src")
    .run_commands("cargo build --release --locked -p rrjj")
)
s3_image = image.pip_install("boto3")

RRJJ_BINARY = Path("/src/target/release/rrjj")


@app.function(image=image, timeout=60 * 60)
def paired(mut: int, working_set: int, file_bytes: int) -> dict:
    result = subprocess.run(
        [
            "python",
            "bench/paired_runner.py",
            "synthetic",
            "--mutations",
            str(mut),
            "--working-set",
            str(working_set),
            "--file-bytes",
            str(file_bytes),
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    return json.loads(result.stdout)


@app.function(
    image=s3_image,
    timeout=20 * 60,
    secrets=[modal.Secret.from_name(MODAL_S3_SECRET)],
)
def s3_smoke(
    prefix: str,
    mutations: int,
    working_set: int,
    file_bytes: int,
    quiescence_ms: int,
    max_delay_ms: int,
    timeout_seconds: float,
) -> dict:
    import os

    import boto3
    from botocore.config import Config

    from bench.s3_smoke import run_s3_smoke

    bucket = os.environ.get("RRJJ_S3_BUCKET")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not bucket:
        raise RuntimeError("Modal secret is missing RRJJ_S3_BUCKET")
    if not region:
        raise RuntimeError(
            "Modal secret is missing both AWS_REGION and AWS_DEFAULT_REGION"
        )
    client = boto3.client(
        "s3",
        region_name=region,
        config=Config(
            connect_timeout=min(timeout_seconds, 30),
            read_timeout=min(timeout_seconds, 60),
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )
    return run_s3_smoke(
        client,
        bucket=bucket,
        region=region,
        prefix=prefix,
        mutations=mutations,
        working_set=working_set,
        file_bytes=file_bytes,
        quiescence_ms=quiescence_ms,
        max_delay_ms=max_delay_ms,
        timeout_seconds=timeout_seconds,
    )


def _live_control(
    process: subprocess.Popen[str],
    socket: Path,
    *command: str,
    timeout: float = 120,
) -> dict:
    if process.poll() is not None:
        raise RuntimeError(f"rrjj daemon exited with status {process.returncode}")
    result = subprocess.run(
        [str(RRJJ_BINARY), *command, "--socket", str(socket)],
        check=True,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if process.poll() is not None:
        raise RuntimeError(f"rrjj daemon exited with status {process.returncode}")
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        raise RuntimeError(f"rrjj {command[0]} returned a non-object response")
    return value


def _wait_for_live_daemon(
    process: subprocess.Popen[str], socket: Path, timeout: float
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"rrjj daemon exited with status {process.returncode}")
        try:
            _live_control(process, socket, "status", timeout=5)
            return
        except (OSError, subprocess.SubprocessError):
            time.sleep(0.05)
    raise TimeoutError(f"rrjj daemon did not become ready within {timeout} seconds")


def _terminate_live_daemon(
    process: subprocess.Popen[str], expected_shutdown: threading.Event
) -> None:
    expected_shutdown.set()
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _run_live_workload(
    process: subprocess.Popen[str],
    socket: Path,
    root: Path,
    expected_shutdown: threading.Event,
) -> None:
    try:
        time.sleep(5)
        activity = root / "activity"
        activity.mkdir(exist_ok=True)
        previous_item: Path | None = None
        for step in range(10):
            review = activity / "review.txt"
            review.write_text(f"review revision {step:02d}\n")
            time.sleep(0.8)

            item = activity / f"item-{step:02d}.txt"
            item.write_text(f"deterministic demo item {step:02d}\n")
            time.sleep(0.8)

            (root / "baseline/status.txt").write_text(
                f"demo progress {step + 1}/10\n"
            )
            time.sleep(0.8)

            if previous_item is not None:
                previous_item.unlink()
                time.sleep(0.8)
            previous_item = item

            _live_control(process, socket, "flush")

        (activity / "complete.txt").write_text("bounded demo workload complete\n")
        if previous_item is not None:
            previous_item.unlink()
        _live_control(process, socket, "flush")
    except Exception:
        print("rrjj live demo workload failed; terminating endpoint", flush=True)
        _terminate_live_daemon(process, expected_shutdown)
        os._exit(1)


@app.function(
    image=s3_image,
    timeout=24 * 60 * 60,
    max_containers=1,
    secrets=[modal.Secret.from_name(MODAL_S3_SECRET)],
)
@modal.web_server(8787, startup_timeout=120)
def live_scrubber() -> None:
    """Serve an unauthenticated, temporary rrjj test endpoint."""
    bucket = os.environ.get("RRJJ_S3_BUCKET")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not bucket:
        raise RuntimeError("Modal secret is missing RRJJ_S3_BUCKET")
    if not region:
        raise RuntimeError(
            "Modal secret is missing both AWS_REGION and AWS_DEFAULT_REGION"
        )

    session_id = f"modal-live-{uuid.uuid4()}"
    workspace = Path("/tmp") / session_id
    root = workspace / "root"
    shadow = workspace / "shadow"
    socket = workspace / "rrjj.sock"
    spool = workspace / "spool.ndjson"
    (root / "baseline").mkdir(parents=True)
    shadow.mkdir()
    (root / "README.txt").write_text(
        "rrjj Modal live scrubber\nThis is deterministic test data.\n"
    )
    (root / "baseline/status.txt").write_text("demo baseline ready\n")
    (root / "baseline/config.json").write_text(
        '{"demo":true,"workload":"bounded"}\n'
    )

    daemon_log = (workspace / "daemon.log").open("w")
    process = subprocess.Popen(
        [
            str(RRJJ_BINARY),
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
            LIVE_PREFIX,
            "--s3-region",
            region,
            "--quiescence-ms",
            "150",
            "--max-delay-ms",
            "1000",
            "--http",
            "0.0.0.0:8787",
        ],
        stdout=daemon_log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    expected_shutdown = threading.Event()
    atexit.register(_terminate_live_daemon, process, expected_shutdown)
    try:
        _wait_for_live_daemon(process, socket, 90)
    except Exception:
        _terminate_live_daemon(process, expected_shutdown)
        raise

    def monitor_daemon() -> None:
        returncode = process.wait()
        if not expected_shutdown.is_set():
            print(
                f"rrjj live demo daemon exited unexpectedly ({returncode})",
                flush=True,
            )
            os._exit(1)

    threading.Thread(target=monitor_daemon, daemon=True).start()
    threading.Thread(
        target=_run_live_workload,
        args=(process, socket, root, expected_shutdown),
        daemon=True,
    ).start()
    print(
        f"rrjj unauthenticated test demo ready: "
        f"session_id={session_id} s3_prefix={LIVE_PREFIX}/{session_id}",
        flush=True,
    )


@app.local_entrypoint()
def main(
    mode: str = "paired",
    mutations: int = 10_000,
    working_set: int = 1_000,
    file_bytes: int = 64,
    s3_mutations: int = 32,
    s3_working_set: int = 8,
    s3_file_bytes: int = 64,
    prefix: str = DEFAULT_S3_PREFIX,
    quiescence_ms: int = 50,
    max_delay_ms: int = 250,
    timeout_seconds: float = 120,
) -> None:
    if mode == "paired":
        result = paired.remote(mutations, working_set, file_bytes)
    elif mode == "s3-smoke":
        result = s3_smoke.remote(
            prefix,
            s3_mutations,
            s3_working_set,
            s3_file_bytes,
            quiescence_ms,
            max_delay_ms,
            timeout_seconds,
        )
    else:
        raise ValueError("--mode must be 'paired' or 's3-smoke'")
    print(json.dumps(result, indent=2, sort_keys=True))
