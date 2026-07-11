# rrjj

`rrjj` is a jj-backed flight recorder for one filesystem root. It watches for
activity, periodically checkpoints the complete visible tree, and emits a
versioned NDJSON timeline for observers.

The product guarantee is checkpoint reconstruction: after a successful
`flush`, every operation through the manifest's durable watermark can be
opened, diffed, and materialized from the published event stream and jj store.
Watcher notifications are triggers and audit context, not the source of truth.
Rapid writes may be coalesced into one touched-path window and one checkpoint,
so rrjj does not guarantee a snapshot or retained byte version for every
filesystem mutation.

rrjj 0.1 is an early release. The event schema is versioned, but the CLI,
durable session layout, and jj storage compatibility may change before 1.0.

## Architecture

```text
workload → native watcher → debounce/overflow recovery → jj tree checkpoint
                              │                         │
                              └→ touched-path events    ├→ NDJSON/SSE
                                                        └→ local or S3 store

durable events + store → rrjj reader → index / inspect / diff / materialize
```

The shadow directory contains jj metadata and must be outside the watched
root. The watched tree receives no `.jj` directory. Normal watcher activity
snapshots after 1500 ms of quiet or after a 10-second maximum delay by default.
A watcher rescan/overflow emits an `overflow` event and forces a full-scan
checkpoint, because correctness comes from scanning the tree rather than from
receiving every notification.

Two related but different records are emitted:

- `touched_paths` aggregates paths and coarse native watcher operation
  categories (`create`, `modify`, `remove`, `rename`, `other`) for an activity
  window. Native notifications are folded into a shared accumulator rather
  than queued individually for the checkpoint coordinator, so control requests
  remain responsive during notification storms. Counts are platform-dependent
  and may be coalesced.
- `snapshot` identifies a complete jj tree checkpoint and its diff from the
  previous checkpoint. This is the reconstructable timeline.

Each active audit accumulator and the coordinator's retained audit are capped
at 100,000 distinct paths; notifications arriving during a capture can fill the
next accumulator concurrently. Watcher overflow detail is capped at 64 reports
per window. Reaching either limit emits an explicit rrjj overflow source,
retains the audit entries already accumulated, and forces a full-scan
checkpoint.

Marks are lightweight semantic annotations tied to the current operation.
Flush events and the manifest's `durable_seq`/`durable_op` identify the prefix
that is safe for readers.

## Install

Prebuilt static `x86_64-unknown-linux-musl` archives and checksums are published
on the [GitHub Releases](https://github.com/aupadhyay/rrjj/releases) page.
Source builds are supported on Linux and macOS. Windows is not currently
supported because the control API uses Unix domain sockets.

To build from source, install the Rust toolchain pinned in
`rust-toolchain.toml`, then run:

```sh
cargo build --release --locked -p rrjj
```

## Record locally

```sh
mkdir -p /tmp/rrjj-work
target/release/rrjj daemon \
  --root /tmp/rrjj-work \
  --shadow /tmp/rrjj-shadow \
  --events /tmp/rrjj-spool.ndjson \
  --session-dir /tmp/rrjj-session \
  --socket /tmp/rrjj.sock \
  --http 127.0.0.1:8787
```

Use a fresh/empty shadow directory for each daemon. `--session-dir` publishes a
directory-shaped durable session. Without it or S3 options, only the bounded
NDJSON spool is written. `SIGINT` and `SIGTERM` trigger a final checkpoint,
`session_end`, durable flush, and control-socket cleanup.

Local publication copies only `shadow/repo`, because readers initialize
`session/store/repo` directly and do not use the shadow's `working-copy-*`
state. Loose Git objects, completed Git pack files, jj operation/view objects,
and jj index segments have content-addressed names and are treated as
immutable. A first publication hard-links those files when source and session
are on the same filesystem, with a synced copy fallback; later flushes reuse
the published path. This relies on jj/Git's contract that completed
content-addressed objects are never modified in place. Pointer, head, type,
configuration, and other index files are not covered by that assumption: rrjj
compares and atomically replaces them with separately synced copies, so later
shadow mutations cannot change published bytes through a shared mutable inode.

New or changed file data is synced once, then changed repository directories
are synced before publication. rrjj does not perform a second fsync pass over
every object. Each flush writes a new immutable
`events/<20-digit-durable-seq>.ndjson`; after repository data and the mutable
cursor are durable, `manifest.json` is atomically replaced and its directory is
synced last. Thus a crash or concurrent reader sees either the previous
manifest and its still-present event object, or the new complete publication.
Incremental sync counts (linked, copied, replaced, reused, removed, and copied
bytes) are emitted on stderr for flush diagnosis.

In another terminal:

```sh
target/release/rrjj status --socket /tmp/rrjj.sock
target/release/rrjj mark tool_call:edit --meta '{"step":12}' --socket /tmp/rrjj.sock
target/release/rrjj snap --socket /tmp/rrjj.sock
target/release/rrjj flush --socket /tmp/rrjj.sock
target/release/rrjj pause --socket /tmp/rrjj.sock
target/release/rrjj resume --socket /tmp/rrjj.sock
```

`pause` continues collecting watcher audit context but defers automatic
checkpointing until resume. `--quiescence-ms`, `--max-delay-ms`, and repeated
`--ignore <glob>` options tune capture. `.git`, `.jj`, the shadow, spool,
session, and socket paths are always excluded.

## S3

```sh
target/release/rrjj daemon \
  --root /tmp/rrjj-work \
  --shadow /tmp/rrjj-shadow \
  --events /tmp/rrjj-spool.ndjson \
  --socket /tmp/rrjj.sock \
  --s3-bucket recordings \
  --s3-prefix rrjj \
  --s3-region us-east-1
```

The AWS SDK credential chain is used (environment variables, shared profiles,
or workload identity). Add `--s3-endpoint http://127.0.0.1:9000` for MinIO.
Each event is appended and synced to the bounded local spool before it is
accepted by the coordinator or published to SSE. An ordered background worker
uploads immutable live objects at
`<prefix>/<session-id>/live/<20-digit-seq>.json`, retrying the same key with
backoff after transient failures. A flush waits for live upload through its
target sequence, uploads changed store objects and an immutable versioned
NDJSON object, then publishes `manifest.json` with the NDJSON object pointer and
durability watermark last.

On restart, an existing S3 spool is parsed and checked for contiguous sequence,
session, and schema identity; its next sequence and persisted upload cursor are
restored. An incompatible or partial spool is rejected instead of being
appended to. Capture continues without S3 network availability while the local
spool has room. Exceeding `--max-spool-bytes` is a fatal local capture condition
reported by the daemon, not a successfully buffered event.

If jj commits a checkpoint but local event acceptance fails, the running
coordinator retains and retries that exact snapshot event before taking another
checkpoint. A process or machine crash in the narrow interval after the jj
transaction commits but before the event reaches the synced spool can still
leave an unreferenced operation in the shadow repository; recovery does not yet
reconstruct that pending event across coordinator restarts.

## SSE

With `--http 127.0.0.1:8787`:

- `GET /` serves the embedded scrubber. `rrjj-live.mjs` and
  `timeline-model.mjs` are embedded in the binary and served as JavaScript.
- `GET /events` is browser-compatible SSE. Schema records use the named
  `event` event; broadcast lag uses the named `overflow` event.
- `GET /health` and `GET /manifest/status` return coordinator status.

SSE is a live feed, not a replay API. On overflow or disconnect, reconnect and
use durable NDJSON to resynchronize history. The timeline component checks
sequence continuity and, after detected lag or a gap, requires loading the
durable NDJSON before reconnecting. Loopback HTTP allows any CORS origin for
local development. A non-loopback listener sends no CORS headers unless
`--cors-origin` is provided.

## Read a durable session

```sh
target/release/rrjj index /tmp/rrjj-session
target/release/rrjj inspect /tmp/rrjj-session op:OPERATION_ID
target/release/rrjj inspect /tmp/rrjj-session t:TREE_ID
target/release/rrjj diff /tmp/rrjj-session op:BEFORE op:AFTER
target/release/rrjj materialize /tmp/rrjj-session op:OPERATION_ID /tmp/restored
```

`index` lists timeline sequence, timestamp, kind, operation, and tree pointers.
`inspect` resolves a durable operation or tree. `diff` compares complete trees.
`materialize` requires an empty destination outside the source session.
Readers reject incompatible formats, sequence gaps, and events beyond the
manifest's durable watermark.

## Timeline UI

```sh
target/release/rrjj daemon \
  --root /tmp/rrjj-work \
  --shadow /tmp/rrjj-shadow \
  --events /tmp/rrjj-spool.ndjson \
  --http 127.0.0.1:8787
```

Open <http://127.0.0.1:8787>. The embedded reusable `<rrjj-live>` Web Component
probes same-origin `/health` and automatically connects to `/events` when rrjj
serves it. Its URL remains editable, and it can load a durable NDJSON event
file offline. To develop the static files independently, run
`python3 -m http.server 8000 --directory ui/scrubber`; the missing rrjj health
response leaves the component idle for manual connection. It renders session,
touched-path operation, snapshot, mark, flush, overflow, and end events.
Overflow and reconnect state are visible; use the NDJSON loader to recover
complete history.

## Benchmark status

The paired local runner and controlled synthetic/real-repository workloads are
documented in `bench/README.md`. It reports recording-off/on workload wall time,
their delta, logical mutations where known, watcher/touched-path/event counts,
snapshot count, time-to-durable flush, cold open/index, warm in-memory
operation-to-tree lookup, and materialization separately. Fixture setup and
recorder baseline initialization are excluded from incremental overhead and
reported separately.

The benchmark runner's short-test defaults (100 ms quiescence, 1000 ms maximum
delay, and 250 ms benchmark settle time) differ from the daemon's product
defaults (1500 ms quiescence and 10,000 ms maximum delay). See
`bench/README.md` before interpreting or publishing benchmark timings.

The separate event-latency runner exercises the full local daemon path:
filesystem edit, native watcher, debounce/projector, durable NDJSON acceptance,
SSE touched-path delivery, jj checkpoint, and matching snapshot SSE delivery.
Its defaults use the product's 1500/10,000 ms timings:

```sh
cargo build --release --locked -p rrjj
python3 bench/latency_runner.py \
  --output bench/results/local-latency-product-defaults.json
python3 bench/validate_latency_result.py \
  bench/results/local-latency-product-defaults.json
```

`bench/README.md` documents the exact shortened smoke command, timestamp
semantics, limitations, and honest claim wording. In particular, client
elapsed intervals are monotonic, while daemon/client watcher and delivery
components are same-host wall-clock estimates limited by millisecond daemon
timestamps. A smoke result is method validation, not a latency SLO.

The retained product-default run is
`bench/results/local-latency-product-defaults.json`. Across 20 isolated edits,
watcher detection was 2.09 ms p50 / 10.23 ms p95, touched-path SSE receipt was
1522.66 / 1526.71 ms, and matching snapshot SSE receipt was 1569.52 / 1581.57
ms from edit start. Across 20 eight-path bursts, snapshot receipt was 1569.66 /
1584.68 ms. Continuous writes crossed the 10-second ceiling as designed:
latency depended on each edit's position in the checkpoint window, with
4640.24 ms p50 / 9545.53 ms p95 across 110 edits. This is one local run, not an
SLO.

The shortened CI-style method smoke remains in
`bench/results/local-latency-smoke.json`.

`bench/results/local-1m.json` records a local one-million-mutation run using the
daemon's product timing defaults. One million deterministic writes over 10,000
files took 42.4265 seconds without rrjj and 60.9542 seconds with rrjj: an
18.5277-second (43.67%) paired wall-time delta. rrjj retained seven complete
checkpoints, observed 2,053,603 coalescible native notifications across all
10,000 paths, and reported no watcher overflow. Baseline initialization
(2.4745 seconds), durable local flush (36.9834 seconds), cold open/index
(0.0409 seconds), and materialization (1.8528 seconds) are reported separately.
This is one local run, not a cross-machine or Modal result.

The explicit reproduction commands are:

```sh
cargo build --release --locked -p rrjj
python3 bench/paired_runner.py \
  --output bench/results/local-1m.json \
  --quiescence-ms 1500 --max-delay-ms 10000 --settle-ms 2000 \
  synthetic --mutations 1000000 --working-set 10000
modal run bench/run.py --mode paired \
  --mutations 1000000 --working-set 10000
```

Modal is optional and appears only in `bench/run.py`; see `bench/README.md` for
dependency, authentication, exact reproduction, and honest claim wording.
The real S3 end-to-end smoke uses the generic Modal secret `rrjj-s3`, whose
bucket and AWS settings are supplied by each user:

```sh
modal run bench/run.py --mode s3-smoke
```

It retains a unique prefix below `rrjj/modal` by default for review, verifies the
downloaded durable session with `rrjj index` and `rrjj materialize`, and prints
the exact safe cleanup target. Remove it afterward with
`aws s3 rm "s3://BUCKET/EXACT_PREFIX_FROM_RESULT" --recursive`; never remove
the shared parent prefix. `bench/README.md` documents secret creation, bucket
selection, prefix overrides, and the temporary public live endpoint. That live
endpoint is unauthenticated and must not be used for sensitive data.

## Validation and static Linux build

```sh
cargo fmt --all -- --check
cargo check --workspace --locked
cargo test --workspace --locked
python3 -m unittest discover -s bench -p "test_*.py"
node --test ui/scrubber/timeline-model.test.mjs
./scripts/build-musl.sh
```

The musl script installs no system packages. Install the Rust musl target and a
suitable musl C toolchain first. CI runs formatting, check, tests, script
validation, and a musl release build; million-operation and Modal benchmarks
remain manual.

## Security

Recordings can contain every readable file under the watched root. The embedded
HTTP server does not authenticate clients; bind it to loopback or place it
behind an authenticated proxy. Scope object-store credentials to the smallest
possible recording prefix. On Unix, rrjj restricts shadow/session directories
to mode `0700` and local spools/control sockets to `0600`. See
[SECURITY.md](SECURITY.md) for vulnerability reporting.

## License

Apache License 2.0. See [LICENSE](LICENSE).
