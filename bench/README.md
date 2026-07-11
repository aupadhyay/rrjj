# rrjj paired benchmarks

The benchmark compares the same deterministic workload with recording off and
on. It reports workload wall time and their delta. Baseline fixture creation
and rrjj's initial baseline checkpoint are timed separately and are excluded
from incremental overhead.

`latency_runner.py` is a separate end-to-end benchmark. It starts a fresh local
daemon, durable NDJSON/session sink, and loopback SSE endpoint, then measures
unique-path edits through native watcher observation, touched-path creation and
SSE receipt, and matching snapshot SSE receipt.

## Event-latency reproduction

The quick local method smoke uses shortened daemon timing:

```sh
cargo build --release --locked -p rrjj
python3 bench/latency_runner.py \
  --output bench/results/local-latency-smoke.json \
  --iterations 2 \
  --quiescence-ms 50 \
  --max-delay-ms 250 \
  --timeout 15 \
  --burst-size 4 \
  --continuous-duration 0.6 \
  --continuous-interval-ms 25
python3 bench/validate_latency_result.py \
  bench/results/local-latency-smoke.json
```

Measure out-of-the-box product timing with the runner defaults:

```sh
python3 bench/latency_runner.py \
  --output bench/results/local-latency-product-defaults.json
```

Those defaults are three isolated edits, three eight-edit bursts, and 11
seconds of writes at 100 ms intervals, with the product's 1500 ms quiescence
and 10,000 ms maximum delay. Continuous duration must exceed maximum delay so
the workload actually crosses the max-delay boundary. Each burst or continuous
window may legitimately share one touched-path event and one complete
checkpoint across multiple samples.

The raw sample distinguishes:

- client edit start and completion, measured with both process-local monotonic
  and same-host wall clocks;
- native watcher first observation from
  `touched_paths.data.window_started_at`;
- touched-path event `ts` and SSE receipt;
- matching snapshot `ts` and SSE receipt.

Client-side elapsed values use `perf_counter` only. Daemon internal intervals
subtract daemon RFC3339 timestamps only. Daemon/client delivery and watcher
values are explicitly named wall-clock estimates; they can be affected by wall
clock adjustment and rrjj's millisecond timestamp precision, and can therefore
be slightly negative. Do not subtract raw monotonic and wall values. Snapshot
completion means receipt of the matching complete jj checkpoint over SSE; it
does not include `flush` or durable session publication.

`window_started_at` belongs to an aggregated watcher window, not independently
to every path in it. Every sample retains that raw timestamp, but the
`watcher_detection_wall_estimate` summary includes only the earliest edit in
each touched-path window; `watcher_detection_window_representative` identifies
those samples. Later burst/continuous edits must not be interpreted as having
been detected before they started.

SSE is live-only. The runner accepts any first sequence after subscription,
then treats a sequence gap, schema/session change, SSE overflow, disconnect,
daemon exit, or timeout as an explicit failure. A unique sample path selects
its aggregated touched-path window; the first subsequent snapshot is that
window's complete checkpoint. Its diff need not repeat an audit path that a
concurrent capture had already incorporated.

For a retained latency result, use:

> On `<platform>`, with `<quiescence>` ms quiescence and `<max-delay>` ms maximum
> delay, `<mode>` edit-start-to-matching-snapshot SSE receipt was `<p50>` ms
> p50, `<p95>` ms p95, and `<p99>` ms p99 across `<N>` successful samples.
> Watcher and SSE wall-clock components are same-host estimates at millisecond
> daemon timestamp precision; this is one local run, not a cross-machine
> guarantee.

Do not describe touched-path creation as snapshot completion, do not combine
wall and monotonic timestamps into one elapsed interval, and do not claim an
SSE delivery service-level objective from a smoke run.

## Recorded local latency smoke

On 2026-07-10, the shortened command above completed on macOS 26.4 arm64 with
Python 3.11.15 and no failures. Edit-start-to-matching-snapshot SSE receipt was
114.245 ms p50 / 117.939 ms p95 / 118.267 ms p99 for two isolated edits,
120.071 / 122.044 / 122.114 ms for eight burst samples, and 179.632 / 313.814 /
322.183 ms for 24 continuous-write samples. The continuous run crossed its
250 ms maximum delay.

These are interpolated percentiles over very small, correlated sample sets
with benchmark-shortened 50 ms quiescence and 250 ms maximum delay. They are
method-validation measurements, not product-default latency or an SLO.
`bench/results/local-latency-smoke.json` contains raw clocks, event timestamps,
all stage summaries, matching metadata, configuration, and non-identifying
runtime details. The runner records only the binary basename and omits the
machine hostname so retained results do not reveal checkout paths or hostnames.

## Recorded product-default latency

On 2026-07-10, a higher-sample run used 1500 ms quiescence, a 10,000 ms maximum
delay, 20 isolated edits, 20 eight-path bursts, and 11 seconds of continuous
writes at 100 ms intervals:

- Isolated watcher detection estimate: 2.09 ms p50 / 10.23 ms p95.
- Isolated edit-start-to-touched SSE: 1522.66 ms p50 / 1526.71 ms p95.
- Isolated edit-start-to-snapshot SSE: 1569.52 ms p50 / 1581.57 ms p95.
- Burst edit-start-to-snapshot SSE: 1569.66 ms p50 / 1584.68 ms p95 across
  160 path samples.
- Continuous edit-start-to-snapshot SSE: 4640.24 ms p50 / 9545.53 ms p95
  across 110 path samples. Position within the shared 10-second checkpoint
  window determines each edit's latency.

All 290 samples matched a touched-path event and subsequent complete snapshot;
there were no failures or SSE gaps. The full raw result is
`bench/results/local-latency-product-defaults.json`. Watcher detection and
daemon-to-client delivery remain same-host, millisecond-quantized wall-clock
estimates; the end-to-end receipt intervals are monotonic. This single local
run is not an SLO.

## Local reproduction

Requirements: Python 3.10+, Rust 1.96.0, and this checkout.

```sh
cargo build --release --locked -p rrjj
python3 bench/paired_runner.py \
  --output bench/results/local-smoke.json \
  synthetic --mutations 10000 --working-set 1000 --file-bytes 64
```

The synthetic workload performs exactly the requested number of deterministic
logical file writes over a fixed working set. Watcher notifications are
platform-dependent and can be coalesced; `raw_watcher_events` is therefore not
the mutation count.

The paired runner intentionally defaults to `--quiescence-ms 100`,
`--max-delay-ms 1000`, and a benchmark-only `--settle-ms 250` to keep
validation runs short. These are **benchmark timing defaults**, not product
defaults. The rrjj daemon defaults are 1500 ms of quiescence and a 10,000 ms
maximum delay. Omit neither distinction when publishing a result; pass
`--quiescence-ms 1500 --max-delay-ms 10000` when measuring the product's
out-of-the-box timing behavior.

The recording contains complete jj tree checkpoints. It does **not** promise a
checkpoint for every write or retain every intermediate byte version. The
reported snapshot count is the number of retained checkpoints, while touched
paths and operation categories are aggregated audit context from the native
watcher.

The runner also reports:

- setup-off and setup-on time;
- recorder baseline initialization time;
- workload time with recording off and on, plus absolute and percentage delta;
- workload-reported logical mutation count;
- total schema events, touched-path windows, distinct touched paths, watcher
  notifications, observed operation categories, overflow events, and snapshots;
- blocking `flush` time until the local manifest advertises the durable
  sequence and operation;
- cold `rrjj index` process/open/index time;
- warm operation-to-tree lookup in an already-built in-memory index;
- materialization time and resulting file count.

Cold open includes CLI process startup and index construction. Warm lookup is a
Python dictionary lookup after that index has been built; it is not a claim
about cold jj object loading. Materialization is measured independently.

## Real repository workload

Pin both the repository and revision, and provide the mutating/build command
after `--`:

```sh
python3 bench/paired_runner.py real \
  --repository https://github.com/OWNER/REPO.git \
  --revision FULL_COMMIT_SHA \
  -- YOUR_BUILD_COMMAND
```

Each side performs a fresh clone and detached checkout. Clone/checkout time is
reported as baseline setup and excluded from workload overhead. A real command
usually cannot provide an exact logical mutation count, so the result reports
that field as `null`; watcher and checkpoint counts remain available.

## Modal

Modal integration exists only in `bench/run.py`. It copies this checkout into a
Debian image, installs Rust, builds the locked release binary in the image, and
runs the same paired runner. Paired mode remains the default:

```sh
python3 -m pip install modal
modal setup
modal run bench/run.py --mode paired \
  --mutations 10000 --working-set 1000 --file-bytes 64
```

`modal setup` is interactive authentication and is intentionally not hidden by
the script. Image building requires network access to Debian, rustup, and
Cargo registries. The one-million run is explicit/manual:

```sh
modal run bench/run.py --mode paired \
  --mutations 1000000 --working-set 10000 --file-bytes 64
```

No Modal result is claimed until that command has completed and its emitted
JSON has been preserved.

### Real S3 end-to-end smoke

Create a generic Modal secret named `rrjj-s3` from credentials and bucket
settings already present in your shell:

```sh
modal secret create rrjj-s3 \
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  AWS_REGION="$AWS_REGION" \
  RRJJ_S3_BUCKET="$RRJJ_S3_BUCKET"
```

Use credentials restricted to the chosen benchmark bucket and prefixes. The
values belong in Modal's secret store, never in this repository. You may use
`AWS_DEFAULT_REGION` instead of `AWS_REGION`. To select another secret, set
`RRJJ_MODAL_S3_SECRET` while invoking Modal; selecting another bucket means
placing its `RRJJ_S3_BUCKET` and matching AWS settings in that secret. Then run:

```sh
modal run bench/run.py --mode s3-smoke
```

The smoke uses 32 deterministic writes over eight files, 50 ms quiescence, and
a 250 ms maximum delay by default. Override those with `--s3-mutations`,
`--s3-working-set`, `--s3-file-bytes`, `--quiescence-ms`, and
`--max-delay-ms`. `--prefix` defaults to `rrjj/modal`; every run adds a unique
`s3-smoke-<uuid>` session segment. Pass `--prefix YOUR/PREFIX` or set
`RRJJ_MODAL_S3_PREFIX` to change the default.

Inside Modal, the smoke builds a baseline, starts the release rrjj daemon with
the real S3 sink, mutates files, marks and flushes, and stops the daemon cleanly.
It then lists and downloads only that run's exact prefix, checks the manifest
and contiguous durable events, runs `rrjj index` and `rrjj materialize`, and
compares SHA-256 hashes for every restored file. Local temporary files are
removed. The S3 prefix is deliberately retained and the result JSON reports
its exact value without credentials.

After review, remove exactly the prefix printed in the result using an AWS CLI
identity authorized for the development bucket:

```sh
aws s3 rm "s3://BUCKET/EXACT_PREFIX_FROM_RESULT" --recursive
```

Do not delete the shared `rrjj/modal` parent prefix.

### Temporary live scrubber

The Modal live scrubber is a **temporary, public, unauthenticated test
endpoint**. Anyone with its URL can access it. It is not a production
deployment and must not contain sensitive source data. Start an ephemeral
development endpoint with:

```sh
modal serve bench/run.py
```

Modal prints the temporary HTTPS URL. Opening its root serves the scrubber from
the rrjj binary; the component probes same-origin `/health` and automatically
connects to same-origin `/events`. `/health` and every SSE event expose the
unique `session_id`. The corresponding durable S3 prefix is:

```text
rrjj/modal-live/<session_id>
```

Set `RRJJ_MODAL_LIVE_PREFIX` when invoking `modal serve` or `modal deploy` to
choose another live parent prefix. The bucket and secret are selected as
described for the S3 smoke.

The container creates a deterministic baseline, then runs a bounded sequence of
visible creates, modifications, and removals with a durable S3 flush after each
round. The UI and SSE endpoint remain available after that workload finishes
until Modal terminates the container. The endpoint does not render or log AWS
credentials.

Only if a persistent review URL is explicitly wanted, deploy it as a separate
manual action:

```sh
modal deploy bench/run.py
```

Stop the served/deployed Modal app when review is complete. Then remove only
the exact session prefix discovered above, using an AWS CLI identity authorized
for the development bucket:

```sh
aws s3 rm "s3://BUCKET/rrjj/modal-live/SESSION_ID" --recursive
```

Never remove the shared `rrjj/modal-live` parent prefix. `modal serve`
and `modal deploy` are intentionally excluded from automated validation.

## CI smoke

CI builds the release binary, writes an eight-mutation paired result under the
CI runner's temporary directory, and validates its schema, paired mutation
counts, timing fields, durable timeline, and baseline-exclusion semantics with
`bench/validate_result.py`. It also runs one isolated edit, one two-edit burst,
and a short continuous max-delay crossing through the latency runner, then
validates event ordering, matching, mode coverage, and successful monotonic
snapshot completion. It uses no performance threshold, does not overwrite
committed local results, and is method coverage rather than a performance
claim.

## Claim wording

For a retained result, use:

> On `<host>`, `<N>` deterministic logical file writes over `<working-set>`
> files took `<off>` seconds without rrjj and `<on>` seconds with rrjj
> (`<delta>` seconds / `<percent>%` paired wall-time delta). rrjj retained
> `<snapshots>` complete tree checkpoints. Baseline setup, flush durability,
> cold open, warm index lookup, and materialization were measured separately.

Do not say "rrjj recorded every intermediate version." Do not call the
recording-on runtime itself "overhead"; overhead is the paired delta. Negative
deltas on short/noisy runs must be reported as measurement noise, not a speedup.

`bench/results/local-smoke.json` is the modest method-validation result.
`bench/results/local-1m.json` is the retained one-million-mutation local result.
No Modal result is currently claimed.

## Recorded local smoke

On 2026-07-10, the exact local command above ran on macOS 26.4 arm64 with
Python 3.11.15. The 10,000-write workload took 0.426269 seconds off and
0.570686 seconds on: a 0.144417-second (33.88%) paired delta. rrjj observed
20,765 coalescible watcher notifications over 1,000 distinct touched paths and
retained one post-baseline checkpoint. Recorder baseline initialization took
0.904334 seconds; local time-to-durable flush took 11.960052 seconds; cold
open/index took 0.010473 seconds; warm in-memory operation-to-tree lookup
averaged 13.72 ns over 100,000 iterations; and materializing 1,000 files took
0.263519 seconds.

This is a short smoke run, so its percentage is not a stable performance
headline. It validates the paired method and metric plumbing. The full raw
result, including separate fixture setup times, is
`bench/results/local-smoke.json`.

## Recorded local one-million run

On 2026-07-10, this product-default timing run completed on the same macOS 26.4
arm64 host:

```sh
python3 bench/paired_runner.py \
  --output bench/results/local-1m.json \
  --quiescence-ms 1500 \
  --max-delay-ms 10000 \
  --settle-ms 2000 \
  synthetic \
  --mutations 1000000 \
  --working-set 10000 \
  --file-bytes 64 \
  --burst-size 10000
```

The workload took 42.4265 seconds with recording off and 60.9542 seconds with
recording on: an 18.5277-second (43.67%) paired wall-time delta. rrjj retained
seven post-baseline checkpoints, observed 2,053,603 coalescible watcher
notifications across 10,000 distinct paths, and emitted no overflow event.

Recorder baseline initialization took 2.4745 seconds. Local time-to-durable
flush took 36.9834 seconds, cold open/index took 0.0409 seconds, warm in-memory
operation-to-tree lookup averaged 22.10 ns, and materializing 10,000 files took
1.8528 seconds. These phases are deliberately excluded from the incremental
overhead figure and reported separately. This is one local result, not a claim
about Modal or other hardware.
