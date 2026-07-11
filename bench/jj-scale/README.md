# jj-lib scale harness

Phase 0 answers one question: can `jj-lib` 0.43.0 snapshot a large external
filesystem tree quickly and correctly while all jj metadata remains private?

The harness uses `LocalWorkingCopy` directly. The watched root and the jj
repository/working-copy state are separate sibling directories. It never calls
checkout, never creates `.jj` in the watched root, and never shells out to the
`jj` CLI. The store uses jj-lib's internal Git backend, matching a normal
non-colocated jj repository while keeping its bare Git data under the shadow
root.

## What it measures

Each JSON line on stdout is machine-readable:

- `cold_baseline`: first snapshot of the generated tree.
- `no_op`: a second snapshot with no filesystem changes; its tree diff must be
  empty.
- `incremental`: snapshot after independently configurable add, modify, remove,
  and move-like operations. A move-like operation is a filesystem rename and
  is expected to produce one removal plus one addition in the raw tree diff;
  rename inference is outside this harness.
- `diff_ms` and `diff_entries`: complete jj tree diff traversal.
- `store_bytes` and `store_growth_bytes`: recursive private-state size.
- `peak_rss_bytes`: process high-water RSS where `getrusage` is available.
- `verification`: byte-for-byte comparison of every final jj file object with
  the watched filesystem, plus an exact file-count check.

Fixture generation, churn application, and repository initialization are
outside the snapshot timers. Locking, snapshotting, fresh-state setup in
full-rescan mode, and persistence of working-copy state are inside them.

`--full-rescan` gives every measured snapshot a fresh private working-copy
state directory against the same object store. This bypasses jj's stat cache
without touching the watched tree and models overflow recovery.

## Commands

Run from the repository root:

```bash
cargo fmt --manifest-path bench/jj-scale/Cargo.toml -- --check
cargo build --release --manifest-path bench/jj-scale/Cargo.toml
cargo test --manifest-path bench/jj-scale/Cargo.toml
```

Small smoke:

```bash
cargo run --release --manifest-path bench/jj-scale/Cargo.toml -- \
  --files 1000 --add 25 --modify 25 --remove 25 --move-like 25
```

100k gate:

```bash
cargo run --release --manifest-path bench/jj-scale/Cargo.toml -- \
  --files 100000 --file-bytes 64 \
  --add 1000 --modify 1000 --remove 1000 --move-like 1000
```

1M gate:

```bash
cargo run --release --manifest-path bench/jj-scale/Cargo.toml -- \
  --files 1000000 --file-bytes 64 \
  --add 10000 --modify 10000 --remove 10000 --move-like 10000
```

Full-rescan recovery gate (run at 100k first, then 1M if healthy):

```bash
cargo run --release --manifest-path bench/jj-scale/Cargo.toml -- \
  --files 100000 --file-bytes 64 \
  --add 1000 --modify 1000 --remove 1000 --move-like 1000 \
  --full-rescan
```

To retain and inspect data, provide two existing empty, non-nested directories:

```bash
cargo run --release --manifest-path bench/jj-scale/Cargo.toml -- \
  --watched-root /tmp/rrjj-watch --shadow-root /tmp/rrjj-shadow \
  --files 100000
```

Without those options, independent temporary roots are deleted on exit.

## Historical feasibility criteria

Correctness is a hard gate at every size:

1. The command exits zero and prints the final `PASS`.
2. `no_op.diff_entries` is `0`.
3. Incremental diff entries equal
   `add + modify + remove + 2 * move_like`.
4. Final verification succeeds for every file and byte.
5. No `.jj` appears in the watched root; all growth is under the shadow root.
6. No-op store growth is zero or explained by bounded working-copy metadata,
   and incremental store growth tracks churn rather than total file count.

The provisional feasibility budget is:

- 100k: each snapshot phase under 10 seconds and peak RSS under 2 GiB.
- 1M: each snapshot phase under 60 seconds and peak RSS under 8 GiB.
- Full-rescan: completes within the same limits and preserves correctness.

These were the initial design-study ceilings, not current product requirements
or performance claims. Record the emitted JSON, hardware, filesystem, and build
profile before comparing results. rrjj subsequently separated one-time baseline
cost, incremental workload overhead, and durable flush cost; see
`../README.md` for the current benchmark methodology.

## Recorded design-study result

Run on 2026-07-10 with:

- macOS 26.4 (`Darwin 25.4.0`), arm64
- Apple model `Mac17,6`, 18 logical CPUs, 64 GiB RAM
- Rust 1.96.0, release profile
- jj-lib 0.43.0 with the internal Git backend

The 100k gate failed its timing ceiling:

| Phase | Time | Diff entries | Peak RSS |
| --- | ---: | ---: | ---: |
| cold baseline | 20.487 s | 100,000 | 193 MiB |
| no-op | 0.381 s | 0 | 226 MiB |
| incremental | 0.468 s | 5,000 | 242 MiB |
| final byte verification | 20.043 s | 5,000 | — |

Correctness passed: the final tree matched all 100,000 filesystem files
byte-for-byte, incremental diff cardinality was exact, no-op store growth was
zero, and no jj metadata appeared in the watched root. The cold snapshot
exceeded the provisional 10-second 100k ceiling by 2.05x. That result led to
dropping cold initialization from the incremental-overhead claim rather than
abandoning jj-lib. This harness did not run its 1M baseline case; the separate
paired runner later measured a one-million-mutation workload and retains that
result under `../results/`.
