# Changelog

All notable changes to rrjj will be documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Composable Git checkpoint and HTTP event sinks coordinated by a durable
  session flush path.
- Recorder-owned Git refs under `refs/rrjj/sessions/<session-id>` with
  compare-and-swap publication and Authorization via
  `RRJJ_GIT_AUTHORIZATION`.
- Authenticated HTTP event batches with contiguous sequence publication,
  acknowledgements, retries, and Authorization via
  `RRJJ_EVENT_HTTP_AUTHORIZATION`.

### Changed

- Durable storage is now modeled as two outputs: checkpoint content and
  timeline events. Local `--session-dir` remains the standalone backend.

### Removed

- S3 and Postgres/Cockroach session backends and their CLI/database schema
  packaging. Pre-1.0 remote users should migrate to Git + HTTP.

## [0.1.0] - 2026-07-11

### Added

- Automatic filesystem watching with bounded debounce and overflow recovery.
- jj-backed checkpoints in a shadow repository outside the watched root.
- Versioned NDJSON events for touched paths, snapshots, marks, flushes, errors,
  overflow, and session lifecycle.
- Durable local-directory and incremental S3 session sinks.
- Session indexing, inspection, diffing, and materialization commands.
- Unix-socket control commands for status, snapshots, marks, flushes, pause,
  resume, and shutdown.
- Live SSE events and an embedded timeline Web Component.
- Synthetic, real-repository, scale, and end-to-end latency benchmarks.

[Unreleased]: https://github.com/aupadhyay/rrjj/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/aupadhyay/rrjj/releases/tag/v0.1.0
