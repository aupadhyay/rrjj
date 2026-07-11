# Changelog

All notable changes to rrjj will be documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
