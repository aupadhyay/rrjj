# Contributing

Contributions are welcome. Please open an issue before making a substantial
behavioral or schema change.

## Development

rrjj uses the Rust toolchain pinned in `rust-toolchain.toml`.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
python3 -m unittest discover -s bench -p "test_*.py"
node --test ui/scrubber/timeline-model.test.mjs
```

The million-mutation, real-S3, and Modal benchmarks are intentionally manual.
Do not present a single-machine benchmark as a general performance claim.

## Compatibility

Changes to `schema/v0.md`, durable session layout, CLI output, or recovery
semantics require corresponding tests and documentation. Unknown schema fields
must remain safe for older readers to ignore within the same schema version.

By submitting a contribution, you agree that it is licensed under the Apache
License 2.0.
