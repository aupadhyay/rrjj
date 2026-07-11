#!/usr/bin/env sh
set -eu

TARGET="${TARGET:-x86_64-unknown-linux-musl}"
if command -v rustup >/dev/null 2>&1; then
  rustup target add "$TARGET"
else
  TARGET_LIBDIR="$(rustc --print target-libdir --target "$TARGET")"
  if ! ls "$TARGET_LIBDIR"/libcore-*.rlib >/dev/null 2>&1; then
    echo "missing Rust target $TARGET and rustup is unavailable" >&2
    exit 1
  fi
fi
cargo build --release --locked --target "$TARGET" -p rrjj
