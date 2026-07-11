# NDJSON tail example

This zero-infrastructure example records a directory into a private jj store
and tails the schema stream.

```bash
mkdir -p /tmp/rrjj-watch /tmp/rrjj-shadow
cargo run -p rrjj -- daemon \
  --root /tmp/rrjj-watch \
  --shadow /tmp/rrjj-shadow \
  --events /tmp/rrjj-events.ndjson \
  --socket /tmp/rrjj.sock
```

In another terminal:

```bash
./examples/ndjson_tail/tail.sh /tmp/rrjj-events.ndjson
echo hello >/tmp/rrjj-watch/hello.txt
cargo run -p rrjj -- snap --socket /tmp/rrjj.sock
cargo run -p rrjj -- mark created-hello \
  --meta '{"tool":"shell"}' --socket /tmp/rrjj.sock
cargo run -p rrjj -- status --socket /tmp/rrjj.sock
```

Stop the daemon with Ctrl-C. The watched directory never receives `.jj`;
repository, operation, commit, tree, blob, and working-copy metadata remain
under the shadow directory.
