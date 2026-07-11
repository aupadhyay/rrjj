#!/bin/sh
set -eu

events="${1:-/tmp/rrjj-events.ndjson}"
touch "$events"
exec tail -n +1 -f "$events"
