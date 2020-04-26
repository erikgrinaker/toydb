#!/usr/bin/env bash

set -euo pipefail

cargo build --bin toydb

for ID in a b c d e; do
    (cargo run -q -- -c toydb-$ID/toydb.yaml 2>&1 | sed -e "s/\\(.*\\)/toydb-$ID \\1/g") &
done

trap 'kill $(jobs -p)' EXIT
wait < <(jobs -p)