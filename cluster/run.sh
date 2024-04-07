#!/usr/bin/env bash
#
# This script builds and runs a 5-node toyDB cluster listening on ports
# 9601-9605. Config and data is stored under the toydb* directories.
# To connect a toysql client to node 5 on port 9605, run:
#
# cargo run --release --bin toysql

set -euo pipefail

# Change into the script directory.
cd "$(dirname $0)"

# Build toyDB using release optimizations.
cargo build --release --bin toydb

# Start nodes 1-5 in the background, prefixing their output with the node ID.
echo "Starting 5 nodes on ports 9601-9605. To connect to node 5, run:"
echo "cargo run --release --bin toysql"
echo ""

for ID in 1 2 3 4 5; do
    (cargo run -q --release -- -c toydb$ID/toydb.yaml 2>&1 | sed -e "s/\\(.*\\)/toydb$ID \\1/g") &
done

# Wait for the background processes to exit. Kill all toyDB processes when the
# script exits (e.g. via Ctrl-C).
trap 'kill $(jobs -p)' EXIT
wait < <(jobs -p)