#!/bin/bash
set -e

rm -rf data/system

# Kill any existing bolt4rs-server process
echo "Cleaning up any existing bolt4rs-server process..."
pkill -f bolt4rs-server || true
sleep 1

export RUST_LOG=info

# Start the server in background
echo "Starting bolt4rs-server..."
cargo run --bin bolt4rs-server &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for server to start..."
sleep 2

# Check if server is still running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "Server failed to start!"
    exit 1
fi

# Run the test client
echo "Running test client..."
if cargo run --bin test_client; then
    echo "Test succeeded!"
    EXIT_CODE=0
else
    echo "Test failed!"
    EXIT_CODE=1
fi

# Cleanup
echo "Shutting down server..."
cargo run --bin test_client -- --shutdown || true

exit $EXIT_CODE
