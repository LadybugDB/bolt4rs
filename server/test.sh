#!/bin/bash
set -e

# Kill any existing bolt4rs-server process
echo "Cleaning up any existing bolt4rs-server process..."
pkill -f bolt4rs-server || true
sleep 1

# Start the server in background
echo "Starting bolt4rs-server..."
RUST_LOG=debug cargo run --bin bolt4rs-server &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for server to start..."
sleep 2

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
echo "Cleaning up..."
kill $SERVER_PID || true
wait $SERVER_PID 2>/dev/null || true

exit $EXIT_CODE
