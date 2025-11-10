#!/bin/bash

# Run Mock RPC Provider Tests

set -e

echo "============================================"
echo "Mock RPC Provider Test Runner"
echo "============================================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Starting with Python directly..."
    python mock_rpc.py &
    MOCK_RPC_PID=$!
    sleep 2
else
    echo "Starting Mock RPC Provider with Docker..."
    docker-compose up -d
    sleep 3
fi

# Check if mock RPC is accessible
if ! curl -s http://localhost:8545/stats > /dev/null; then
    echo "Error: Mock RPC provider not accessible at http://localhost:8545"
    exit 1
fi

echo "Mock RPC provider is running!"
echo ""

# Run test scenarios
echo "Running test scenarios..."
python test_scenarios.py

# Cleanup
if [ -n "$MOCK_RPC_PID" ]; then
    echo "Stopping Mock RPC provider..."
    kill $MOCK_RPC_PID
else
    echo "Stopping Docker containers..."
    docker-compose down
fi

echo ""
echo "Tests completed!"

