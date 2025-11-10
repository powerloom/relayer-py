#!/bin/bash

# Setup script for Mock RPC Provider tests

set -e

echo "============================================"
echo "Mock RPC Provider Setup"
echo "============================================"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

echo "Python version: $(python3 --version)"

# Install dependencies
echo ""
echo "Installing dependencies..."
pip3 install -r requirements.txt

echo ""
echo "============================================"
echo "Setup complete!"
echo "============================================"
echo ""
echo "To start the mock RPC provider:"
echo "  python3 mock_rpc.py"
echo ""
echo "To run tests:"
echo "  python3 test_scenarios.py"
echo ""
echo "Or use Docker:"
echo "  docker-compose up -d"
echo ""

