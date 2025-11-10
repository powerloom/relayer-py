# Quick Start: Mock RPC Provider Testing

## Start Mock RPC Provider

```bash
cd tests/rpc_simulator

# Option 1: Using Docker (recommended)
docker-compose up -d

# Option 2: Using Python directly
pip install -r requirements.txt
python mock_rpc.py
```

## Run Tests

```bash
# Run all test scenarios
python test_scenarios.py

# Or use the automated script
./run_tests.sh
```

## Configure Failure Rates

```bash
# Set 30% false negative rate
curl -X POST http://localhost:8545/config \
  -H "Content-Type: application/json" \
  -d '{"false_negative_rate": 0.3}'

# Set 20% timeout rate
curl -X POST http://localhost:8545/config \
  -H "Content-Type: application/json" \
  -d '{"timeout_rate": 0.2}'

# Set mixed failures
curl -X POST http://localhost:8545/config \
  -H "Content-Type: application/json" \
  -d '{
    "false_negative_rate": 0.2,
    "timeout_rate": 0.1,
    "delay_rate": 0.2,
    "drop_rate": 0.05
  }'
```

## Monitor Stats

```bash
# Get current statistics
curl http://localhost:8545/stats

# Reset state
curl -X POST http://localhost:8545/reset
```

## Test Scenarios Included

1. **Normal Behavior** - Baseline with no failures
2. **False Negative Receipts** - Returns wrong status (30% rate)
3. **RPC Timeouts** - Request timeouts (20% rate)
4. **Dropped Transactions** - Rejected transactions (10% rate)
5. **Mixed Failures** - Combination of all failure modes

## Example Output

```
Test Scenario 2: False Negative Receipts (30% rate)
============================================================
Mined transaction: 0xabc123... with status: 1
RETURNING FALSE NEGATIVE RECEIPT for 0xabc123...
Transaction 1: FALSE NEGATIVE (received status=0)
Transaction 2: Success (status=1)
...

Results: 3 false negatives, 7 successful
✅ Test passed: False negatives detected
```

## What Gets Tested

- Receipt verification logic
- Nonce management with unreliable RPC
- Retry behavior
- Error handling
- Transaction queue resilience
- False negative detection

## Stop Mock RPC

```bash
# If using Docker
docker-compose down

# If using Python directly
Ctrl+C
```

