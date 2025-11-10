# Mock Unreliable RPC Provider

A test harness for simulating unreliable RPC provider behavior to test transaction queue resilience.

## Quick Start

### Using Docker (Recommended)

```bash
# Start the mock RPC provider
cd tests/rpc_simulator
docker-compose up -d

# Run test scenarios
python test_scenarios.py

# Stop the provider
docker-compose down
```

### Using Python Directly

```bash
# Install dependencies
pip install -r requirements.txt

# Start the mock RPC provider
python mock_rpc.py

# In another terminal, run tests
python test_scenarios.py
```

## Features

### Simulated Failure Modes

1. **False Negative Receipts**: Returns `status=0` even though transaction succeeded
2. **Timeouts**: Randomly times out on requests
3. **Delays**: Adds artificial delay to responses
4. **Dropped Transactions**: Randomly rejects transactions

### Configuration

Control failure rates via `/config` endpoint:

```python
{
    'false_negative_rate': 0.3,  # 30% chance of false negative
    'timeout_rate': 0.1,         # 10% chance of timeout
    'delay_rate': 0.2,           # 20% chance of delay
    'delay_seconds': 5.0,        # Delay duration
    'drop_rate': 0.05,          # 5% chance of dropping tx
}
```

### Endpoints

- `POST /` - JSON-RPC endpoint (eth_sendRawTransaction, eth_getTransactionReceipt, etc.)
- `GET /config` - Get current configuration
- `POST /config` - Update configuration
- `GET /stats` - Get statistics
- `POST /reset` - Reset state

## Test Scenarios

Run `python test_scenarios.py` to execute:

1. **Normal Behavior**: Baseline test with no failures
2. **False Negative Receipts**: 30% false negative rate
3. **RPC Timeouts**: 20% timeout rate
4. **Dropped Transactions**: 10% drop rate
5. **Mixed Failures**: Combination of all failure modes

## Usage Example

```python
import httpx

# Update configuration
async with httpx.AsyncClient() as client:
    # Set 30% false negative rate
    await client.post('http://localhost:8545/config', json={
        'false_negative_rate': 0.3
    })
    
    # Make RPC call
    response = await client.post('http://localhost:8545', json={
        'jsonrpc': '2.0',
        'method': 'eth_sendRawTransaction',
        'params': ['0x...'],
        'id': 1
    })
    
    print(response.json())
```

## Integration with relayer-py

To test transaction queue with mock RPC:

1. Update `settings.json` to point to mock RPC:
```json
{
  "anchor_chain": {
    "rpc": {
      "full_nodes": [{
        "url": "http://localhost:8545"
      }]
    }
  }
}
```

2. Configure failure rates:
```bash
curl -X POST http://localhost:8545/config \
  -H "Content-Type: application/json" \
  -d '{"false_negative_rate": 0.3}'
```

3. Run relayer-py and trigger transactions

4. Monitor logs for false negative detection

## What It Tests

- Receipt verification logic
- Nonce management with unreliable RPC
- Retry behavior
- Error handling
- Transaction queue resilience

## Example Output

```
[Mock RPC] Transaction submitted: 0xabc123...
[Mock RPC] Mined transaction: 0xabc123... with status: 1
[Mock RPC] RETURNING FALSE NEGATIVE RECEIPT for 0xabc123...
[Relayer] Initial receipt showed failure for 0xabc123, verifying...
[Relayer] Verification succeeded: transaction actually succeeded!
```

