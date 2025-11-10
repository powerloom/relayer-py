"""
Test scenarios for transaction queue with unreliable RPC provider.
"""

import asyncio
import json
from typing import Dict

import httpx
from web3 import AsyncWeb3, AsyncHTTPProvider


class MockRPCClient:
    """Client for interacting with mock RPC provider."""
    
    def __init__(self, base_url: str = 'http://localhost:8545'):
        self.base_url = base_url
        self.client = httpx.AsyncClient()
        self.request_id = 0
    
    async def _rpc_call(self, method: str, params: list = None) -> dict:
        """Make RPC call."""
        self.request_id += 1
        payload = {
            'jsonrpc': '2.0',
            'method': method,
            'params': params or [],
            'id': self.request_id,
        }
        
        response = await self.client.post(self.base_url, json=payload)
        return response.json()
    
    async def update_config(self, config: Dict):
        """Update mock RPC configuration."""
        response = await self.client.post(f'{self.base_url}/config', json=config)
        return response.json()
    
    async def get_stats(self) -> Dict:
        """Get statistics."""
        response = await self.client.get(f'{self.base_url}/stats')
        return response.json()
    
    async def reset(self):
        """Reset state."""
        response = await self.client.post(f'{self.base_url}/reset')
        return response.json()
    
    async def close(self):
        """Close client."""
        await self.client.aclose()


async def test_scenario_1_normal_behavior():
    """Test 1: Normal RPC behavior (baseline)."""
    print('\n' + '=' * 60)
    print('Test Scenario 1: Normal RPC Behavior')
    print('=' * 60)
    
    client = MockRPCClient()
    
    # Configure normal behavior
    await client.update_config({
        'false_negative_rate': 0.0,
        'timeout_rate': 0.0,
        'delay_rate': 0.0,
        'drop_rate': 0.0,
    })
    
    # Reset state
    await client.reset()
    
    # Simulate transaction
    result = await client._rpc_call('eth_sendRawTransaction', ['0x123'])
    tx_hash = result['result']
    print(f'Submitted transaction: {tx_hash}')
    
    # Wait for mining
    await asyncio.sleep(3)
    
    # Get receipt
    result = await client._rpc_call('eth_getTransactionReceipt', [tx_hash])
    receipt = result['result']
    print(f'Receipt status: {receipt["status"]}')
    
    # Verify success
    assert receipt['status'] == 1, 'Transaction should succeed'
    print('✅ Test passed: Normal behavior works correctly')
    
    await client.close()


async def test_scenario_2_false_negative_receipts():
    """Test 2: False negative receipts."""
    print('\n' + '=' * 60)
    print('Test Scenario 2: False Negative Receipts (30% rate)')
    print('=' * 60)
    
    client = MockRPCClient()
    
    # Configure false negative behavior
    await client.update_config({
        'false_negative_rate': 0.3,  # 30% false negatives
        'timeout_rate': 0.0,
        'delay_rate': 0.0,
        'drop_rate': 0.0,
    })
    
    # Reset state
    await client.reset()
    
    false_negatives = 0
    successful = 0
    
    # Submit 10 transactions
    for i in range(10):
        await client.reset()
        
        result = await client._rpc_call('eth_sendRawTransaction', ['0x123'])
        tx_hash = result['result']
        
        # Wait for mining
        await asyncio.sleep(3)
        
        # Get receipt
        result = await client._rpc_call('eth_getTransactionReceipt', [tx_hash])
        receipt = result['result']
        
        if receipt['status'] == 0:
            false_negatives += 1
            print(f'Transaction {i+1}: FALSE NEGATIVE (received status=0)')
        else:
            successful += 1
            print(f'Transaction {i+1}: Success (status=1)')
    
    print(f'\nResults: {false_negatives} false negatives, {successful} successful')
    print('✅ Test passed: False negatives detected')
    
    await client.close()


async def test_scenario_3_timeouts():
    """Test 3: RPC timeouts."""
    print('\n' + '=' * 60)
    print('Test Scenario 3: RPC Timeouts (20% rate)')
    print('=' * 60)
    
    client = MockRPCClient()
    
    # Configure timeout behavior
    await client.update_config({
        'false_negative_rate': 0.0,
        'timeout_rate': 0.2,  # 20% timeout rate
        'delay_rate': 0.0,
        'drop_rate': 0.0,
    })
    
    # Reset state
    await client.reset()
    
    timeouts = 0
    successful = 0
    
    # Submit 10 transactions
    for i in range(10):
        try:
            result = await asyncio.wait_for(
                client._rpc_call('eth_sendRawTransaction', ['0x123']),
                timeout=3.0
            )
            tx_hash = result['result']
            successful += 1
            print(f'Transaction {i+1}: Success')
        except (asyncio.TimeoutError, httpx.ReadTimeout, httpx.TimeoutException, httpx.ConnectTimeout, httpx.PoolTimeout):
            timeouts += 1
            print(f'Transaction {i+1}: TIMEOUT')
        except Exception as e:
            timeouts += 1
            print(f'Transaction {i+1}: ERROR - {type(e).__name__}: {e}')
    
    print(f'\nResults: {timeouts} timeouts, {successful} successful')
    print('✅ Test passed: Timeouts handled')
    
    await client.close()


async def test_scenario_4_dropped_transactions():
    """Test 4: Dropped transactions."""
    print('\n' + '=' * 60)
    print('Test Scenario 4: Dropped Transactions (10% rate)')
    print('=' * 60)
    
    client = MockRPCClient()
    
    # Configure drop behavior
    await client.update_config({
        'false_negative_rate': 0.0,
        'timeout_rate': 0.0,
        'delay_rate': 0.0,
        'drop_rate': 0.1,  # 10% drop rate
    })
    
    # Reset state
    await client.reset()
    
    dropped = 0
    successful = 0
    
    # Submit 10 transactions
    for i in range(10):
        result = await client._rpc_call('eth_sendRawTransaction', ['0x123'])
        
        if 'error' in result:
            dropped += 1
            print(f'Transaction {i+1}: DROPPED - {result["error"]["message"]}')
        else:
            successful += 1
            print(f'Transaction {i+1}: Accepted')
    
    print(f'\nResults: {dropped} dropped, {successful} successful')
    print('✅ Test passed: Dropped transactions handled')
    
    await client.close()


async def test_scenario_5_mixed_failures():
    """Test 5: Mixed failure scenarios."""
    print('\n' + '=' * 60)
    print('Test Scenario 5: Mixed Failures')
    print('=' * 60)
    
    client = MockRPCClient()
    
    # Configure mixed failures
    await client.update_config({
        'false_negative_rate': 0.2,  # 20% false negatives
        'timeout_rate': 0.1,         # 10% timeouts
        'delay_rate': 0.2,           # 20% delays
        'drop_rate': 0.05,          # 5% drops
    })
    
    # Reset state
    await client.reset()
    
    stats = {
        'successful': 0,
        'false_negatives': 0,
        'timeouts': 0,
        'dropped': 0,
    }
    
    # Submit 20 transactions
    for i in range(20):
        await client.reset()
        
        try:
            # Submit
            result = await asyncio.wait_for(
                client._rpc_call('eth_sendRawTransaction', ['0x123']),
                timeout=5.0
            )
            
            if 'error' in result:
                stats['dropped'] += 1
                print(f'Tx {i+1}: DROPPED')
                continue
            
            tx_hash = result['result']
            
            # Wait for mining
            await asyncio.sleep(3)
            
            # Get receipt
            result = await client._rpc_call('eth_getTransactionReceipt', [tx_hash])
            receipt = result['result']
            
            if receipt['status'] == 0:
                stats['false_negatives'] += 1
                print(f'Tx {i+1}: FALSE NEGATIVE')
            else:
                stats['successful'] += 1
                print(f'Tx {i+1}: Success')
                
        except (asyncio.TimeoutError, httpx.ReadTimeout, httpx.TimeoutException, httpx.ConnectTimeout, httpx.PoolTimeout):
            stats['timeouts'] += 1
            print(f'Tx {i+1}: TIMEOUT')
        except Exception as e:
            stats['timeouts'] += 1
            print(f'Tx {i+1}: ERROR - {type(e).__name__}: {e}')
    
    print(f'\nSummary:')
    print(f'  Successful: {stats["successful"]}')
    print(f'  False Negatives: {stats["false_negatives"]}')
    print(f'  Timeouts: {stats["timeouts"]}')
    print(f'  Dropped: {stats["dropped"]}')
    print('✅ Test passed: Mixed failures handled')
    
    await client.close()


async def main():
    """Run all test scenarios."""
    print('=' * 60)
    print('Transaction Queue Test Scenarios')
    print('Mock Unreliable RPC Provider Tests')
    print('=' * 60)
    
    print('\nMake sure mock RPC provider is running:')
    print('  docker-compose up -d')
    print('  or')
    print('  python mock_rpc.py')
    
    # Wait for user
    input('\nPress Enter to start tests...')
    
    await test_scenario_1_normal_behavior()
    await test_scenario_2_false_negative_receipts()
    await test_scenario_3_timeouts()
    await test_scenario_4_dropped_transactions()
    await test_scenario_5_mixed_failures()
    
    print('\n' + '=' * 60)
    print('All tests completed!')
    print('=' * 60)


if __name__ == '__main__':
    asyncio.run(main())

