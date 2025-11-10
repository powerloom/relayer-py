"""
Integration test for transaction queue with mock unreliable RPC provider.
Tests the actual transaction queue implementation with various failure scenarios.
"""

import asyncio
import json
import sys
import traceback
from pathlib import Path

# Add parent directory to path to import relayer-py modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.tx_queue import TransactionQueue
from utils.tx_queue_factories import create_transaction_func
from web3 import AsyncWeb3, AsyncHTTPProvider
import httpx


async def simulate_transaction_with_mock_rpc():
    """Simulate a transaction using mock RPC provider."""
    
    print('=' * 60)
    print('Transaction Queue Integration Test')
    print('=' * 60)
    
    # Connect to mock RPC
    w3 = AsyncWeb3(AsyncHTTPProvider('http://localhost:8545'))
    
    # Load ProtocolState contract ABI from protocol-contracts
    # ProtocolState.submitSubmissionBatch routes to DataMarket.submitSubmissionBatch
    project_root = Path(__file__).parent.parent.parent
    protocol_contracts_root = project_root.parent / 'decentralized_sequencer_eigen_workspace' / 'protocol-contracts'
    abi_path = protocol_contracts_root / 'hardhat' / 'artifacts' / 'contracts' / 'ProtocolState.sol' / 'PowerloomProtocolState.json'
    
    with open(abi_path, 'r') as f:
        artifact = json.load(f)
        abi = artifact['abi']
    
    # Create ProtocolState contract instance
    # ProtocolState.submitSubmissionBatch takes dataMarket as first param and routes to it
    protocol_state_address = '0x' + '0' * 40  # Mock ProtocolState address
    protocol_state_contract = w3.eth.contract(address=protocol_state_address, abi=abi)
    
    # Mock data market address (passed as first parameter to submitSubmissionBatch)
    data_market_address = '0x' + '3' * 40
    
    # Create transaction function (relayer-py pattern)
    # ProtocolState.submitSubmissionBatch(dataMarket, batchCid, epochId, projectIds, snapshotCids, finalizedCidsRootHash)
    tx_func = create_transaction_func(
        w3,
        '0x' + '1' * 40,  # Mock signer address
        '0x' + '2' * 64,  # Mock private key
        protocol_state_contract,
        'submitSubmissionBatch',
        1000000000,  # Mock gas price
        0,  # priority_gas_multiplier
        # Parameters for ProtocolState.submitSubmissionBatch (passed as *args)
        data_market_address,  # dataMarket (address) - ProtocolState routes to this
        'bafkreitest',  # batchCID
        12345,  # epochID
        ['project1', 'project2'],  # projectIDs
        ['cid1', 'cid2'],  # snapshotCIDs
        '0x' + '4' * 64,  # finalizedCIDsRootHash
    )
    
    # Initialize transaction queue (fire-and-forget mode - default)
    tx_queue = TransactionQueue(max_size=100, wait_for_receipt=False)
    await tx_queue.start(initial_nonce=100)
    
    # Configure false negatives BEFORE first test
    print('\n1. Configuring false negative receipts (30% rate)...')
    async with httpx.AsyncClient() as client:
        await client.post('http://localhost:8545/config', json={
            'false_negative_rate': 0.3
        })
    
    false_negatives_detected = 0
    successful = 0
    
    print('\n2. Testing normal transaction flow...')
    try:
        # Submit transaction with contract info for gas estimation
        tx_id = await tx_queue.submit_transaction(
            tx_func,
            w3=w3,
            contract=protocol_state_contract,
            function_name='submitSubmissionBatch',
            signer_address='0x' + '1' * 40,
            function_args=(
                data_market_address,  # dataMarket (address)
                'bafkreitest',  # batchCID
                12345,  # epochID
                ['project1', 'project2'],  # projectIDs
                ['cid1', 'cid2'],  # snapshotCIDs
                '0x' + '4' * 64,  # finalizedCIDsRootHash
            ),
        )
        print(f'   Submitted transaction: {tx_id}')
        
        # In fire-and-forget mode, we need to wait for the future to get the result
        # The future is set immediately with tx_hash, then updated with receipt later
        result = await asyncio.wait_for(
            tx_queue._pending_txs[tx_id],
            timeout=10.0
        )
        
        # Wait a bit more for receipt confirmation (happens in background)
        await asyncio.sleep(2)
        
        # Get updated result with receipt (if available)
        status = await tx_queue.get_status(tx_id)
        if status:
            result = status
        
        receipt = result.get('receipt')
        if receipt and receipt['status'] == 0:
            false_negatives_detected += 1
            print(f'   Transaction: Received false negative')
        else:
            successful += 1
            print(f'   Transaction: Success')
        
        print(f'   Result: {result}')
        print('   ✅ Normal flow works')
    except Exception as e:
        print(f'   ❌ Normal flow failed: {e}')
        traceback.print_exc()
    
    # Continue testing with false negatives
    print('\n3. Testing with false negative receipts (30% rate)...')
    
    for i in range(10):
        try:
            tx_id = await tx_queue.submit_transaction(
                tx_func,
                w3=w3,
                contract=protocol_state_contract,
                function_name='submitSubmissionBatch',
                signer_address='0x' + '1' * 40,
                function_args=(
                    data_market_address,  # dataMarket (address)
                    'bafkreitest',  # batchCID
                    12345,  # epochID
                    ['project1', 'project2'],  # projectIDs
                    ['cid1', 'cid2'],  # snapshotCIDs
                    '0x' + '4' * 64,  # finalizedCIDsRootHash
                ),
            )
            result = await asyncio.wait_for(
                tx_queue._pending_txs[tx_id],
                timeout=10.0
            )
            
            # Wait a bit for receipt confirmation (happens in background)
            await asyncio.sleep(2)
            
            # Get updated result with receipt (if available)
            status = await tx_queue.get_status(tx_id)
            if status:
                result = status
            
            receipt = result.get('receipt')
            if receipt and receipt['status'] == 0:
                false_negatives_detected += 1
                print(f'   Transaction {i+1}: Received false negative')
            else:
                successful += 1
                print(f'   Transaction {i+1}: Success')
        except Exception as e:
            print(f'   Transaction {i+1}: Error - {e}')
            traceback.print_exc()
    
    print(f'\n   Results: {false_negatives_detected} false negatives, {successful} successful')
    
    # Check nonce
    # Nonce started at 100, and should equal 100 + number of successful transactions
    print(f'\n   Initial nonce: 100')
    print(f'   Final nonce: {tx_queue._current_nonce}')
    print(f'   Successful transactions: {successful}')
    print(f'   Expected nonce: {100 + successful}')
    
    if tx_queue._current_nonce == 100 + successful:
        print('   ✅ Nonce managed correctly (no false negatives consumed nonce)')
    else:
        print('   ❌ Nonce management issue detected')
    
    # Cleanup
    await tx_queue.stop()
    
    print('\n' + '=' * 60)
    print('Integration test completed!')
    print('=' * 60)


async def main():
    """Main entry point."""
    print('Make sure mock RPC provider is running:')
    print('  cd tests/rpc_simulator && docker-compose up -d')
    print('  or')
    print('  python tests/rpc_simulator/mock_rpc.py')
    print()
    
    input('Press Enter to start integration test...')
    
    await simulate_transaction_with_mock_rpc()


if __name__ == '__main__':
    asyncio.run(main())

