#!/usr/bin/env python3
"""
Diagnostic script to trace transaction status for a specific project ID.

This script helps diagnose what happened to transactions related to finalizing
snapshots for a given project ID by:
1. Querying contract events to find relevant transactions
2. Checking transaction receipts and status
3. Querying contract state for lastSequencerFinalizedSnapshot
4. Providing a comprehensive report

Usage:
    python diagnose_project_tx.py

You will be prompted for:
    - Project ID (e.g., 'allTradesSnapshot:0xb5cE2F9B71e785e3eC0C45EDE06Ad95c3bb71a4d:devnet-BDS_DEVNET_ALPHA_UNISWAPV3-ETH')
    - Optional: Block range to search (from_block, to_block)

The script will automatically use:
    - RPC endpoint URL from settings.anchor_chain.rpc.full_nodes[0].url
    - Protocol State contract address from settings.protocol_state_address
    - Data Market contract address (extracted from project ID if in format prefix:address:suffix, otherwise prompted)

If settings are not available, you will be prompted for RPC URL and Protocol State address.
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

from web3 import AsyncWeb3, AsyncHTTPProvider, Web3
from web3.middleware import async_geth_poa_middleware
from web3.exceptions import TransactionNotFound, BlockNotFound
from httpx import Timeout


# Add parent directory to path to import settings
sys.path.insert(0, str(Path(__file__).parent))

try:
    from settings.conf import settings
    SETTINGS_AVAILABLE = True
except ImportError:
    settings = None
    SETTINGS_AVAILABLE = False


async def get_contract_events(
    w3: AsyncWeb3,
    contract_address: str,
    event_name: str,
    abi: List[Dict],
    from_block: int,
    to_block: int,
    data_market_address: Optional[str] = None,
    epoch_id: Optional[int] = None,
) -> List[Dict]:
    """
    Query contract events from blockchain.
    
    Args:
        w3: Web3 instance
        contract_address: Contract address
        event_name: Name of the event to query
        abi: Contract ABI
        from_block: Starting block number
        to_block: Ending block number
        data_market_address: Optional filter for dataMarketAddress
        epoch_id: Optional filter for epochId
        
    Returns:
        List of event logs
    """
    contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=abi)
    
    # Get event ABI
    event_abi = None
    for item in abi:
        if item.get('type') == 'event' and item.get('name') == event_name:
            event_abi = item
            break
    
    if not event_abi:
        return []
    
    # Build event filter
    event_filter = contract.events[event_name].create_filter(
        fromBlock=from_block,
        toBlock=to_block
    )
    
    # Get events
    try:
        events = await event_filter.get_all_entries()
    except Exception as e:
        print(f"Error fetching events: {e}")
        return []
    
    # Filter by data_market_address if provided
    if data_market_address:
        filtered_events = []
        for event in events:
            args = event.get('args', {})
            if args.get('dataMarketAddress', '').lower() == data_market_address.lower():
                if epoch_id is None or args.get('epochId') == epoch_id:
                    filtered_events.append(event)
        return filtered_events
    
    return list(events)


async def get_transaction_receipt(w3: AsyncWeb3, tx_hash: str) -> Optional[Dict]:
    """Get transaction receipt with error handling."""
    try:
        receipt = await w3.eth.get_transaction_receipt(tx_hash)
        return receipt
    except TransactionNotFound:
        return None
    except Exception as e:
        print(f"Error fetching receipt for {tx_hash}: {e}")
        return None


async def get_transaction(w3: AsyncWeb3, tx_hash: str) -> Optional[Dict]:
    """Get transaction details with error handling."""
    try:
        tx = await w3.eth.get_transaction(tx_hash)
        return tx
    except TransactionNotFound:
        return None
    except Exception as e:
        print(f"Error fetching transaction {tx_hash}: {e}")
        return None


async def query_last_sequencer_finalized_snapshot(
    w3: AsyncWeb3,
    protocol_state_address: str,
    data_market_address: str,
    project_id: str,
    abi: List[Dict],
) -> int:
    """Query lastSequencerFinalizedSnapshot from contract."""
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(protocol_state_address),
        abi=abi
    )
    
    try:
        result = await contract.functions.lastSequencerFinalizedSnapshot(
            Web3.to_checksum_address(data_market_address),
            project_id
        ).call()
        return result
    except Exception as e:
        print(f"Error querying lastSequencerFinalizedSnapshot: {e}")
        return 0




async def diagnose_project_transactions():
    """Main diagnostic function."""
    
    print("=" * 80)
    print("Project Transaction Diagnostic Tool")
    print("=" * 80)
    print()
    
    # Get user inputs
    project_id = input("Project ID: ").strip()
    if not project_id:
        print("Error: Project ID is required")
        return
    
    # Try to get RPC URL from settings
    if SETTINGS_AVAILABLE and settings and settings.anchor_chain.rpc.full_nodes:
        rpc_url = settings.anchor_chain.rpc.full_nodes[0].url
        print(f"Using RPC URL from settings: {rpc_url}")
    else:
        rpc_url = input("RPC endpoint URL: ").strip()
        if not rpc_url:
            print("Error: RPC endpoint is required")
            return
    
    # Try to get protocol state address from settings
    if SETTINGS_AVAILABLE and settings and settings.protocol_state_address:
        protocol_state_address = settings.protocol_state_address
        print(f"Using Protocol State address from settings: {protocol_state_address}")
    else:
        protocol_state_address = input("Protocol State contract address: ").strip()
        if not protocol_state_address:
            print("Error: Protocol State address is required")
            return
    
    # Try to extract data market address from project ID (format: prefix:address:suffix)
    # Example: 'allTradesSnapshot:0xb5cE2F9B71e785e3eC0C45EDE06Ad95c3bb71a4d:devnet-BDS_DEVNET_ALPHA_UNISWAPV3-ETH'
    data_market_address = None
    project_id_parts = project_id.split(':')
    if len(project_id_parts) >= 2:
        # Check if second part looks like an Ethereum address
        potential_address = project_id_parts[1]
        if len(potential_address) == 42 and potential_address.startswith('0x'):
            data_market_address = potential_address
            print(f"Extracted Data Market address from project ID: {data_market_address}")
    
    if not data_market_address:
        data_market_address = input("Data Market contract address: ").strip()
        if not data_market_address:
            print("Error: Data Market address is required")
            return
    
    # Optional: block range
    from_block_input = input("From block (optional, press Enter for last 10000 blocks): ").strip()
    to_block_input = input("To block (optional, press Enter for 'latest'): ").strip()
    
    print()
    print("=" * 80)
    print("Connecting to blockchain...")
    print("=" * 80)
    
    # Connect to Web3 with timeout from settings if available
    request_kwargs = {}
    if SETTINGS_AVAILABLE and settings and hasattr(settings, 'anchor_chain') and hasattr(settings.anchor_chain, 'rpc'):
        request_timeout = settings.anchor_chain.rpc.request_time_out
        request_kwargs = {"timeout": Timeout(timeout=float(request_timeout))}
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url, request_kwargs=request_kwargs))
    
    # Add POA middleware if needed
    try:
        w3.middleware_onion.inject(async_geth_poa_middleware, layer=0)
    except ValueError:
        pass
    
    # Get current block
    try:
        current_block = await w3.eth.block_number
        print(f"Current block: {current_block}")
    except Exception as e:
        print(f"Error getting current block: {e}")
        return
    
    # Determine block range
    if from_block_input:
        from_block = int(from_block_input)
    else:
        from_block = max(0, current_block - 10000)
    
    if to_block_input:
        to_block = int(to_block_input)
    else:
        to_block = current_block
    
    print(f"Searching blocks: {from_block} to {to_block}")
    print()
    
    # Load ABI
    abi_path = Path(__file__).parent / 'utils' / 'static' / 'abi.json'
    if not abi_path.exists():
        print(f"Error: ABI file not found at {abi_path}")
        return
    
    with open(abi_path, 'r') as f:
        abi = json.load(f)
    
    print("=" * 80)
    print("Querying Contract State")
    print("=" * 80)
    
    # Query current state
    last_sequencer_finalized = await query_last_sequencer_finalized_snapshot(
        w3, protocol_state_address, data_market_address, project_id, abi
    )
    
    print(f"lastSequencerFinalizedSnapshot: {last_sequencer_finalized}")
    print()
    
    print("=" * 80)
    print("Querying Contract Events")
    print("=" * 80)
    
    # Query SnapshotBatchSubmitted events (emitted when submitSubmissionBatch is called)
    print("Querying SnapshotBatchSubmitted events...")
    batch_submitted_events = await get_contract_events(
        w3,
        protocol_state_address,
        'SnapshotBatchSubmitted',
        abi,
        from_block,
        to_block,
        data_market_address=data_market_address,
    )
    print(f"Found {len(batch_submitted_events)} SnapshotBatchSubmitted events")
    
    # Query SnapshotBatchFinalized events
    print("Querying SnapshotBatchFinalized events...")
    finalized_batch_events = await get_contract_events(
        w3,
        protocol_state_address,
        'SnapshotBatchFinalized',
        abi,
        from_block,
        to_block,
        data_market_address=data_market_address,
    )
    print(f"Found {len(finalized_batch_events)} SnapshotBatchFinalized events")
    
    # Query SnapshotFinalized events (for individual project snapshots)
    print("Querying SnapshotFinalized events...")
    snapshot_finalized_events = await get_contract_events(
        w3,
        protocol_state_address,
        'SnapshotFinalized',
        abi,
        from_block,
        to_block,
        data_market_address=data_market_address,
    )
    
    # Filter SnapshotFinalized events for this project ID
    project_snapshot_events = [
        e for e in snapshot_finalized_events
        if e.get('args', {}).get('projectId') == project_id
    ]
    print(f"Found {len(project_snapshot_events)} SnapshotFinalized events for project ID")
    print()
    
    # Collect unique transaction hashes
    tx_hashes = set()
    
    for event in batch_submitted_events:
        tx_hash = event.get('transactionHash')
        if tx_hash:
            tx_hashes.add(tx_hash.hex() if hasattr(tx_hash, 'hex') else tx_hash)
    
    for event in finalized_batch_events:
        tx_hash = event.get('transactionHash')
        if tx_hash:
            tx_hashes.add(tx_hash.hex() if hasattr(tx_hash, 'hex') else tx_hash)
    
    for event in project_snapshot_events:
        tx_hash = event.get('transactionHash')
        if tx_hash:
            tx_hashes.add(tx_hash.hex() if hasattr(tx_hash, 'hex') else tx_hash)
    
    # Optionally search for transactions by scanning blocks (if events don't capture everything)
    # This is slow, so we only do it if no transactions were found from events
    # or if explicitly requested
    if len(tx_hashes) == 0 and (to_block - from_block) < 5000:
        print("No transactions found from events. Scanning blocks for transactions...")
        print("(This may take a while)")
        
        project_id_bytes = project_id.encode('utf-8')
        project_id_hex = project_id_bytes.hex()
        
        found_txs_from_scan = set()
        blocks_scanned = 0
        
        # Search in smaller chunks to avoid timeout
        chunk_size = 500
        for chunk_start in range(from_block, to_block + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, to_block)
            try:
                for block_num in range(chunk_start, chunk_end + 1):
                    try:
                        block = await w3.eth.get_block(block_num, full_transactions=True)
                        blocks_scanned += 1
                        if blocks_scanned % 100 == 0:
                            print(f"  Scanned {blocks_scanned} blocks...")
                        
                        for tx in block.get('transactions', []):
                            if tx.get('to') and tx['to'].lower() == protocol_state_address.lower():
                                # Check if project ID appears in input data (simple string search)
                                input_data = tx.get('input', '')
                                if isinstance(input_data, bytes):
                                    input_data = input_data.hex()
                                if project_id_hex in input_data.lower() or project_id in str(input_data):
                                    tx_hash = tx['hash']
                                    found_txs_from_scan.add(tx_hash.hex() if hasattr(tx_hash, 'hex') else tx_hash)
                    except BlockNotFound:
                        continue
                    except Exception as e:
                        if 'not found' not in str(e).lower():
                            print(f"  Warning: Error scanning block {block_num}: {e}")
            except Exception as e:
                print(f"  Warning: Error scanning blocks {chunk_start}-{chunk_end}: {e}")
        
        if found_txs_from_scan:
            print(f"Found {len(found_txs_from_scan)} transactions from block scan")
            tx_hashes.update(found_txs_from_scan)
        else:
            print("No transactions found from block scan")
    elif len(tx_hashes) == 0:
        print("No transactions found from events, and block range is too large for scanning.")
        print("Try narrowing the block range or check if the project ID is correct.")
    
    print("=" * 80)
    print("Transaction Analysis")
    print("=" * 80)
    print(f"Found {len(tx_hashes)} unique transactions related to this project")
    print()
    
    if not tx_hashes:
        print("⚠️  No transactions found in the specified block range.")
        print("   This could mean:")
        print("   - The transaction hasn't been submitted yet")
        print("   - The block range is too narrow")
        print("   - The transaction failed before being mined")
        return
    
    # Analyze each transaction
    successful_txs = []
    failed_txs = []
    pending_txs = []
    
    for i, tx_hash in enumerate(sorted(tx_hashes), 1):
        print(f"\n[{i}/{len(tx_hashes)}] Transaction: {tx_hash}")
        print("-" * 80)
        
        # Get transaction details
        tx = await get_transaction(w3, tx_hash)
        if tx:
            print(f"From: {tx['from']}")
            print(f"To: {tx['to']}")
            print(f"Block Number: {tx.get('blockNumber', 'Pending')}")
            print(f"Gas Price: {tx.get('gasPrice', 0)} wei")
            print(f"Gas Limit: {tx.get('gas', 0)}")
        else:
            print("⚠️  Transaction not found on chain")
            pending_txs.append(tx_hash)
            continue
        
        # Get receipt
        receipt = await get_transaction_receipt(w3, tx_hash)
        if receipt:
            status = receipt['status']
            print(f"Status: {'✅ SUCCESS' if status == 1 else '❌ FAILED'}")
            print(f"Block Number: {receipt['blockNumber']}")
            print(f"Gas Used: {receipt['gasUsed']}")
            print(f"Transaction Index: {receipt['transactionIndex']}")
            
            if status == 1:
                successful_txs.append(tx_hash)
            else:
                failed_txs.append(tx_hash)
            
            # Check events in receipt
            if receipt.get('logs'):
                print(f"Events in receipt: {len(receipt['logs'])}")
                # Decode events if possible
                contract = w3.eth.contract(address=protocol_state_address, abi=abi)
                for log in receipt['logs']:
                    try:
                        decoded = contract.events.SnapshotFinalized().process_log(log)
                        if decoded and decoded.get('args', {}).get('projectId') == project_id:
                            print(f"  ✅ SnapshotFinalized event found for project ID!")
                            print(f"     Epoch ID: {decoded['args'].get('epochId')}")
                            print(f"     Snapshot CID: {decoded['args'].get('snapshotCid')}")
                    except:
                        pass
        else:
            print("⚠️  Receipt not found (transaction may be pending)")
            pending_txs.append(tx_hash)
    
    print()
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Total transactions found: {len(tx_hashes)}")
    print(f"✅ Successful: {len(successful_txs)}")
    print(f"❌ Failed: {len(failed_txs)}")
    print(f"⏳ Pending: {len(pending_txs)}")
    print()
    print(f"Contract State:")
    print(f"  lastSequencerFinalizedSnapshot: {last_sequencer_finalized}")
    print()
    
    if last_sequencer_finalized == 0:
        print("⚠️  WARNING: lastSequencerFinalizedSnapshot is 0")
        print("   This indicates that no snapshot has been finalized for this project ID.")
        print()
        if successful_txs:
            print("   However, successful transactions were found.")
            print("   Possible reasons:")
            print("   - The transactions were for batch submissions, not finalizations")
            print("   - The project ID in the transactions doesn't match exactly")
            print("   - The finalization requires multiple sequencers and hasn't reached threshold")
        elif failed_txs:
            print("   Failed transactions were found:")
            for tx_hash in failed_txs:
                print(f"     - {tx_hash}")
        else:
            print("   No successful transactions found in the block range.")
    else:
        print(f"✅ lastSequencerFinalizedSnapshot is {last_sequencer_finalized}")
        print("   A snapshot has been finalized for this project ID.")
    
    print()
    print("=" * 80)


if __name__ == '__main__':
    try:
        asyncio.run(diagnose_project_transactions())
    except KeyboardInterrupt:
        print("\n\nCancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

