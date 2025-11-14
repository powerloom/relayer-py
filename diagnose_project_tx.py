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
    
    Uses indexed parameter filtering (topics) for efficient querying when possible.
    
    Args:
        w3: Web3 instance
        contract_address: Contract address
        event_name: Name of the event to query
        abi: Contract ABI
        from_block: Starting block number
        to_block: Ending block number
        data_market_address: Optional filter for dataMarketAddress (indexed)
        epoch_id: Optional filter for epochId (indexed)
        
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
    
    # Build filter arguments with indexed parameter filtering
    filter_args = {
        'fromBlock': from_block,
        'toBlock': to_block,
    }
    
    # Use indexed parameter filtering if available
    # dataMarketAddress is typically the first indexed parameter
    # epochId is typically the second indexed parameter
    argument_filters = {}
    if data_market_address:
        # Find which input is dataMarketAddress
        for i, input_item in enumerate(event_abi.get('inputs', [])):
            if input_item.get('name') == 'dataMarketAddress' and input_item.get('indexed'):
                argument_filters[input_item['name']] = Web3.to_checksum_address(data_market_address)
                break
    
    if epoch_id is not None:
        # Find which input is epochId
        for i, input_item in enumerate(event_abi.get('inputs', [])):
            if input_item.get('name') == 'epochId' and input_item.get('indexed'):
                argument_filters[input_item['name']] = epoch_id
                break
    
    # Build event filter with argument filters (uses indexed parameters)
    try:
        if argument_filters:
            event_filter = await contract.events[event_name].create_filter(
                argument_filters=argument_filters,
                **filter_args
            )
        else:
            event_filter = await contract.events[event_name].create_filter(**filter_args)
    except Exception as e:
        print(f"Error creating event filter for {event_name} on {contract_address}: {e}")
        return []
    
    # Get events
    try:
        events = await event_filter.get_all_entries()
    except Exception as e:
        print(f"Error fetching events for {event_name} on {contract_address}: {e}")
        return []
    
    # Additional filtering for non-indexed parameters (shouldn't be needed if indexed filtering worked)
    if data_market_address or epoch_id is not None:
        filtered_events = []
        for event in events:
            args = event.get('args', {})
            match = True
            
            if data_market_address:
                event_dm = args.get('dataMarketAddress', '')
                if isinstance(event_dm, str):
                    if event_dm.lower() != data_market_address.lower():
                        match = False
                elif hasattr(event_dm, 'lower'):
                    if event_dm.lower() != data_market_address.lower():
                        match = False
                else:
                    # Try to convert to checksum address and compare
                    try:
                        if Web3.to_checksum_address(str(event_dm)).lower() != Web3.to_checksum_address(data_market_address).lower():
                            match = False
                    except:
                        match = False
            
            if epoch_id is not None and match:
                if args.get('epochId') != epoch_id:
                    match = False
            
            if match:
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


async def check_contract_conditions(
    w3: AsyncWeb3,
    protocol_state_address: str,
    data_market_address: str,
    abi: List[Dict],
    signer_address: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Check contract conditions that might cause transaction reverts.
    
    Returns:
        Dict with diagnostic information
    """
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(protocol_state_address),
        abi=abi
    )
    
    diagnostics = {}
    
    try:
        # Check if signer is a sequencer (if provided)
        if signer_address:
            try:
                # Try to call isSequencer on DataMarket (via ProtocolState or directly)
                # Note: This might not be available in ProtocolState ABI, so we'll try
                diagnostics['signer_address'] = signer_address
                diagnostics['signer_checksum'] = Web3.to_checksum_address(signer_address)
            except Exception as e:
                diagnostics['signer_check_error'] = str(e)
    except Exception as e:
        diagnostics['error'] = f'Error checking contract conditions: {e}'
    
    return diagnostics


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
    
    # Connect to Web3
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    
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
    
    # Check batch submission window (critical parameter that can cause reverts)
    print("Checking batch submission window...")
    try:
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(protocol_state_address),
            abi=abi
        )
        submission_window = await contract.functions.snapshotSubmissionWindow(
            Web3.to_checksum_address(data_market_address)
        ).call()
        print(f"snapshotSubmissionWindow: {submission_window}")
        if submission_window == 0:
            print("  ⚠️  WARNING: snapshotSubmissionWindow is 0!")
            print("     This will cause all batch submissions to revert.")
            print("     Transactions cannot be submitted when window is 0.")
        else:
            print(f"  ✅ Window is set to {submission_window} blocks")
    except Exception as e:
        print(f"  Error checking submission window: {e}")
    print()
    
    if SETTINGS_AVAILABLE and settings and settings.signers:
        signer_addresses = [signer.address for signer in settings.signers]
        print(f"Checking {len(signer_addresses)} relayer signers...")
        
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(protocol_state_address),
            abi=abi
        )
        
        # Check signer balances and transaction counts
        for i, signer_addr in enumerate(signer_addresses[:3], 1):  # Check first 3
            try:
                checksum_addr = Web3.to_checksum_address(signer_addr)
                balance = await w3.eth.get_balance(checksum_addr)
                balance_eth = w3.from_wei(balance, 'ether')
                tx_count = await w3.eth.get_transaction_count(checksum_addr)
                
                print(f"\nSigner {i}: {signer_addr}")
                print(f"  Balance: {balance_eth} ETH")
                print(f"  Transaction count (nonce): {tx_count}")
                
                if balance_eth < 0.01:
                    print(f"  ⚠️  WARNING: Low balance! May cause transaction failures.")
            except Exception as e:
                print(f"  Error checking signer: {e}")
    
    print()
    
    print("=" * 80)
    print("Querying Contract Events")
    print("=" * 80)
    
    # First, check if there are ANY events at all (without filtering) to diagnose
    print("Checking for ANY SnapshotBatchSubmitted events (no filters)...")
    all_events_check = await get_contract_events(
        w3,
        protocol_state_address,
        'SnapshotBatchSubmitted',
        abi,
        from_block,
        to_block,
        data_market_address=None,  # No filter
        epoch_id=None,
    )
    print(f"Found {len(all_events_check)} total SnapshotBatchSubmitted events from ProtocolState (all data markets)")
    
    if len(all_events_check) > 0:
        print("Sample events (first 3):")
        for i, event in enumerate(all_events_check[:3], 1):
            args = event.get('args', {})
            print(f"  {i}. dataMarket={args.get('dataMarketAddress')}, epochID={args.get('epochId')}, batchCID={args.get('batchCid')}")
    
    # Query SnapshotBatchSubmitted events from ProtocolState with filtering
    # Note: Events might be emitted from DataMarket when ProtocolState routes calls
    print("\nQuerying SnapshotBatchSubmitted events from ProtocolState (filtered)...")
    batch_submitted_events_ps = await get_contract_events(
        w3,
        protocol_state_address,
        'SnapshotBatchSubmitted',
        abi,
        from_block,
        to_block,
        data_market_address=data_market_address,
    )
    print(f"Found {len(batch_submitted_events_ps)} SnapshotBatchSubmitted events from ProtocolState")
    
    # Also query from DataMarket contract (events might be emitted there)
    print("Querying SnapshotBatchSubmitted events from DataMarket (filtered)...")
    batch_submitted_events_dm = await get_contract_events(
        w3,
        data_market_address,
        'SnapshotBatchSubmitted',
        abi,
        from_block,
        to_block,
        data_market_address=data_market_address,
    )
    print(f"Found {len(batch_submitted_events_dm)} SnapshotBatchSubmitted events from DataMarket")
    
    # Combine results
    batch_submitted_events = batch_submitted_events_ps + batch_submitted_events_dm
    print(f"Total SnapshotBatchSubmitted events (filtered): {len(batch_submitted_events)}")
    
    # Also query DelayedBatchSubmitted events (delayed submissions)
    print("\nQuerying DelayedBatchSubmitted events from ProtocolState...")
    delayed_batch_events_ps = await get_contract_events(
        w3,
        protocol_state_address,
        'DelayedBatchSubmitted',
        abi,
        from_block,
        to_block,
        data_market_address=data_market_address,
    )
    print(f"Found {len(delayed_batch_events_ps)} DelayedBatchSubmitted events from ProtocolState")
    
    # Also query from DataMarket
    print("Querying DelayedBatchSubmitted events from DataMarket...")
    delayed_batch_events_dm = await get_contract_events(
        w3,
        data_market_address,
        'DelayedBatchSubmitted',
        abi,
        from_block,
        to_block,
        data_market_address=data_market_address,
    )
    print(f"Found {len(delayed_batch_events_dm)} DelayedBatchSubmitted events from DataMarket")
    
    # Combine delayed batch events
    delayed_batch_events = delayed_batch_events_ps + delayed_batch_events_dm
    print(f"Total DelayedBatchSubmitted events: {len(delayed_batch_events)}")
    print()
    
    # Collect unique transaction hashes from SnapshotBatchSubmitted and DelayedBatchSubmitted events
    tx_hashes = set()
    
    for event in batch_submitted_events:
        tx_hash = event.get('transactionHash')
        if tx_hash:
            tx_hashes.add(tx_hash.hex() if hasattr(tx_hash, 'hex') else tx_hash)
    
    for event in delayed_batch_events:
        tx_hash = event.get('transactionHash')
        if tx_hash:
            tx_hashes.add(tx_hash.hex() if hasattr(tx_hash, 'hex') else tx_hash)
    
    # Optionally search for transactions by scanning blocks (if events don't capture everything)
    # This is slow, so we only do it if no transactions were found from events
    # and the block range is reasonable (< 5000 blocks)
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
        print()
        print("⚠️  No transactions found from events in the specified block range.")
        print(f"   Searched blocks {from_block} to {to_block} ({to_block - from_block + 1} blocks)")
        print()
        print("   Possible reasons:")
        print("   - The transaction hasn't been submitted yet")
        print("   - The transaction occurred outside this block range")
        print("   - The project ID might be incorrect")
        print()
        print("   To search a wider range, run the script again and specify a larger 'from_block'")
        print("   (e.g., if you know the approximate block number, search from there)")
    
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
                # Decode SnapshotBatchSubmitted events if possible
                contract = w3.eth.contract(address=protocol_state_address, abi=abi)
                for log in receipt['logs']:
                    try:
                        decoded = contract.events.SnapshotBatchSubmitted().process_log(log)
                        if decoded:
                            args = decoded.get('args', {})
                            if args.get('dataMarketAddress', '').lower() == data_market_address.lower():
                                print(f"  ✅ SnapshotBatchSubmitted event found!")
                                print(f"     Epoch ID: {args.get('epochId')}")
                                print(f"     Batch CID: {args.get('batchCid')}")
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
            print()
            print("   Common revert reasons:")
            print("   - E04: Signer is not registered as sequencer")
            print("   - E25: Snapshot batch already submitted")
            print("   - E43: Batch submissions already completed for epoch")
            print("   - E23: Project IDs and snapshot CIDs length mismatch")
            print("   - E24: Project IDs and snapshot CIDs cannot be empty")
            print("   - Insufficient gas or funds")
        else:
            print("   No successful transactions found in the block range.")
            print()
            print("   Possible reasons for no transactions:")
            print("   - Transactions are reverting before being mined (check relayer logs for gas estimation errors)")
            print("   - Transactions are being submitted but failing silently")
            print("   - The block range doesn't cover when transactions were submitted")
            print("   - Signers are not registered as sequencers (E04)")
            print("   - Low signer balance causing transaction failures")
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

