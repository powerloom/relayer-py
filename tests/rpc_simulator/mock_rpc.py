"""
Mock Unreliable RPC Provider
Simulates various RPC failure scenarios for testing transaction queue behavior.
"""

import asyncio
import json
import random
from datetime import datetime
from typing import Dict, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="Mock Unreliable RPC Provider")

# Storage for transactions and receipts
transactions: Dict[str, dict] = {}
receipts: Dict[str, dict] = {}
block_number = 1000000
pending_transactions = []

# Configuration
config = {
    'false_negative_rate': 0.0,  # Probability of returning wrong receipt status
    'timeout_rate': 0.0,          # Probability of timing out
    'delay_rate': 0.0,            # Probability of adding delay
    'delay_seconds': 5.0,         # Delay duration in seconds
    'drop_rate': 0.0,             # Probability of dropping transaction
    'enable_logging': True,       # Log all requests
}


def generate_tx_hash():
    """Generate a mock transaction hash."""
    return '0x' + ''.join(random.choices('0123456789abcdef', k=64))


def log(message: str):
    """Log message if logging is enabled."""
    if config['enable_logging']:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'[{timestamp}] {message}')


@app.post('/')
async def rpc_handler(request: Request):
    """Handle JSON-RPC requests."""
    data = await request.json()
    method = data.get('method')
    params = data.get('params', [])
    request_id = data.get('id')
    
    log(f'RPC Request: {method} with params: {params}')
    
    # Simulate timeout
    if random.random() < config['timeout_rate']:
        log('SIMULATING TIMEOUT')
        await asyncio.sleep(config['delay_seconds'] + 5)
        return JSONResponse({
            'jsonrpc': '2.0',
            'error': {'code': -32000, 'message': 'Request timeout'},
            'id': request_id,
        })
    
    # Simulate delay
    if random.random() < config['delay_rate']:
        log(f'SIMULATING DELAY: {config["delay_seconds"]}s')
        await asyncio.sleep(config['delay_seconds'])
    
    result = None
    
    try:
        if method == 'eth_sendRawTransaction':
            result = handle_send_raw_transaction(params)
            
        elif method == 'eth_getTransactionReceipt':
            result = handle_get_transaction_receipt(params)
            
        elif method == 'eth_getTransactionCount':
            result = handle_get_transaction_count(params)
            
        elif method == 'eth_blockNumber':
            result = hex(block_number)
            
        elif method == 'eth_gasPrice':
            result = hex(20000000000)  # 20 gwei
            
        elif method == 'eth_getCode':
            result = '0x'  # Contract not deployed
            
        elif method == 'eth_chainId':
            result = hex(8453)  # Base mainnet chain ID (or use 11167 for devnet)
            
        elif method == 'eth_estimateGas':
            # Return a reasonable gas estimate
            result = hex(21000)
            
        else:
            log(f'Unknown method: {method}')
            return JSONResponse({
                'jsonrpc': '2.0',
                'error': {'code': -32601, 'message': f'Method not found: {method}'},
                'id': request_id,
            })
            
    except Exception as e:
        log(f'Error handling {method}: {e}')
        return JSONResponse({
            'jsonrpc': '2.0',
            'error': {'code': -32000, 'message': str(e)},
            'id': request_id,
        })
    
    return JSONResponse({
        'jsonrpc': '2.0',
        'result': result,
        'id': request_id,
    })


def handle_send_raw_transaction(params: list) -> str:
    """Handle eth_sendRawTransaction - stores transaction and returns hash."""
    raw_tx = params[0] if params else ''
    
    # Simulate dropping transaction
    if random.random() < config['drop_rate']:
        log('SIMULATING DROPPED TRANSACTION')
        raise Exception('Transaction dropped by network')
    
    tx_hash = generate_tx_hash()
    
    # Store transaction
    transactions[tx_hash] = {
        'hash': tx_hash,
        'raw': raw_tx,
        'timestamp': datetime.now().isoformat(),
    }
    
    # Create a receipt (will be mined in 2 seconds)
    asyncio.create_task(mine_transaction(tx_hash))
    
    log(f'Accepted transaction: {tx_hash}')
    return tx_hash


async def mine_transaction(tx_hash: str):
    """Simulate mining a transaction after 2 seconds."""
    await asyncio.sleep(2)
    
    # Create receipt - randomly return wrong status if configured
    status = 1  # Success
    if random.random() < config['false_negative_rate']:
        status = 0  # False negative
        log(f'CREATING FALSE NEGATIVE RECEIPT for {tx_hash}')
    
    receipt = {
        'transactionHash': tx_hash,
        'blockNumber': hex(block_number),
        'blockHash': '0x' + ''.join(random.choices('0123456789abcdef', k=64)),
        'status': status,
        'gasUsed': hex(21000),
        'cumulativeGasUsed': hex(21000),
        'transactionIndex': hex(0),
        'from': '0x' + ''.join(random.choices('0123456789abcdef', k=40)),
        'to': '0x' + ''.join(random.choices('0123456789abcdef', k=40)),
        'logs': [],
    }
    
    receipts[tx_hash] = receipt
    log(f'Mined transaction: {tx_hash} with status: {status}')


def handle_get_transaction_receipt(params: list) -> Optional[dict]:
    """Handle eth_getTransactionReceipt - returns receipt with potential false negative."""
    tx_hash = params[0] if params else ''
    
    if tx_hash not in receipts:
        return None
    
    receipt = receipts[tx_hash].copy()
    
    # Simulate false negative receipt response
    if random.random() < config['false_negative_rate']:
        receipt['status'] = 0  # Return failed even though it succeeded
        log(f'RETURNING FALSE NEGATIVE RECEIPT for {tx_hash}')
    
    return receipt


def handle_get_transaction_count(params: list) -> str:
    """Handle eth_getTransactionCount - returns nonce."""
    address = params[0] if params else ''
    block = params[1] if len(params) > 1 else 'latest'
    
    # Return a random nonce (or track it properly)
    nonce = random.randint(0, 1000)
    log(f'Returning nonce {nonce} for address {address}')
    return hex(nonce)


@app.get('/config')
async def get_config():
    """Get current configuration."""
    return config


@app.post('/config')
async def update_config(request: Request):
    """Update configuration."""
    global config
    data = await request.json()
    config.update(data)
    log(f'Configuration updated: {config}')
    return config


@app.get('/stats')
async def get_stats():
    """Get statistics."""
    return {
        'transactions': len(transactions),
        'receipts': len(receipts),
        'pending': len(pending_transactions),
        'block_number': block_number,
        'config': config,
    }


@app.post('/reset')
async def reset():
    """Reset state."""
    global transactions, receipts, pending_transactions, block_number
    transactions.clear()
    receipts.clear()
    pending_transactions.clear()
    block_number = 1000000
    log('State reset')
    return {'status': 'reset'}


if __name__ == '__main__':
    import uvicorn
    print('=' * 60)
    print('Mock Unreliable RPC Provider')
    print('=' * 60)
    print('Endpoints:')
    print('  POST / - JSON-RPC endpoint')
    print('  GET  /config - Get configuration')
    print('  POST /config - Update configuration')
    print('  GET  /stats - Get statistics')
    print('  POST /reset - Reset state')
    print('=' * 60)
    uvicorn.run(app, host='0.0.0.0', port=8545)


