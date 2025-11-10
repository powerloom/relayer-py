"""
Transaction function factories for relayer-py transaction pattern.

These factories create async transaction functions compatible with TransactionQueue,
adapting relayer-py's write_transaction signature to the queue's expected format.
"""

from typing import Callable, Any
from utils.transaction_utils import write_transaction


def create_transaction_func(
    w3,
    address,
    private_key,
    contract,
    function,
    gas_price,
    priority_gas_multiplier: int = 0,
    *args,
):
    """
    Create a transaction function for use with tx queue (relayer-py pattern).
    
    This adapts relayer-py's write_transaction signature to work with TransactionQueue.
    The queue will pass nonce as the first parameter.
    
    Args:
        w3: Web3 instance
        address: Signer address
        private_key: Signer private key
        contract: Contract instance
        function: Function name
        gas_price: Base gas price
        priority_gas_multiplier: Priority gas multiplier (default: 0)
        *args: Function arguments
        
    Returns:
        Async function that accepts nonce as first parameter
    """
    async def _tx_func(nonce: int):
        return await write_transaction(
            w3,
            address,
            private_key,
            contract,
            function,
            nonce,
            gas_price,
            priority_gas_multiplier,
            *args,
        )
    return _tx_func


def create_transaction_func_with_dynamic_gas(
    w3,
    address,
    private_key,
    contract,
    function,
    get_gas_price_func: Callable[[], Any],
    priority_gas_multiplier: int = 0,
    *args,
):
    """
    Create a transaction function with dynamic gas price for use with tx queue.
    
    Gas price is fetched dynamically on each transaction attempt.
    Useful when gas price changes between retries.
    
    Args:
        w3: Web3 instance
        address: Signer address
        private_key: Signer private key
        contract: Contract instance
        function: Function name
        get_gas_price_func: Async function that returns current gas price
        priority_gas_multiplier: Priority gas multiplier (default: 0)
        *args: Function arguments
        
    Returns:
        Async function that accepts nonce as first parameter
    """
    async def _tx_func(nonce: int):
        gas_price = await get_gas_price_func()
        return await write_transaction(
            w3,
            address,
            private_key,
            contract,
            function,
            nonce,
            gas_price,
            priority_gas_multiplier,
            *args,
        )
    return _tx_func

