"""
Generic Transaction Queue Framework

A reusable async queue-based transaction manager with proper nonce management.
Designed to work with both gpm_backend and relayer-py patterns.

Key Features:
- Nonce incremented ONLY after successful receipt confirmation
- Gas estimation before transaction submission
- Automatic nonce error recovery with retry
- Receipt verification to handle unreliable RPC providers
- Queue-based async processing
- Redis state commitment for epoch tracking (DSV integration)
"""

import asyncio
import re
import time
from typing import Callable, Optional, Any

from web3 import Web3
from utils.default_logger import logger
from utils.redis_conn import get_writer_redis_conn
from settings.conf import settings

tx_logger = logger.bind(service='PowerLoom|Relayer|TX Queue')


class TransactionQueue:
    """
    Generic async queue-based transaction manager.
    
    Provides proper nonce management with receipt-based confirmation.
    Nonce is only incremented AFTER successful receipt confirmation.
    
    Supports two modes:
    - fire_and_forget: Submit transaction, return immediately (nonce/receipt handled in background)
    - wait_for_receipt: Submit transaction, wait for receipt confirmation, then return
    """
    
    def __init__(
        self,
        max_size: int = 100,
        service_name: str = "Relayer",
        wait_for_receipt: bool = False,
        max_results_cache: int = 1000,
    ):
        """
        Initialize the transaction queue.
        
        Args:
            max_size: Maximum queue size before rejecting new transactions
            service_name: Service name for logging (default: "Relayer")
            wait_for_receipt: If True, wait for receipt confirmation before returning.
                             If False (default), return immediately after submission (fire-and-forget)
            max_results_cache: Maximum number of transaction results to keep in memory (default: 1000)
        """
        self._queue = asyncio.Queue(maxsize=max_size)
        self._processing = False
        self._pending_txs = {}  # tx_id -> Future
        self._tx_results = {}  # tx_id -> result dict (for receipt updates)
        self._tx_id_counter = 0
        self._current_nonce = None
        self._nonce_lock = asyncio.Lock()
        self._tx_processor_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._service_name = service_name
        self._wait_for_receipt = wait_for_receipt
        self._max_results_cache = max_results_cache
        
        # Bind logger with service name
        self._logger = logger.bind(service=f'PowerLoom|{service_name}|TX Queue')
        
    async def start(self, initial_nonce: int):
        """
        Start the transaction processor.
        
        Args:
            initial_nonce: The initial nonce value from the blockchain
        """
        self._current_nonce = initial_nonce
        self._processing = True
        self._tx_processor_task = asyncio.create_task(self._process_transactions())
        self._cleanup_task = asyncio.create_task(self._cleanup_old_results())
        self._logger.info(f'Transaction queue started with nonce: {initial_nonce}')
        
    async def stop(self):
        """Stop the transaction processor."""
        self._processing = False
        await self._queue.put(None)  # Poison pill
        if self._tx_processor_task:
            await self._tx_processor_task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        self._logger.info('Transaction queue stopped')
        
    async def submit_transaction(
        self,
        tx_func: Callable,
        w3=None,
        contract=None,
        function_name=None,
        signer_address=None,
        function_args=None,
        epoch_id: Optional[int] = None,
        data_market_address: Optional[str] = None,
        *args,
        **kwargs
    ) -> str:
        """
        Submit a transaction for async processing.
        
        Args:
            tx_func: Async function that sends the transaction (takes nonce as parameter)
            w3: Optional Web3 instance for receipt checking
            contract: Optional contract instance for gas estimation
            function_name: Optional function name for gas estimation
            signer_address: Optional signer address for gas estimation
            function_args: Optional tuple/list of function arguments for gas estimation
            epoch_id: Optional epoch ID for Redis state tracking (DSV integration)
            data_market_address: Optional data market address for Redis state tracking (DSV integration)
            *args: Arguments to pass to tx_func (usually empty, nonce is passed separately)
            **kwargs: Keyword arguments to pass to tx_func
            
        Returns:
            Transaction ID (for tracking purposes)
            
        Raises:
            RuntimeError: If queue is full
        """
        tx_id = self._get_next_tx_id()
        future = asyncio.Future()
        self._pending_txs[tx_id] = future
        
        tx_item = {
            'tx_id': tx_id,
            'tx_func': tx_func,
            'args': args,
            'kwargs': kwargs,
            'future': future,
            'w3': w3,
            'contract': contract,
            'function_name': function_name,
            'signer_address': signer_address,
            'function_args': function_args,
            'epoch_id': epoch_id,
            'data_market_address': data_market_address,
        }
        
        # Put with timeout to avoid indefinite blocking if queue is full
        try:
            await asyncio.wait_for(self._queue.put(tx_item), timeout=5.0)
            self._logger.debug(f'Queued transaction {tx_id}')
            
            # If wait_for_receipt mode, wait for the transaction to complete
            if self._wait_for_receipt:
                try:
                    result = await asyncio.wait_for(future, timeout=120.0)
                    # Check if transaction failed
                    receipt = result.get('receipt')
                    if receipt and receipt['status'] == 0:
                        raise Exception(f'Transaction {result["tx_hash"]} failed on-chain')
                    return result.get('tx_hash', tx_id)
                except asyncio.TimeoutError:
                    self._logger.warning(f'Transaction {tx_id} timeout waiting for receipt')
                    raise RuntimeError('Transaction timeout waiting for receipt')
            
            # Fire-and-forget mode: return immediately
            return tx_id
        except asyncio.TimeoutError:
            del self._pending_txs[tx_id]
            self._logger.error(f'Transaction queue full (timeout) for {tx_id}')
            raise RuntimeError('Transaction queue full, please retry')
    
    async def _process_transactions(self):
        """Background task to process queued transactions."""
        while self._processing:
            try:
                tx_item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                
                # Poison pill to stop processing
                if tx_item is None:
                    break
                    
                await self._execute_transaction(tx_item)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self._logger.opt(exception=True).error(f'Error in transaction processor: {e}')
    
    async def _execute_transaction(self, tx_item: dict, retry_count: int = 0):
        """
        Execute a single transaction with proper nonce management.
        
        CRITICAL: Nonce is only incremented AFTER successful receipt confirmation.
        This prevents nonce gaps from false negative receipts or network failures.
        
        Includes gas estimation to catch insufficient funds early, and automatic
        nonce reset when nonce errors are detected. Automatically retries once
        after nonce reset to recover from nonce sync issues.
        
        Args:
            tx_item: Transaction item with tx_func, args, kwargs, future, w3, and contract info
            retry_count: Number of retries already attempted (for nonce error recovery)
        """
        tx_id = tx_item['tx_id']
        tx_func = tx_item['tx_func']
        args = tx_item['args']
        kwargs = tx_item['kwargs']
        future = tx_item['future']
        w3 = tx_item.get('w3')
        contract = tx_item.get('contract')
        function_name = tx_item.get('function_name')
        signer_address = tx_item.get('signer_address')
        nonce = None
        
        try:
            # Get nonce but DON'T increment yet
            async with self._nonce_lock:
                nonce = self._current_nonce
            
            self._logger.info(f'Processing transaction {tx_id} with nonce {nonce} (retry {retry_count})')
            
            # Estimate gas before submission (catches insufficient funds and contract errors early)
            function_args = tx_item.get('function_args')
            if contract and function_name and signer_address and w3 and function_args:
                try:
                    func = getattr(contract.functions, function_name)
                    estimated_gas = await func(*function_args).estimate_gas({'from': signer_address})
                    self._logger.debug(f'Transaction {tx_id} gas estimate: {estimated_gas}')
                except Exception as gas_error:
                    error_msg = str(gas_error).lower()
                    error_name = type(gas_error).__name__.lower()
                    
                    # Handle RPC errors during gas estimation
                    if 'timeout' in error_msg or 'timeexhausted' in error_name or 'dataerror' in error_name:
                        self._logger.error(
                            f'RPC error during gas estimation for {tx_id}: {gas_error}. '
                            f'RPC endpoint may be unreliable.'
                        )
                        raise RuntimeError(
                            f'RPC error during gas estimation: {gas_error}. '
                            f'Please check RPC endpoint reliability.'
                        ) from gas_error
                    
                    if 'insufficient funds' in error_msg or 'insufficient balance' in error_msg:
                        self._logger.error(f'Transaction {tx_id} failed gas estimation: insufficient funds')
                        raise RuntimeError('Insufficient funds for transaction') from gas_error
                    # Other gas estimation errors are logged but don't block submission
                    self._logger.warning(f'Gas estimation failed for {tx_id}: {gas_error}')
            
            # Execute transaction (tx_func takes nonce as parameter)
            tx_hash = await tx_func(nonce, *args, **kwargs)
            
            self._logger.info(f'Transaction {tx_id} submitted with hash {tx_hash}')
            
            # Create result dict (will be updated with receipt later)
            result = {
                'tx_id': tx_id,
                'tx_hash': tx_hash,
                'nonce': nonce,
                'status': 'submitted',
                'receipt': None,
            }
            
            # Store result for tracking
            self._tx_results[tx_id] = result
            
            # Set future result immediately (for fire-and-forget mode)
            # Caller gets tx_hash right away, receipt comes later
            if not future.done():
                future.set_result(result)
            
            # Update epoch state in Redis immediately (submitted status)
            await self._update_epoch_state_in_redis(
                tx_item, tx_hash, None, failed=False
            )
            
            # Wait for receipt if w3 is provided (always done in background for nonce management)
            receipt = None
            if w3:
                receipt = await self._wait_for_receipt_with_verification(w3, tx_hash)
                self._logger.info(f'Transaction {tx_id} receipt received, status: {receipt["status"]}')
                
                # Update result dict with receipt (for get_status() later)
                result['status'] = 'confirmed'
                result['receipt'] = receipt
                
                # Write epoch state to Redis for DSV monitoring (if epoch metadata available)
                await self._update_epoch_state_in_redis(
                    tx_item, tx_hash, receipt
                )
            
            # CRITICAL: Only increment nonce AFTER successful receipt confirmation
            if receipt and receipt['status'] == 1:
                async with self._nonce_lock:
                    self._current_nonce += 1
                    self._logger.info(f'Nonce incremented to {self._current_nonce} after successful receipt')
            elif receipt and receipt['status'] == 0:
                error_msg = f'Transaction reverted on-chain (receipt.status=0) | tx_hash={tx_hash}'
                self._logger.error(f'Transaction {tx_id} failed on-chain, nonce {nonce} not consumed')
                # Update epoch state to failed with error message
                await self._update_epoch_state_in_redis(
                    tx_item, tx_hash, receipt, failed=True, error_message=error_msg
                )
            elif not receipt:
                # No receipt checking, assume success (for backwards compatibility)
                # But this should rarely happen if w3 is provided
                async with self._nonce_lock:
                    self._current_nonce += 1
                    self._logger.warning(f'Transaction {tx_id} completed without receipt confirmation, incrementing nonce')
            
        except Exception as e:
            error_str = str(e).lower()
            error_repr = repr(e).lower()
            error_msg = str(e)
            
            # Update epoch state to failed with error message (if we have epoch metadata)
            try:
                await self._update_epoch_state_in_redis(
                    tx_item, tx_hash if 'tx_hash' in locals() else '', None, 
                    failed=True, error_message=error_msg
                )
            except Exception as redis_error:
                # Don't fail transaction processing if Redis update fails
                self._logger.warning(f'Failed to update epoch state in Redis: {redis_error}')
            
            # Handle nonce errors - extract correct nonce and retry
            if 'nonce too low' in error_str or 'nonce too high' in error_str or 'nonce too low' in error_repr or 'nonce too high' in error_repr:
                error_msg = str(e)
                
                # Handle web3 exceptions - they often have args with error dict
                if hasattr(e, 'args') and e.args:
                    for arg in e.args:
                        if isinstance(arg, dict) and 'message' in arg:
                            error_msg = arg['message']
                            break
                        elif isinstance(arg, str) and 'nonce' in arg.lower():
                            error_msg = arg
                            break
                        elif isinstance(arg, dict):
                            error_msg = str(arg)
                            break
                
                # Handle dict-style errors directly
                if isinstance(e, dict) and 'message' in e:
                    error_msg = e['message']
                
                # Also check repr in case error dict is in string representation
                if error_msg == str(e) and ('{' in error_repr and 'message' in error_repr):
                    match = re.search(r"'message':\s*['\"]([^'\"]+)['\"]", error_repr)
                    if match:
                        error_msg = match.group(1)
                
                # Try to extract nonce from error message
                correct_nonce = self._extract_nonce_from_error(error_msg)
                if correct_nonce is not None:
                    await self.reset_nonce(correct_nonce)
                    self._logger.warning(f'Nonce error detected for {tx_id}. Reset nonce from {nonce} to {correct_nonce}')
                else:
                    # Fallback: reset to blockchain nonce
                    if w3 and signer_address:
                        try:
                            blockchain_nonce = await w3.eth.get_transaction_count(signer_address)
                            await self.reset_nonce(blockchain_nonce)
                            self._logger.warning(f'Nonce error detected for {tx_id}. Reset nonce from {nonce} to {blockchain_nonce} (from blockchain)')
                            correct_nonce = blockchain_nonce
                        except Exception as reset_error:
                            self._logger.error(f'Failed to reset nonce: {reset_error}')
                            correct_nonce = None
                
                # Retry transaction with corrected nonce (max 1 retry to prevent infinite loops)
                if correct_nonce is not None and retry_count < 1:
                    self._logger.info(f'Retrying transaction {tx_id} with corrected nonce {correct_nonce}')
                    await self._execute_transaction(tx_item, retry_count=retry_count + 1)
                    return  # Exit early - retry will handle future result/exception
                else:
                    if retry_count >= 1:
                        self._logger.error(f'Transaction {tx_id} failed after {retry_count + 1} attempts with nonce errors')
                    else:
                        self._logger.error(f'Transaction {tx_id} failed: could not determine correct nonce')
            
            self._logger.opt(exception=True).error(f'Transaction {tx_id} failed: {e}')
            
            # Store error in results
            self._tx_results[tx_id] = {
                'tx_id': tx_id,
                'status': 'failed',
                'error': str(e),
            }
            
            # Set exception in future (nonce already not incremented)
            if not future.done():
                future.set_exception(e)
    
    def _extract_nonce_from_error(self, error_msg: str) -> Optional[int]:
        """
        Extract correct nonce from error message.
        
        Handles errors like:
        - "nonce too low: next nonce 1372, tx nonce 1367"
        - "nonce too high: expected 1372, got 1367"
        - JSON error: {'code': -32000, 'message': 'nonce too low: next nonce 1372'}
        
        Args:
            error_msg: Error message string
            
        Returns:
            Correct nonce if found, None otherwise
        """
        # Pattern 1: "next nonce 1372"
        match = re.search(r'next nonce (\d+)', error_msg, re.IGNORECASE)
        if match:
            return int(match.group(1))
        
        # Pattern 2: "expected 1372"
        match = re.search(r'expected (\d+)', error_msg, re.IGNORECASE)
        if match:
            return int(match.group(1))
        
        # Pattern 3: "state: 1372"
        match = re.search(r'state:\s*(\d+)', error_msg, re.IGNORECASE)
        if match:
            return int(match.group(1))
        
        return None
    
    async def _wait_for_receipt_with_verification(self, w3, tx_hash: str, timeout: int = 120):
        """
        Wait for transaction receipt with verification to handle unreliable RPC providers.
        
        If initial receipt shows failure (status=0), re-fetch from blockchain to verify.
        This handles false negative receipts from bad RPC providers.
        
        Also handles RPC timeouts and DataError exceptions with retries.
        
        Args:
            w3: Web3 instance
            tx_hash: Transaction hash
            timeout: Timeout in seconds
            
        Returns:
            Transaction receipt dict
            
        Raises:
            Exception: If receipt cannot be obtained after all retries
        """
        from web3.exceptions import TransactionNotFound, TimeExhausted
        
        receipt = None
        max_attempts = 5
        
        for attempt in range(max_attempts):
            try:
                # Calculate timeout with exponential backoff
                attempt_timeout = timeout * (2 ** attempt) / max_attempts
                receipt = await w3.eth.wait_for_transaction_receipt(
                    tx_hash, timeout=attempt_timeout
                )
                break
            except TransactionNotFound:
                if attempt == max_attempts - 1:
                    self._logger.error(f'Transaction {tx_hash} not found after {max_attempts} attempts')
                    raise
                wait_time = 5 * (2 ** attempt)
                self._logger.warning(
                    f'Transaction {tx_hash} not found (attempt {attempt + 1}/{max_attempts}), '
                    f'retrying in {wait_time}s...'
                )
                await asyncio.sleep(wait_time)
            except (TimeExhausted, asyncio.TimeoutError, TimeoutError) as timeout_error:
                if attempt == max_attempts - 1:
                    self._logger.error(
                        f'Transaction {tx_hash} receipt timeout after {max_attempts} attempts. '
                        f'RPC endpoint may be unreliable or slow.'
                    )
                    raise Exception(
                        f'RPC timeout waiting for receipt: {timeout_error}. '
                        f'Transaction {tx_hash} may still be pending.'
                    ) from timeout_error
                wait_time = 5 * (2 ** attempt)
                self._logger.warning(
                    f'RPC timeout waiting for receipt {tx_hash} (attempt {attempt + 1}/{max_attempts}), '
                    f'retrying in {wait_time}s...'
                )
                await asyncio.sleep(wait_time)
            except Exception as rpc_error:
                # Handle DataError and other RPC errors
                error_name = type(rpc_error).__name__
                error_msg = str(rpc_error).lower()
                
                # Check if it's a DataError or malformed response
                if 'dataerror' in error_name.lower() or 'malformed' in error_msg or 'invalid' in error_msg:
                    if attempt == max_attempts - 1:
                        self._logger.error(
                            f'RPC DataError for transaction {tx_hash} after {max_attempts} attempts. '
                            f'RPC endpoint returned malformed response: {rpc_error}'
                        )
                        raise Exception(
                            f'RPC DataError: {rpc_error}. '
                            f'RPC endpoint may be returning malformed responses.'
                        ) from rpc_error
                    wait_time = 5 * (2 ** attempt)
                    self._logger.warning(
                        f'RPC DataError for {tx_hash} (attempt {attempt + 1}/{max_attempts}), '
                        f'retrying in {wait_time}s... Error: {rpc_error}'
                    )
                    await asyncio.sleep(wait_time)
                else:
                    # Unknown error, re-raise
                    self._logger.error(f'Unexpected RPC error for {tx_hash}: {rpc_error}')
                    raise
        
        # Verify receipt if it shows failure (handle false negatives)
        if receipt and receipt['status'] == 0:
            self._logger.warning(f'Receipt shows failure for {tx_hash}, re-verifying...')
            # Re-fetch receipt to verify against false negatives
            try:
                verified_receipt = await w3.eth.get_transaction_receipt(tx_hash)
                if verified_receipt['status'] == 1:
                    self._logger.warning(f'False negative detected! Transaction {tx_hash} actually succeeded')
                    return verified_receipt
            except Exception as verify_error:
                self._logger.warning(f'Could not verify receipt: {verify_error}')
        
        return receipt
    
    async def reset_nonce(self, new_nonce: int):
        """
        Reset the nonce (call when detecting nonce errors).
        
        Args:
            new_nonce: The correct nonce value from blockchain
        """
        async with self._nonce_lock:
            old_nonce = self._current_nonce
            self._current_nonce = new_nonce
            self._logger.warning(f'Reset nonce from {old_nonce} to {new_nonce}')
    
    def _get_next_tx_id(self) -> str:
        """Generate next transaction ID."""
        self._tx_id_counter += 1
        return f'tx_{self._tx_id_counter}'
    
    async def get_status(self, tx_id: str) -> Optional[dict]:
        """
        Get the status of a transaction.
        
        Args:
            tx_id: Transaction ID
            
        Returns:
            Transaction status dict or None if not found
        """
        # Check if we have a result stored (includes receipt if available)
        if tx_id in self._tx_results:
            return self._tx_results[tx_id]
        
        # Fallback to future if available
        future = self._pending_txs.get(tx_id)
        if future is None:
            return None
        
        if future.done():
            try:
                return future.result()
            except Exception as e:
                return {'status': 'failed', 'error': str(e)}
        else:
            return {'status': 'pending'}
    
    async def _cleanup_old_results(self):
        """
        Background task to clean up old transaction results and pending futures.
        
        This prevents memory leaks by removing completed transactions from memory
        after they're no longer needed. Keeps only the most recent N transactions.
        """
        while self._processing:
            try:
                await asyncio.sleep(300)  # Run cleanup every 5 minutes
                
                # Clean up completed futures from _pending_txs
                completed_txs = [
                    tx_id for tx_id, future in self._pending_txs.items()
                    if future.done()
                ]
                for tx_id in completed_txs:
                    del self._pending_txs[tx_id]
                
                # Clean up old results from _tx_results if cache is too large
                if len(self._tx_results) > self._max_results_cache:
                    # Keep only the most recent transactions
                    # Sort by tx_id (which includes counter, so newer = higher number)
                    sorted_tx_ids = sorted(
                        self._tx_results.keys(),
                        key=lambda x: int(x.split('_')[1]) if '_' in x else 0,
                        reverse=True
                    )
                    # Remove oldest entries
                    to_remove = sorted_tx_ids[self._max_results_cache:]
                    for tx_id in to_remove:
                        del self._tx_results[tx_id]
                    
                    self._logger.debug(
                        f'Cleaned up {len(to_remove)} old transaction results. '
                        f'Cache size: {len(self._tx_results)}'
                    )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.opt(exception=True).warning(f'Error in cleanup task: {e}')
    
    async def _update_epoch_state_in_redis(
        self,
        tx_item: dict,
        tx_hash: str,
        receipt: Optional[dict],
        failed: bool = False,
        error_message: Optional[str] = None
    ):
        """
        Update epoch state in Redis for DSV monitoring integration.
        
        Writes to the same Redis key format used by DSV nodes:
        {protocol}:{market}:epoch:{epochId}:state
        
        Args:
            tx_item: Transaction item with metadata
            tx_hash: Transaction hash
            receipt: Transaction receipt (if available)
            failed: Whether transaction failed
        """
        try:
            # Extract epoch metadata from tx_item
            epoch_id = tx_item.get('epoch_id')
            data_market_address = tx_item.get('data_market_address')
            function_name = tx_item.get('function_name')
            function_args = tx_item.get('function_args')
            
            # For submitSubmissionBatch, extract from function_args if not provided
            if not epoch_id and function_name == 'submitSubmissionBatch' and function_args:
                # function_args: (dataMarketAddress, batchCID, epochID, projectIDs, snapshotCIDs, finalizedCIDsRootHash)
                if len(function_args) >= 3:
                    if not data_market_address:
                        data_market_address = function_args[0]
                    if not epoch_id:
                        epoch_id = function_args[2]
            
            # Only update Redis if we have epoch metadata
            if not epoch_id or not data_market_address:
                return
            
            # Convert addresses to checksummed format (EIP-55) for consistent Redis keys
            protocol_state_address = Web3.to_checksum_address(settings.protocol_state_address)
            data_market = Web3.to_checksum_address(data_market_address)
            epoch_id_str = str(epoch_id)
            
            # Build Redis key: {protocol}:{market}:epoch:{epochId}:state
            epoch_state_key = f"{protocol_state_address}:{data_market}:epoch:{epoch_id_str}:state"
            
            # Connect to Redis
            redis_conn = await get_writer_redis_conn()
            
            try:
                timestamp = int(time.time())
                state_updates = {
                    'last_updated': timestamp,
                }
                
                if failed:
                    state_updates['onchain_status'] = 'failed'
                    if error_message:
                        # Store error message (truncate to 500 chars to avoid huge Redis values)
                        state_updates['onchain_error'] = error_message[:500]
                elif receipt:
                    if receipt.get('status') == 1:
                        # Transaction confirmed successfully
                        state_updates['onchain_status'] = 'confirmed'
                        state_updates['onchain_tx_hash'] = tx_hash
                        state_updates['onchain_block_number'] = receipt.get('blockNumber', 0)
                        state_updates['onchain_submitted_at'] = timestamp
                    else:
                        # Transaction failed on-chain
                        state_updates['onchain_status'] = 'failed'
                        state_updates['onchain_tx_hash'] = tx_hash
                        # Try to extract error from receipt if available
                        if receipt.get('status') == 0:
                            state_updates['onchain_error'] = 'Transaction reverted on-chain (receipt.status=0)'
                else:
                    # Transaction submitted but no receipt yet
                    state_updates['onchain_status'] = 'submitted'
                    state_updates['onchain_tx_hash'] = tx_hash
                    state_updates['onchain_submitted_at'] = timestamp
                
                # Update Redis hash (HSET)
                await redis_conn.hset(epoch_state_key, mapping=state_updates)
                
                # Set TTL (7 days, same as DSV)
                await redis_conn.expire(epoch_state_key, 7 * 24 * 3600)
                
                self._logger.info(
                    f'Updated epoch state in Redis | epoch={epoch_id_str} | '
                    f'status={state_updates.get("onchain_status")} | tx_hash={tx_hash}'
                )
            finally:
                await redis_conn.close()
                
        except Exception as e:
            # Don't fail transaction processing if Redis update fails
            self._logger.warning(
                f'Failed to update epoch state in Redis: {e} | '
                f'epoch_id={epoch_id} | tx_hash={tx_hash}'
            )

