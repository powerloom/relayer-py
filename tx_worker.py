import asyncio
import random
import re
from typing import Optional

import tenacity
from aio_pika import IncomingMessage
from pydantic import ValidationError
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from web3.exceptions import TransactionNotFound
from web3.exceptions import ContractLogicError

from data_models import BatchSubmissionRequest
from data_models import EndBatchRequest
from data_models import ErrorMessage
from data_models import UpdateRewardsRequest
from data_models import UpdateSubmissionCountsRequest
from data_models import UpdateEligibleNodesRequest
from data_models import UpdateEligibleSubmissionCountsRequest
from helpers.redis_keys import end_batch_submission_called
from helpers.redis_keys import epoch_batch_size
from helpers.redis_keys import epoch_batch_submissions
from init_rabbitmq import get_tx_send_q_routing_key
from settings.conf import settings
from utils.default_logger import logger
from utils.generic_worker import GenericAsyncWorker
from utils.helpers import aiorwlock_aqcuire_release
from utils.notification_utils import send_failure_notifications
from utils.transaction_utils import write_transaction
from utils.tx_queue import TransactionQueue
from utils.tx_queue_factories import create_transaction_func


def txn_retry_callback(retry_state: tenacity.RetryCallState):
    """
    Callback function for transaction retry attempts.

    Args:
        retry_state (tenacity.RetryCallState): The current state of the retry.

    Returns:
        None
    """
    if retry_state.attempt_number >= 3:
        logger.opt(exception=True).error(
            'Submission worker failed after 3 attempts | Txn payload: {}',
            retry_state.kwargs['txn_payload'],
        )
    else:
        if retry_state.outcome.failed:
            # Use priority gas if not already set
            if 'priority_gas_multiplier' not in retry_state.kwargs:
                retry_state.kwargs['priority_gas_multiplier'] = 1
            retry_state.kwargs['priority_gas_multiplier'] += 1
            logger.info(
                'Txn failed, retrying with priority gas multiplier {} | '
                'Txn payload: {}',
                retry_state.kwargs['priority_gas_multiplier'],
                retry_state.kwargs['txn_payload'],
            )
        logger.warning(
            'Tx signing attempt number {} result {} | Txn payload: {}',
            retry_state.attempt_number,
            retry_state.outcome,
            retry_state.kwargs['txn_payload'],
        )


class TxWorker(GenericAsyncWorker):
    """
    Transaction Worker class for handling blockchain transactions.
    """

    def __init__(self, name, **kwargs):
        """
        Initialize the TxWorker.

        Args:
            name (str): The name of the worker.
            worker_idx (int): The index of the worker.
            **kwargs: Additional keyword arguments for the GenericAsyncWorker.
        """
        self._q, self._rmq_routing = get_tx_send_q_routing_key()
        super(TxWorker, self).__init__(
            name=name, **kwargs,
        )

    def _extract_error_code(self, error: Exception) -> Optional[str]:
        """
        Extract error code (E04, E23, E24, E25, E43, etc.) from exception.
        
        Args:
            error: The exception to extract error code from
            
        Returns:
            Optional[str]: Error code if found, None otherwise
        """
        error_str = str(error)
        error_repr = repr(error)
        
        # Try to extract error code pattern E## from error message
        error_code_match = re.search(r'E\d+', error_str)
        if error_code_match:
            return error_code_match.group(0)
        
        # Also check repr in case error code is in string representation
        error_code_match = re.search(r'E\d+', error_repr)
        if error_code_match:
            return error_code_match.group(0)
        
        # Check if ContractLogicError has args with error dict
        if isinstance(error, ContractLogicError) and hasattr(error, 'args') and error.args:
            for arg in error.args:
                if isinstance(arg, dict):
                    # Check for error code in dict values
                    for value in arg.values():
                        if isinstance(value, str):
                            match = re.search(r'E\d+', value)
                            if match:
                                return match.group(0)
                elif isinstance(arg, str):
                    match = re.search(r'E\d+', arg)
                    if match:
                        return match.group(0)
        
        return None

    @aiorwlock_aqcuire_release
    async def _return_and_increment_nonce(self):
        """
        Return the current nonce and increment it for the next transaction.

        Returns:
            int: The current nonce before incrementing.
        """
        nonce = self._signer_nonce
        self._signer_nonce += 1
        self._logger.info(
            'Using signer {} for submission task. Incremented nonce {}',
            self._signer_account, self._signer_nonce,
        )
        return nonce

    @aiorwlock_aqcuire_release
    async def _reset_nonce(self, value: int = 0):
        """
        Reset the nonce to a specific value or fetch the correct nonce from
        the blockchain.

        Args:
            value (int, optional): The value to reset the nonce to.
                Defaults to 0.
        """
        if value > 0:
            self._signer_nonce = value
            self._logger.info(
                'Using signer {} for submission task. Reset nonce to {}',
                self._signer_account, self._signer_nonce,
            )
        else:
            correct_nonce = await self._w3.eth.get_transaction_count(
                self._signer_account,
            )
            if correct_nonce and isinstance(correct_nonce, int):
                self._signer_nonce = correct_nonce
                self._logger.info(
                    'Using signer {} for submission task. Reset nonce to {}',
                    self._signer_account, self._signer_nonce,
                )
            else:
                self._logger.error(
                    'Using signer {} for submission task. '
                    'Could not reset nonce',
                    self._signer_account,
                )

    @retry(
        reraise=True,
        retry=retry_if_exception_type(Exception),
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(10),
        after=txn_retry_callback,
    )
    async def submit_batch(
        self,
        txn_payload: BatchSubmissionRequest,
        priority_gas_multiplier: int = 0,
    ):
        """
        Submit a batch of transactions to the blockchain using transaction queue.

        Args:
            txn_payload (BatchSubmissionRequest): The payload containing batch
                submission data.
            priority_gas_multiplier (int, optional): Gas price multiplier for
                priority. Defaults to 0.

        Returns:
            str: The transaction hash if successful.

        Raises:
            Exception: If the transaction fails or encounters a nonce error.
        """
        protocol_state_contract = await self.get_protocol_state_contract(
            settings.protocol_state_address,
        )
        
        # Check for contract errors BEFORE submitting to queue
        try:
            _ = await self._protocol_state_contract.functions.\
                submitSubmissionBatch(
                    txn_payload.dataMarketAddress,
                    txn_payload.batchCID,
                    txn_payload.epochID,
                    list(txn_payload.projectIDs),
                    list(txn_payload.snapshotCIDs),
                    txn_payload.finalizedCIDsRootHash,
                ).estimate_gas({'from': self._signer_account})
        except ContractLogicError as gas_error:
            # Extract error code from ContractLogicError
            error_code = self._extract_error_code(gas_error)
            
            # Log detailed error information WITHOUT full payload (reduces verbosity)
            self._logger.error(
                'Gas estimation failed with ContractLogicError (contract would revert) | '
                'Error code: {} | batchCID={} | epochID={} | '
                'dataMarket={} | projectIDs={} | projectIDs_count={} | snapshotCIDs_count={} | signer={}',
                error_code or 'UNKNOWN',
                txn_payload.batchCID,
                txn_payload.epochID,
                txn_payload.dataMarketAddress,
                list(txn_payload.projectIDs),
                len(txn_payload.projectIDs),
                len(txn_payload.snapshotCIDs),
                self._signer_account,
            )
            
            # Handle specific error codes that are expected/skippable
            if error_code == 'E25':
                self._logger.info(
                    'Snapshot batch already submitted (E25) | batchCID={} | epochID={} | skipping',
                    txn_payload.batchCID,
                    txn_payload.epochID,
                )
                return ''
            elif error_code == 'E43':
                self._logger.info(
                    'Batch submissions already completed for epoch (E43) | epochID={} | skipping',
                    txn_payload.epochID,
                )
                return ''
            elif error_code == 'E04':
                self._logger.error(
                    'Signer {} is not registered as sequencer (E04) | batchCID={} | epochID={} | '
                    'Please add signer to sequencer set on contract.',
                    self._signer_account,
                    txn_payload.batchCID,
                    txn_payload.epochID,
                )
                raise Exception(
                    f'Signer {self._signer_account} is not a sequencer (E04). '
                    f'Add signer to sequencer set on contract.'
                ) from gas_error
            elif error_code == 'E23':
                self._logger.error(
                    'Project IDs and snapshot CIDs length mismatch (E23) | epochID={} | '
                    'projectIDs_count={} | snapshotCIDs_count={}',
                    txn_payload.epochID,
                    len(txn_payload.projectIDs),
                    len(txn_payload.snapshotCIDs),
                )
                raise Exception(
                    f'Project IDs ({len(txn_payload.projectIDs)}) and snapshot CIDs '
                    f'({len(txn_payload.snapshotCIDs)}) length mismatch (E23)'
                ) from gas_error
            elif error_code == 'E24':
                self._logger.error(
                    'Project IDs and snapshot CIDs cannot be empty (E24) | epochID={}',
                    txn_payload.epochID,
                )
                raise Exception('Project IDs and snapshot CIDs cannot be empty (E24)') from gas_error
            
            # For unknown error codes, raise with details
            raise Exception(
                f'Contract logic error during gas estimation (Error code: {error_code or "UNKNOWN"}). '
                f'batchCID={txn_payload.batchCID}, epochID={txn_payload.epochID}. '
                f'Transaction would revert on-chain.'
            ) from gas_error
        
        # Create transaction function for queue
        tx_func = create_transaction_func(
            self._w3,
            self._signer_account,
            self._signer_pkey,
            protocol_state_contract,
            'submitSubmissionBatch',
            self._last_gas_price,
            priority_gas_multiplier,
            txn_payload.dataMarketAddress,
            txn_payload.batchCID,
            txn_payload.epochID,
            list(txn_payload.projectIDs),
            list(txn_payload.snapshotCIDs),
            txn_payload.finalizedCIDsRootHash,
        )
        
        # Submit transaction to queue (fire-and-forget mode)
        # Returns tx_id immediately, receipt confirmation happens in background
        tx_id = await self.tx_queue.submit_transaction(
            tx_func,
            w3=self._w3,
            contract=self._protocol_state_contract,
            function_name='submitSubmissionBatch',
            signer_address=self._signer_account,
            function_args=(
                txn_payload.dataMarketAddress,
                txn_payload.batchCID,
                txn_payload.epochID,
                list(txn_payload.projectIDs),
                list(txn_payload.snapshotCIDs),
                txn_payload.finalizedCIDsRootHash,
            ),
            epoch_id=txn_payload.epochID,
            data_market_address=txn_payload.dataMarketAddress,
        )
        
        self._logger.info(
            'Queued batch submission transaction | '
            'tx_id={} | batchCID={} | epochID={} | '
            'dataMarket={} | projectIDs={} | projectIDs_count={} | snapshotCIDs_count={}',
            tx_id,
            txn_payload.batchCID,
            txn_payload.epochID,
            txn_payload.dataMarketAddress,
            list(txn_payload.projectIDs),
            len(txn_payload.projectIDs),
            len(txn_payload.snapshotCIDs),
        )
        
        # Fire-and-forget: return tx_id immediately
        # Transaction hash and receipt will be available via tx_queue.get_status(tx_id) later
        # For Redis tracking, we can use tx_id or poll for tx_hash
        return tx_id

    @retry(
        reraise=True,
        retry=retry_if_exception_type(Exception),
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(10),
        after=txn_retry_callback,
    )
    async def submit_update_rewards(
        self,
        txn_payload: UpdateRewardsRequest,
        priority_gas_multiplier: int = 0,
    ):
        """
        Submit update rewards transaction using transaction queue.

        Args:
            txn_payload (UpdateRewardsRequest): The payload containing update
                rewards data.
            priority_gas_multiplier (int, optional): Gas price multiplier for
                priority. Defaults to 0.

        Returns:
            str: The transaction hash if successful.

        Raises:
            Exception: If the transaction fails or encounters a nonce error.
        """
        protocol_state_contract = await self.get_protocol_state_contract(
            settings.protocol_state_address,
        )
        
        # Check for contract errors BEFORE submitting to queue
        try:
            _ = await self._protocol_state_contract.functions.\
                updateRewards(
                    txn_payload.dataMarketAddress,
                    txn_payload.slotIDs,
                    txn_payload.submissionsList,
                    txn_payload.day,
                    txn_payload.eligibleNodes,
                ).estimate_gas({'from': self._signer_account})
        except ContractLogicError as gas_error:
            error_code = self._extract_error_code(gas_error)
            self._logger.error(
                'Gas estimation failed for update rewards with ContractLogicError | '
                'Error: {} | Error code: {}',
                gas_error,
                error_code or 'UNKNOWN',
            )
            if error_code in ('E47', 'E48'):
                self._logger.info(
                    'Update rewards already called ({}). Skipping...',
                    error_code,
                )
                return ''
            raise Exception(
                f'Contract logic error during gas estimation: {gas_error} '
                f'(Error code: {error_code or "UNKNOWN"})'
            ) from gas_error
        
        # Create transaction function for queue
        tx_func = create_transaction_func(
            self._w3,
            self._signer_account,
            self._signer_pkey,
            protocol_state_contract,
            'updateRewards',
            self._last_gas_price,
            priority_gas_multiplier,
            txn_payload.dataMarketAddress,
            txn_payload.slotIDs,
            txn_payload.submissionsList,
            txn_payload.day,
            txn_payload.eligibleNodes,
        )
        
        # Submit transaction to queue (fire-and-forget mode)
        tx_id = await self.tx_queue.submit_transaction(
            tx_func,
            w3=self._w3,
            contract=self._protocol_state_contract,
            function_name='updateRewards',
            signer_address=self._signer_account,
            function_args=(
                txn_payload.dataMarketAddress,
                txn_payload.slotIDs,
                txn_payload.submissionsList,
                txn_payload.day,
                txn_payload.eligibleNodes,
            ),
        )
        
        self._logger.info(
            f'Queued update rewards transaction {tx_id}',
        )
        
        # Fire-and-forget: return tx_id immediately
        return tx_id

    @retry(
        reraise=True,
        retry=retry_if_exception_type(Exception),
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(10),
        after=txn_retry_callback,
    )
    async def submit_update_submission_counts(
        self,
        txn_payload: UpdateSubmissionCountsRequest,
        priority_gas_multiplier: int = 0,
    ):
        """
        Submit update submission counts transaction (periodic updates, no rewards).

        Args:
            txn_payload (UpdateSubmissionCountsRequest): The payload containing submission count update data.
            priority_gas_multiplier (int, optional): Gas price multiplier for priority. Defaults to 0.

        Returns:
            str: The transaction ID if successful.
        """
        protocol_state_contract = await self.get_protocol_state_contract(
            settings.protocol_state_address,
        )
        
        try:
            _ = await self._protocol_state_contract.functions.\
                updateSubmissionCounts(
                    txn_payload.dataMarketAddress,
                    txn_payload.slotIDs,
                    txn_payload.submissionsList,
                    txn_payload.day,
                ).estimate_gas({'from': self._signer_account})
        except ContractLogicError as gas_error:
            error_code = self._extract_error_code(gas_error)
            self._logger.error(
                'Gas estimation failed for update submission counts with ContractLogicError | '
                'Error: {} | Error code: {}',
                gas_error,
                error_code or 'UNKNOWN',
            )
            raise Exception(
                f'Contract logic error during gas estimation: {gas_error} '
                f'(Error code: {error_code or "UNKNOWN"})'
            ) from gas_error
        
        tx_func = create_transaction_func(
            self._w3,
            self._signer_account,
            self._signer_pkey,
            protocol_state_contract,
            'updateSubmissionCounts',
            self._last_gas_price,
            priority_gas_multiplier,
            txn_payload.dataMarketAddress,
            txn_payload.slotIDs,
            txn_payload.submissionsList,
            txn_payload.day,
        )
        
        tx_id = await self.tx_queue.submit_transaction(
            tx_func,
            w3=self._w3,
            contract=self._protocol_state_contract,
            function_name='updateSubmissionCounts',
            signer_address=self._signer_account,
            function_args=(
                txn_payload.dataMarketAddress,
                txn_payload.slotIDs,
                txn_payload.submissionsList,
                txn_payload.day,
            ),
        )
        
        self._logger.info(f'Queued update submission counts transaction {tx_id}')
        return tx_id

    @retry(
        reraise=True,
        retry=retry_if_exception_type(Exception),
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(10),
        after=txn_retry_callback,
    )
    async def submit_update_eligible_nodes(
        self,
        txn_payload: UpdateEligibleNodesRequest,
        priority_gas_multiplier: int = 0,
    ):
        """
        Submit update eligible nodes transaction (Step 1 of end-of-day update).

        Args:
            txn_payload (UpdateEligibleNodesRequest): The payload containing eligible nodes update data.
            priority_gas_multiplier (int, optional): Gas price multiplier for priority. Defaults to 0.

        Returns:
            str: The transaction ID if successful.
        """
        protocol_state_contract = await self.get_protocol_state_contract(
            settings.protocol_state_address,
        )
        
        try:
            _ = await self._protocol_state_contract.functions.\
                updateEligibleNodesForDay(
                    txn_payload.dataMarketAddress,
                    txn_payload.day,
                    txn_payload.eligibleNodes,
                ).estimate_gas({'from': self._signer_account})
        except ContractLogicError as gas_error:
            error_code = self._extract_error_code(gas_error)
            self._logger.error(
                'Gas estimation failed for update eligible nodes with ContractLogicError | '
                'Error: {} | Error code: {}',
                gas_error,
                error_code or 'UNKNOWN',
            )
            raise Exception(
                f'Contract logic error during gas estimation: {gas_error} '
                f'(Error code: {error_code or "UNKNOWN"})'
            ) from gas_error
        
        tx_func = create_transaction_func(
            self._w3,
            self._signer_account,
            self._signer_pkey,
            protocol_state_contract,
            'updateEligibleNodesForDay',
            self._last_gas_price,
            priority_gas_multiplier,
            txn_payload.dataMarketAddress,
            txn_payload.day,
            txn_payload.eligibleNodes,
        )
        
        tx_id = await self.tx_queue.submit_transaction(
            tx_func,
            w3=self._w3,
            contract=self._protocol_state_contract,
            function_name='updateEligibleNodesForDay',
            signer_address=self._signer_account,
            function_args=(
                txn_payload.dataMarketAddress,
                txn_payload.day,
                txn_payload.eligibleNodes,
            ),
        )
        
        self._logger.info(f'Queued update eligible nodes transaction {tx_id}')
        return tx_id

    @retry(
        reraise=True,
        retry=retry_if_exception_type(Exception),
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(10),
        after=txn_retry_callback,
    )
    async def submit_update_eligible_submission_counts(
        self,
        txn_payload: UpdateEligibleSubmissionCountsRequest,
        priority_gas_multiplier: int = 0,
    ):
        """
        Submit update eligible submission counts transaction (Step 2 of end-of-day update).

        Args:
            txn_payload (UpdateEligibleSubmissionCountsRequest): The payload containing eligible submission counts update data.
            priority_gas_multiplier (int, optional): Gas price multiplier for priority. Defaults to 0.

        Returns:
            str: The transaction ID if successful.
        """
        protocol_state_contract = await self.get_protocol_state_contract(
            settings.protocol_state_address,
        )
        
        try:
            _ = await self._protocol_state_contract.functions.\
                updateEligibleSubmissionCountsForDay(
                    txn_payload.dataMarketAddress,
                    txn_payload.slotIDs,
                    txn_payload.submissionsList,
                    txn_payload.day,
                ).estimate_gas({'from': self._signer_account})
        except ContractLogicError as gas_error:
            error_code = self._extract_error_code(gas_error)
            self._logger.error(
                'Gas estimation failed for update eligible submission counts with ContractLogicError | '
                'Error: {} | Error code: {}',
                gas_error,
                error_code or 'UNKNOWN',
            )
            raise Exception(
                f'Contract logic error during gas estimation: {gas_error} '
                f'(Error code: {error_code or "UNKNOWN"})'
            ) from gas_error
        
        tx_func = create_transaction_func(
            self._w3,
            self._signer_account,
            self._signer_pkey,
            protocol_state_contract,
            'updateEligibleSubmissionCountsForDay',
            self._last_gas_price,
            priority_gas_multiplier,
            txn_payload.dataMarketAddress,
            txn_payload.slotIDs,
            txn_payload.submissionsList,
            txn_payload.day,
        )
        
        tx_id = await self.tx_queue.submit_transaction(
            tx_func,
            w3=self._w3,
            contract=self._protocol_state_contract,
            function_name='updateEligibleSubmissionCountsForDay',
            signer_address=self._signer_account,
            function_args=(
                txn_payload.dataMarketAddress,
                txn_payload.slotIDs,
                txn_payload.submissionsList,
                txn_payload.day,
            ),
        )
        
        self._logger.info(f'Queued update eligible submission counts transaction {tx_id}')
        return tx_id

    @retry(
        reraise=True,
        retry=retry_if_exception_type(Exception),
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(10),
        after=txn_retry_callback,
    )
    async def end_batch(
        self,
        txn_payload: EndBatchRequest,
        priority_gas_multiplier: int = 0,
    ):
        """
        End a batch submission on the blockchain.

        Args:
            data_market (str): The address of the data market.
            epoch_id (int): The ID of the epoch.
            priority_gas_multiplier (int, optional): Gas price multiplier for
                priority. Defaults to 0.

        Returns:
            str: The transaction hash if successful.

        Raises:
            Exception: If the transaction fails or encounters a nonce error.
        """
        # Check for contract errors BEFORE submitting to queue
        try:
            _ = await self._protocol_state_contract.functions.\
                endBatchSubmissions(
                    txn_payload.dataMarketAddress, txn_payload.epochID,
                ).estimate_gas({'from': self._signer_account})
        except ContractLogicError as gas_error:
            error_code = self._extract_error_code(gas_error)
            self._logger.error(
                'Gas estimation failed for end batch submission with ContractLogicError | '
                'Error: {} | Error code: {} | '
                'dataMarket={}, epochID={}, signer={}',
                gas_error,
                error_code or 'UNKNOWN',
                txn_payload.dataMarketAddress,
                txn_payload.epochID,
                self._signer_account,
            )
            if error_code == 'E39':
                self._logger.info(
                    'End batch submission already called (E39). Skipping...',
                )
                return ''
            elif error_code == 'E04':
                self._logger.error(
                    'Signer {} is not registered as sequencer (E04). '
                    'Please add signer to sequencer set on contract.',
                    self._signer_account,
                )
                raise Exception(
                    f'Signer {self._signer_account} is not a sequencer (E04). '
                    f'Add signer to sequencer set on contract.'
                ) from gas_error
            else:
                raise Exception(
                    f'Contract logic error during gas estimation: {gas_error} '
                    f'(Error code: {error_code or "UNKNOWN"})'
                ) from gas_error
        except Exception as e:
            # Also check error string for E39 (backwards compatibility)
            if 'E39' in str(e):
                self._logger.info(
                    'End batch submission already called. Skipping...',
                )
                return ''
            else:
                self._logger.opt(exception=True).error(
                    'Error estimating gas for end batch submission. Error: {}',
                    e,
                )
                raise e

        # Check Redis cache
        if await self.reader_redis_pool.get(
            end_batch_submission_called(
                txn_payload.dataMarketAddress, txn_payload.epochID,
            ),
        ):
            self._logger.info(
                f'End batch submission already called for epoch '
                f'{txn_payload.epochID}. Skipping...',
            )
            return ''
        
        protocol_state_contract = await self.get_protocol_state_contract(
            settings.protocol_state_address,
        )
        
        # Create transaction function for queue
        tx_func = create_transaction_func(
            self._w3,
            self._signer_account,
            self._signer_pkey,
            protocol_state_contract,
            'endBatchSubmissions',
            self._last_gas_price,
            priority_gas_multiplier,
            txn_payload.dataMarketAddress,
            txn_payload.epochID,
        )
        
        # Submit transaction to queue (fire-and-forget mode)
        tx_id = await self.tx_queue.submit_transaction(
            tx_func,
            w3=self._w3,
            contract=self._protocol_state_contract,
            function_name='endBatchSubmissions',
            signer_address=self._signer_account,
            function_args=(
                txn_payload.dataMarketAddress,
                txn_payload.epochID,
            ),
        )
        
        self._logger.info(
            f'Queued end batch transaction {tx_id} for epochID: {txn_payload.epochID}',
        )
        
        # Mark as called in Redis (optimistically, before receipt confirmation)
        # Receipt confirmation happens in background
        await self.writer_redis_pool.set(
            end_batch_submission_called(
                txn_payload.dataMarketAddress, txn_payload.epochID,
            ),
            '1',  # Redis requires string/int/bytes, not bool
            ex=3600,
        )
        
        # Fire-and-forget: return tx_id immediately
        return tx_id

    async def _on_rabbitmq_message(self, message: IncomingMessage):
        """
        Process incoming RabbitMQ messages and handle batch submissions and
        reward updates.

        This method processes two types of messages:
        1. BatchSubmissionRequest - For submitting batches of snapshots
        2. UpdateRewardsRequest - For updating reward distributions

        For BatchSubmissionRequest messages, it:
        - Submits the batch transaction
        - Tracks submission counts against batch size
        - Triggers end batch when size threshold is reached

        For UpdateRewardsRequest messages, it:
        - Submits the reward update transaction directly

        Args:
            message (IncomingMessage): The incoming RabbitMQ message containing
                either a BatchSubmissionRequest or UpdateRewardsRequest payload

        Returns:
            None
        """
        # Acknowledge message receipt and initialize
        await message.ack()
        await self.init()

        try:
            # Parse message payload - try different request types in order
            msg_obj = None
            try:
                msg_obj = BatchSubmissionRequest.parse_raw(message.body)
                self._logger.debug('Parsed message as BatchSubmissionRequest')
            except ValidationError:
                try:
                    msg_obj = UpdateRewardsRequest.parse_raw(message.body)
                    self._logger.debug('Parsed message as UpdateRewardsRequest')
                except ValidationError:
                    try:
                        msg_obj = UpdateSubmissionCountsRequest.parse_raw(message.body)
                        self._logger.debug('Parsed message as UpdateSubmissionCountsRequest')
                    except ValidationError:
                        try:
                            msg_obj = UpdateEligibleNodesRequest.parse_raw(message.body)
                            self._logger.debug('Parsed message as UpdateEligibleNodesRequest')
                        except ValidationError:
                            msg_obj = UpdateEligibleSubmissionCountsRequest.parse_raw(message.body)
                            self._logger.debug('Parsed message as UpdateEligibleSubmissionCountsRequest')
            
            if msg_obj is None:
                raise ValidationError('Could not parse message as any known request type')

        except ValidationError as e:
            # Log validation errors with message details
            self._logger.opt(exception=False).error(
                'Bad message structure of callback processor. Error: {}, {}',
                e, message.body,
            )
            return
        except Exception as e:
            # Log unexpected parsing errors
            self._logger.opt(exception=False).error(
                'Unexpected message structure of callback in processor. '
                'Error: {}', e,
            )
            return
        else:
            try:
                if isinstance(msg_obj, BatchSubmissionRequest):
                    # Handle batch submission request
                    self._logger.info(
                        'Processing batch submission request | '
                        'batchCID={} | epochID={} | dataMarket={} | '
                        'projectIDs={} | projectIDs_count={} | snapshotCIDs_count={}',
                        msg_obj.batchCID,
                        msg_obj.epochID,
                        msg_obj.dataMarketAddress,
                        list(msg_obj.projectIDs),
                        len(msg_obj.projectIDs),
                        len(msg_obj.snapshotCIDs),
                    )
                    
                    tx_id = await self.submit_batch(txn_payload=msg_obj)
                    if tx_id != '':
                        # Get configured batch size for this epoch
                        batch_size = await self.writer_redis_pool.get(
                            epoch_batch_size(msg_obj.epochID),
                        )
                        
                        if batch_size:
                            batch_size_int = int(batch_size)
                            self._logger.info(
                                'Batch tracking | epochID={} | configured_batch_size={}',
                                msg_obj.epochID,
                                batch_size_int,
                            )
                            
                            # Track this submission by tx_id (fire-and-forget mode)
                            # tx_hash will be available later via tx_queue.get_status(tx_id)
                            submissions_key = epoch_batch_submissions(msg_obj.epochID)
                            await self.writer_redis_pool.sadd(
                                submissions_key,
                                tx_id,  # Use tx_id instead of tx_hash for tracking
                            )
                            # Set TTL on the SET key (24 hours - covers epoch lifecycle)
                            # Only set TTL if key doesn't already have one (to avoid resetting expiry)
                            ttl = await self.writer_redis_pool.ttl(submissions_key)
                            if ttl == -1:  # Key exists but has no TTL
                                await self.writer_redis_pool.expire(submissions_key, 86400)  # 24 hours
                            
                            # Get current submission count
                            set_size = await self.writer_redis_pool.scard(
                                epoch_batch_submissions(msg_obj.epochID),
                            )
                            set_size_int = int(set_size)
                            
                            self._logger.info(
                                'Batch submission tracked | epochID={} | tx_id={} | '
                                'current_submissions={} | required_batch_size={} | '
                                'remaining={}',
                                msg_obj.epochID,
                                tx_id,
                                set_size_int,
                                batch_size_int,
                                max(0, batch_size_int - set_size_int),
                            )
                            
                            # End batch if size threshold reached
                            if set_size_int >= batch_size_int:
                                self._logger.info(
                                    'Batch size threshold reached | epochID={} | '
                                    'submissions={} | threshold={} | triggering end_batch',
                                    msg_obj.epochID,
                                    set_size_int,
                                    batch_size_int,
                                )
                                txn_payload = EndBatchRequest(
                                    dataMarketAddress=msg_obj.dataMarketAddress,
                                    epochID=msg_obj.epochID,
                                )
                                await self.end_batch(txn_payload=txn_payload)
                        else:
                            self._logger.debug(
                                'No batch size configured for epochID={}, skipping batch tracking',
                                msg_obj.epochID,
                            )
                elif isinstance(msg_obj, UpdateRewardsRequest):
                    # Handle reward update request (deprecated, kept for backward compatibility)
                    tx_id = await self.submit_update_rewards(
                        txn_payload=msg_obj,
                    )
                elif isinstance(msg_obj, UpdateSubmissionCountsRequest):
                    # Handle periodic submission count update (no rewards)
                    tx_id = await self.submit_update_submission_counts(
                        txn_payload=msg_obj,
                    )
                elif isinstance(msg_obj, UpdateEligibleNodesRequest):
                    # Handle eligible nodes update (Step 1 of end-of-day)
                    tx_id = await self.submit_update_eligible_nodes(
                        txn_payload=msg_obj,
                    )
                elif isinstance(msg_obj, UpdateEligibleSubmissionCountsRequest):
                    # Handle eligible submission counts update (Step 2 of end-of-day)
                    tx_id = await self.submit_update_eligible_submission_counts(
                        txn_payload=msg_obj,
                    )
                else:
                    self._logger.error(
                        'Unknown message type: {}',
                        type(msg_obj).__name__,
                    )
            except Exception as e:
                # Log error without full exception traceback to reduce verbosity
                # Include key identifiers for debugging
                error_summary = str(e)[:500]  # Limit error message length
                if isinstance(msg_obj, BatchSubmissionRequest):
                    self._logger.error(
                        'Error submitting batch | batchCID={} | epochID={} | '
                        'dataMarket={} | projectIDs={} | error={}',
                        msg_obj.batchCID,
                        msg_obj.epochID,
                        msg_obj.dataMarketAddress,
                        list(msg_obj.projectIDs),
                        error_summary,
                    )
                elif isinstance(msg_obj, UpdateRewardsRequest):
                    self._logger.error(
                        'Error submitting update rewards | dataMarket={} | day={} | '
                        'slotIDs_count={} | error={}',
                        msg_obj.dataMarketAddress,
                        msg_obj.day,
                        len(msg_obj.slotIDs),
                        error_summary,
                    )
                elif isinstance(msg_obj, UpdateSubmissionCountsRequest):
                    self._logger.error(
                        'Error submitting update submission counts | dataMarket={} | day={} | '
                        'slotIDs_count={} | error={}',
                        msg_obj.dataMarketAddress,
                        msg_obj.day,
                        len(msg_obj.slotIDs),
                        error_summary,
                    )
                elif isinstance(msg_obj, UpdateEligibleNodesRequest):
                    self._logger.error(
                        'Error submitting update eligible nodes | dataMarket={} | day={} | '
                        'eligibleNodes={} | error={}',
                        msg_obj.dataMarketAddress,
                        msg_obj.day,
                        msg_obj.eligibleNodes,
                        error_summary,
                    )
                elif isinstance(msg_obj, UpdateEligibleSubmissionCountsRequest):
                    self._logger.error(
                        'Error submitting update eligible submission counts | dataMarket={} | day={} | '
                        'slotIDs_count={} | error={}',
                        msg_obj.dataMarketAddress,
                        msg_obj.day,
                        len(msg_obj.slotIDs),
                        error_summary,
                    )
                else:
                    self._logger.error(
                        'Error submitting transaction | error={}',
                        error_summary,
                    )
                
                # Log full exception details at DEBUG level for deep debugging
                self._logger.opt(exception=True).debug(
                    'Full exception details for error: {}',
                    error_summary,
                )
                
                error_message = ErrorMessage(
                    error=str(e),
                    raw_payload=str(msg_obj.dict()),
                )
                # Only send notifications if http_client is initialized
                if hasattr(self, '_http_client') and self._http_client:
                    await send_failure_notifications(
                        client=self._http_client,
                        message=error_message,
                    )
                else:
                    self._logger.warning(
                        'Cannot send failure notifications: http_client not initialized',
                    )


if __name__ == '__main__':
    tx_worker = TxWorker(name='tx_worker')
    tx_worker.run()
