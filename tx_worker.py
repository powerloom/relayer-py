import asyncio

import tenacity
from aio_pika import IncomingMessage
from pydantic import ValidationError
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from web3.exceptions import TransactionNotFound

from data_models import BatchSubmissionRequest
from helpers.redis_keys import epoch_batch_size
from helpers.redis_keys import epoch_batch_submissions
from init_rabbitmq import get_tx_send_q_routing_key
from settings.conf import settings
from utils.default_logger import logger
from utils.generic_worker import GenericAsyncWorker
from utils.helpers import aiorwlock_aqcuire_release
from utils.transaction_utils import write_transaction


def txn_retry_callback(retry_state: tenacity.RetryCallState):
    """
    Callback function for transaction retry attempts.

    Args:
        retry_state (tenacity.RetryCallState): The current state of the retry.

    Returns:
        None
    """
    if retry_state.attempt_number >= 3:
        logger.error(
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

    def __init__(self, name, worker_idx, **kwargs):
        """
        Initialize the TxWorker.

        Args:
            name (str): The name of the worker.
            worker_idx (int): The index of the worker.
            **kwargs: Additional keyword arguments for the GenericAsyncWorker.
        """
        self._q, self._rmq_routing = get_tx_send_q_routing_key()
        super(TxWorker, self).__init__(
            name=name, worker_idx=worker_idx, **kwargs,
        )

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
        Reset the nonce to a specific value or fetch the correct nonce from the blockchain.

        Args:
            value (int, optional): The value to reset the nonce to. Defaults to 0.
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
                    'Using signer {} for submission task. Could not reset nonce',
                    self._signer_account,
                )

    @retry(
        reraise=True,
        retry=retry_if_exception_type(Exception),
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        after=txn_retry_callback,
    )
    async def submit_batch(self, txn_payload: BatchSubmissionRequest, priority_gas_multiplier: int = 0):
        """
        Submit a batch of transactions to the blockchain.

        Args:
            txn_payload (BatchSubmissionRequest): The payload containing batch submission data.
            priority_gas_multiplier (int, optional): Gas price multiplier for priority. Defaults to 0.

        Returns:
            str: The transaction hash if successful.

        Raises:
            Exception: If the transaction fails or encounters a nonce error.
        """
        _nonce = await self._return_and_increment_nonce()
        protocol_state_contract = await self.get_protocol_state_contract(settings.protocol_state_address)
        self._logger.trace(f'nonce: {_nonce}')
        try:
            tx_hash = await write_transaction(
                self._w3,
                self._signer_account,
                self._signer_pkey,
                protocol_state_contract,
                'submitSubmissionBatch',
                _nonce,
                self._last_gas_price,
                priority_gas_multiplier,
                txn_payload.dataMarket,
                txn_payload.batchCid,
                txn_payload.epochId,
                txn_payload.projectIds,
                txn_payload.snapshotCids,
                txn_payload.finalizedCidsRootHash,
            )

            self._logger.info(
                f'Submitted transaction with tx_hash: {tx_hash}, payload {txn_payload}',
            )

            # Wait for transaction receipt with exponential backoff
            receipt = None
            for attempt in range(5):
                try:
                    receipt = await self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30 * (2 ** attempt))
                    break
                except TransactionNotFound:
                    if attempt == 4:
                        raise
                    await asyncio.sleep(5 * (2 ** attempt))

            if 'baseFeePerGas' in receipt:
                self._last_gas_price = receipt['baseFeePerGas']
            if receipt.status != 1:
                raise Exception('Transaction failed')

        except Exception as e:
            # Handle nonce errors
            if 'nonce too low' in str(e):
                error = eval(str(e))
                message = error['message']
                next_nonce = int(message.split('next nonce ')[1].split(',')[0])
                self._logger.info(
                    'Nonce too low error. Next nonce: {}', next_nonce,
                )
                await self._reset_nonce(next_nonce)
                raise Exception('nonce error, reset nonce')
            else:
                self._logger.info(
                    'Error submitting snapshot. Retrying...',
                )
                await self._reset_nonce()
                raise e
        else:
            return tx_hash

    @retry(
        reraise=True,
        retry=retry_if_exception_type(Exception),
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        after=txn_retry_callback,
    )
    async def end_batch(self, data_market: str, epoch_id: int, priority_gas_multiplier: int = 0):
        """
        End a batch submission on the blockchain.

        Args:
            data_market (str): The address of the data market.
            epoch_id (int): The ID of the epoch.
            priority_gas_multiplier (int, optional): Gas price multiplier for priority. Defaults to 0.

        Returns:
            str: The transaction hash if successful.

        Raises:
            Exception: If the transaction fails or encounters a nonce error.
        """
        try:
            _ = await self._protocol_state_contract.functions.endBatchSubmissions(
                data_market, epoch_id,
            ).estimate_gas({'from': settings.signers[0].address})
        except Exception as e:
            if 'E39' in str(e):
                self._logger.info(
                    'End batch submission already called. Skipping...',
                )
                return
            else:
                self._logger.opt(exception=True).error(
                    'Error estimating gas for end batch submission. Error: {}',
                    e,
                )
                raise e
        _nonce = await self._return_and_increment_nonce()
        protocol_state_contract = await self.get_protocol_state_contract(settings.protocol_state_address)
        self._logger.trace(f'nonce: {_nonce}')
        try:
            # Attempt to end the batch submission
            tx_hash = await write_transaction(
                self._w3,
                self._signer_account,
                self._signer_pkey,
                protocol_state_contract,
                'endBatchSubmissions',
                _nonce,
                self._last_gas_price,
                priority_gas_multiplier,
                data_market,
                epoch_id,
            )

            self._logger.info(
                f'submitted batch end transaction with tx_hash: {tx_hash}, '
                f'data_market: {data_market}, epoch_id: {epoch_id}',
            )

            # Wait for transaction receipt and update gas price
            transaction_receipt = await self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if 'baseFeePerGas' in transaction_receipt:
                self._last_gas_price = transaction_receipt['baseFeePerGas']

        except Exception as e:
            # Handle nonce errors
            if 'nonce too low' in str(e):
                error = eval(str(e))
                message = error['message']
                next_nonce = int(message.split('next nonce ')[1].split(',')[0])
                self._logger.info(
                    'Nonce too low error. Next nonce: {}', next_nonce,
                )
                await self._reset_nonce(next_nonce)
                raise Exception('nonce error, reset nonce')
            else:
                self._logger.info(
                    'Error submitting batch end. Retrying...',
                )
                await self._reset_nonce()
                raise e
        else:
            return tx_hash

    async def _on_rabbitmq_message(self, message: IncomingMessage):
        """
        Process incoming RabbitMQ messages.

        This method is called when a message is received from RabbitMQ.
        It processes the message and initiates the batch submission process.

        Args:
            message (IncomingMessage): The incoming message from RabbitMQ.

        Returns:
            None
        """
        await message.ack()
        await self.init()
        try:
            # Parse the incoming message
            msg_obj: BatchSubmissionRequest = BatchSubmissionRequest.parse_raw(
                message.body,
            )
        except ValidationError as e:
            self._logger.opt(exception=False).error(
                'Bad message structure of callback processor. Error: {}, {}',
                e, message.body,
            )
            return
        except Exception as e:
            self._logger.opt(exception=False).error(
                'Unexpected message structure of callback in processor. Error: {}',
                e,
            )
            return
        else:
            tx_hash = await self.submit_batch(txn_payload=msg_obj)

            # Implement a more robust checking mechanism
            max_attempts = 3
            for attempt in range(max_attempts):
                batch_size = await self.writer_redis_pool.get(epoch_batch_size(msg_obj.epochId))
                if batch_size:
                    await self.writer_redis_pool.sadd(epoch_batch_submissions(msg_obj.epochId), tx_hash)
                    set_size = await self.writer_redis_pool.scard(epoch_batch_submissions(msg_obj.epochId))
                    if int(set_size) >= int(batch_size):
                        await self.end_batch(data_market=msg_obj.dataMarket, epoch_id=msg_obj.epochId)
                        break
                if attempt < max_attempts - 1:
                    await asyncio.sleep(5)  # Wait before next attempt
