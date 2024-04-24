import asyncio
import time

import tenacity
from aio_pika import IncomingMessage
from pydantic import ValidationError
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential

from data_models import TxnPayload
from init_rabbitmq import get_tx_send_q_routing_key
from utils.default_logger import logger
from utils.generic_worker import GenericAsyncWorker
from utils.helpers import aiorwlock_aqcuire_release
from utils.transaction_utils import write_transaction


def submit_snapshot_retry_callback(retry_state: tenacity.RetryCallState):
    if retry_state.attempt_number >= 3:
        logger.error(
            'Txn signing worker failed after 3 attempts | Txn payload: {}', retry_state.kwargs[
                'txn_payload'
            ],
        )
    else:
        if retry_state.outcome.failed:
            # use priority gas too
            if 'priority_gas_multiplier' not in retry_state.kwargs:
                retry_state.kwargs['priority_gas_multiplier'] = 1
            retry_state.kwargs['priority_gas_multiplier'] += 1
            logger.info(
                'Txn failed, retrying with priority gas multiplier {} | Txn payload: {}',
            )
        logger.warning(
            'Tx signing attempt number {} result {} | Txn payload: {}',
            retry_state.attempt_number, retry_state.outcome,
            retry_state.kwargs['txn_payload'],
        )


class TxWorker(GenericAsyncWorker):
    def __init__(self, name, worker_idx, **kwargs):
        """
        Initializes a TxWorker object.

        Args:
            name (str): The name of the worker.
            **kwargs: Additional keyword arguments to be passed to the AsyncWorker constructor.
        """
        self._q, self._rmq_routing = get_tx_send_q_routing_key()
        super(TxWorker, self).__init__(
            name=name, worker_idx=worker_idx, **kwargs,
        )

    @aiorwlock_aqcuire_release
    async def _increment_nonce(self):
        self._signer_nonce += 1
        self._logger.info(
            'Using signer {} for submission task. Incremented nonce {}',
            self._signer_account, self._signer_nonce,
        )

    @aiorwlock_aqcuire_release
    async def _reset_nonce(self, value: int = 0):
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
            if correct_nonce and type(correct_nonce) is int:
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

    # submitSnapshot
    @retry(
        reraise=True,
        retry=retry_if_exception_type(Exception),
        wait=wait_random_exponential(multiplier=1, max=2),
        stop=stop_after_attempt(3),
        after=submit_snapshot_retry_callback,
    )
    async def submit_snapshot(self, txn_payload: TxnPayload, priority_gas_multiplier: int = 0):
        """
        Submit Snapshot
        """
        _nonce = self._signer_nonce
        await self._increment_nonce()
        protocol_state_contract = await self.get_protocol_state_contract(txn_payload.contractAddress)
        self._logger.trace(f'nonce: {_nonce}')
        try:
            tx_hash = await write_transaction(
                self._w3,
                self._signer_account,
                self._signer_pkey,
                protocol_state_contract,
                'submitSnapshot',
                _nonce,
                self._last_gas_price,
                priority_gas_multiplier,
                txn_payload.slotId,
                txn_payload.snapshotCid,
                txn_payload.epochId,
                txn_payload.projectId,
                (
                    txn_payload.request.slotId, txn_payload.request.deadline,
                    txn_payload.request.snapshotCid, txn_payload.request.epochId,
                    txn_payload.request.projectId,
                ),
                txn_payload.signature,
            )

            self._logger.info(
                f'submitted transaction with tx_hash: {tx_hash}, payload {txn_payload}',
            )

            transaction_receipt = await self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=20)
            if 'baseFeePerGas' in transaction_receipt:
                self._last_gas_price = transaction_receipt['baseFeePerGas']

        except Exception as e:
            if 'nonce too low' in str(e):
                error = eval(str(e))
                message = error['message']
                next_nonce = int(message.split('next nonce ')[1].split(',')[0])
                self._logger.info(
                    'Nonce too low error. Next nonce: {}', next_nonce,
                )
                await self._reset_nonce(next_nonce)
                # reset queue
                raise Exception('nonce error, reset nonce')
            else:
                self._logger.info(
                    'Error submitting snapshot. Retrying...',
                )
                # sleep for 5 seconds before updating nonce
                time.sleep(5)
                await self._reset_nonce()

                raise e
        else:
            return tx_hash

    async def _on_rabbitmq_message(self, message: IncomingMessage):
        """
        Callback function that is called when a message is received from RabbitMQ.
        It processes the message and starts the processor task.

        Args:
            message (IncomingMessage): The incoming message from RabbitMQ.

        Returns:
            None
        """
        await message.ack()
        await self.init()
        try:
            msg_obj: TxnPayload = TxnPayload.parse_raw(message.body)
        except ValidationError as e:
            self._logger.opt(exception=False).error(
                (
                    'Bad message structure of callback processor. Error: {}, {}'
                ),
                e, message.body,
            )
            return
        except Exception as e:
            self._logger.opt(exception=False).error(
                (
                    'Unexpected message structure of callback in processor. Error: {}'
                ),
                e,
            )
            return
        else:
            asyncio.ensure_future(
                self.submit_snapshot(txn_payload=msg_obj),
            )
