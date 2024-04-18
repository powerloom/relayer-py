import asyncio
import time

import sha3
import tenacity
from aio_pika import IncomingMessage
from eip712_structs import EIP712Struct
from eip712_structs import String
from eip712_structs import Uint
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


def keccak_hash(x):
    return sha3.keccak_256(x).digest()


def teancity_retry_callback(retry_state: tenacity.RetryCallState):
    if retry_state.attempt_number >= 3:
        logger.error('Txn signing worker failed after 3 attempts')
    else:
        logger.warning(
            f'Tx signing worker attempt number {retry_state.attempt_number} result {retry_state.outcome}',
        )


class Request(EIP712Struct):
    slotId = Uint()
    deadline = Uint()
    snapshotCid = String()
    epochId = Uint()
    projectId = String()


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

    # submitSnapshot
    @aiorwlock_aqcuire_release
    @retry(
        reraise=True,
        retry=retry_if_exception_type(Exception),
        wait=wait_random_exponential(multiplier=1, max=2),
        stop=stop_after_attempt(3),
        after=teancity_retry_callback,
    )
    async def submit_snapshot(self, txn_payload: TxnPayload):
        """
        Submit Snapshot
        """
        _nonce = self._signer_nonce
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

            self._signer_nonce += 1

            self._logger.info(
                f'submitted transaction with tx_hash: {tx_hash}, payload {txn_payload}',
            )
            if not tx_hash:
                self._logger.info('tx_hash is None for submission task')
                self._logger.info(
                    'Using signer {} for submission task. Put nonce {} back in queue',
                    self._signer_account, self._signer_nonce - 1,
                )
                await self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=10)

        except Exception as e:
            if 'nonce too low' in str(e):
                error = eval(str(e))
                message = error['message']
                next_nonce = int(message.split('next nonce ')[1].split(',')[0])
                self._logger.info(
                    'Nonce too low error. Next nonce: {}', next_nonce,
                )
                self._signer_nonce = next_nonce
                # reset queue
                raise Exception('nonce error, reset nonce')
            else:
                self._logger.info(
                    'replacement transaction underpriced, sleeping for 10 seconds, then retrying...',
                )
                time.sleep(5)
                self._signer_nonce = await self._w3.eth.get_transaction_count(
                    self._signer_account,
                )
                self._logger.info(
                    f'nonce for {self._signer_account} reset to: {self._signer_nonce}',
                )
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
            if await self._check(msg_obj):
                self._logger.info(
                    f'Processing message: {msg_obj}',
                )
                asyncio.ensure_future(
                    self.submit_snapshot(txn_payload=msg_obj),
                )
            else:
                self._logger.trace(
                    f'Snapshot received but not submitted to chain because _check failed! {msg_obj}',
                )
