import asyncio
import tenacity
import sha3
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
        # TODO: figure out how to access the retried functions state since retry_state.args returns the self object in a single tuple
        # logger.error('Txn signing worker for payload {retry_state.args[1]} failed after 3 attempts')
        logger.error('Txn signing worker failed after 3 attempts')
    else:
        logger.warning('Tx signing worker attempt number {retry_state.attempt_number} result {retry_state.outcome}')

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
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        after=teancity_retry_callback,
    )
    async def submit_snapshot(self, txn_payload: TxnPayload):
        """
        Submit Snapshot
        """
        # ideally this should not be necessary given that async code can not be parallel
        _nonce = self._signer_nonce
        try:
            tx_hash = await write_transaction(
                self._w3,
                self._signer_account,
                self._signer_pkey,
                self._protocol_state_contract,
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
                f'submitted transaction with tx_hash: {tx_hash}',
            )

        except Exception as e:
            self._logger.error(f'Exception: {e}')

            if 'nonce' in str(e):
                # sleep for 10 seconds and reset nonce
                await asyncio.sleep(10)
                self._signer_nonce = await self._w3.eth.get_transaction_count(
                    self._signer_account,
                )
                self._logger.info(
                    f'nonce reset to: {self._signer_nonce}',
                )
                raise Exception('nonce error, reset nonce')
            else:
                raise Exception('other error, still retrying')

        try:
            receipt = await self._w3.eth.wait_for_transaction_receipt(tx_hash)

            if receipt['status'] == 0:
                self._logger.info(
                    f'tx_hash: {tx_hash} failed to gather success receipt after 120 seconds, receipt: {receipt}, payload: {txn_payload}',
                )
                # retry
            else:
                self._logger.info(
                    f'tx_hash: {tx_hash} succeeded!, project_id: {txn_payload.projectId}, epoch_id: {txn_payload.epochId}',
                )
        except:
            pass

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
            asyncio.ensure_future(self.submit_snapshot(txn_payload=msg_obj))
