import asyncio
import json
import multiprocessing
import resource
from functools import partial
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from uuid import uuid4

import sha3
import uvloop
from aio_pika import IncomingMessage
from aio_pika import Message
from aio_pika.pool import Pool
from eip712_structs import EIP712Struct
from eip712_structs import make_domain
from eip712_structs import String
from eip712_structs import Uint
from eth_utils.crypto import keccak
from pydantic import ValidationError
from web3 import AsyncHTTPProvider
from web3 import AsyncWeb3
from web3 import Web3

from data_models import TxnPayload
from helpers.redis_keys import timeslot_preference
from init_rabbitmq import get_core_exchange_name
from init_rabbitmq import get_tx_check_q_routing_key
from init_rabbitmq import get_tx_send_q_routing_key
from settings.conf import settings
from utils.default_logger import logger
from utils.generic_worker import GenericAsyncWorker
from utils.helpers import get_rabbitmq_channel
from utils.helpers import get_rabbitmq_robust_connection_async
from utils.redis_conn import RedisPool


# day buffer due to chain migration or other issues
DAY_BUFFER = 36


def keccak_hash(x):
    return sha3.keccak_256(x).digest()


class Request(EIP712Struct):
    slotId = Uint()
    deadline = Uint()
    snapshotCid = String()
    epochId = Uint()
    projectId = String()


class TxChecker(multiprocessing.Process):
    _rmq_connection_pool: Pool
    _rmq_channel_pool: Pool

    def __init__(self, name, **kwargs):
        """
        Initializes a TxWorker object.

        Args:
            name (str): The name of the worker.
            **kwargs: Additional keyword arguments to be passed to the AsyncWorker constructor.
        """
        self._core_rmq_consumer: asyncio.Task
        self._exchange_name = get_core_exchange_name()
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        super(TxChecker, self).__init__(name=name, **kwargs)
        self._protocol_state_contract = None
        self._qos = 1
        self.protocol_state_contract_address = settings.protocol_state_address
        self._initialized = False
        self.protocol_state_contract_instance_mapping = {}

        self._q, self._rmq_routing = get_tx_check_q_routing_key()

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
                async with self._rmq_channel_pool.acquire() as channel:
                    exchange = await channel.get_exchange(name=get_core_exchange_name())
                    queue_name, routing_key = get_tx_send_q_routing_key()
                    await exchange.publish(
                        routing_key=routing_key,
                        message=Message(msg_obj.json().encode('utf-8')),
                    )
            else:
                self._logger.trace(
                    f'Snapshot received but not submitted to chain because _check failed! {msg_obj}',
                )

    def _signal_handler(self, signum, frame):
        """
        Signal handler function that cancels the core RMQ consumer when a SIGINT, SIGTERM or SIGQUIT signal is received.

        Args:
            signum (int): The signal number.
            frame (frame): The current stack frame at the time the signal was received.
        """
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._core_rmq_consumer.cancel()

    async def get_protocol_state_contract(self, contract_address: str):
        """
        Get Protocol State Contract
        """
        # validate contract address
        if not self._w3.is_address(contract_address):
            return None

        # get contract object
        if self._w3.to_checksum_address(contract_address) not in self.protocol_state_contract_instance_mapping:
            contract = self._w3.eth.contract(
                address=contract_address, abi=self._abi,
            )
            self.protocol_state_contract_instance_mapping[
                self._w3.to_checksum_address(contract_address)
            ] = contract
            return contract
        else:
            return self.protocol_state_contract_instance_mapping[
                self._w3.to_checksum_address(contract_address)
            ]

    async def _get_signer_address(self, txn_payload: TxnPayload):
        """
        Get Signer Address
        """

        request_ = txn_payload.request.dict()
        signature = bytes.fromhex(txn_payload.signature[2:])
        signature_request = Request(
            slotId=request_['slotId'],
            deadline=request_['deadline'],
            snapshotCid=request_['snapshotCid'],
            epochId=request_['epochId'],
            projectId=request_['projectId'],
        )

        domain_separator = make_domain(
            name='PowerloomProtocolContract',
            version='0.1', chainId=settings.anchor_chain.chain_id,
            verifyingContract=self._w3.to_checksum_address(
                txn_payload.contractAddress,
            ),
        )

        signable_bytes = signature_request.signable_bytes(domain_separator)
        message_hash = keccak_hash(signable_bytes)
        signer_address = self._w3.eth.account._recover_hash(
            message_hash, signature=signature,
        )
        return signer_address

    async def _check(self, txn_payload: TxnPayload):
        # get timeslot preference
        timeslot_pref = await self.reader_redis_pool.get(timeslot_preference(txn_payload.slotId))
        if timeslot_pref:
            timeslot_pref = int(timeslot_pref.decode('utf-8'))
        else:
            timeslot_pref = await self._protocol_state_contract.functions.getSnapshotterTimeSlot(
                txn_payload.slotId,
            ).call()
            if timeslot_pref:
                await self.writer_redis_pool.set(
                    timeslot_preference(txn_payload.slotId), timeslot_pref,
                )
        if not timeslot_pref:
            return False
        if (txn_payload.epochId % self.epochs_in_a_day) // (self.epochs_in_a_day // self.slots_per_day) != timeslot_pref - 1:
            return False

        current_epoch = txn_payload.epochId
        snapshotter_address = await self._get_signer_address(txn_payload)
        snapshotter_hash = hash(int(snapshotter_address.lower(), 16))

        current_day = (current_epoch // 720) + DAY_BUFFER

        pair_idx = (
            current_epoch + snapshotter_hash + txn_payload.slotId +
            current_day
        ) % len(self._pairs)
        # projectId check
        if self._pairs[pair_idx].lower() not in txn_payload.projectId.lower():
            return False
        return True

    async def _rabbitmq_consumer(self, loop):
        """
        Consume messages from a RabbitMQ queue.

        Args:
            loop (asyncio.AbstractEventLoop): The event loop to use for the consumer.

        Returns:
            None
        """
        self._rmq_connection_pool = Pool(
            get_rabbitmq_robust_connection_async, max_size=5, loop=loop,
        )
        self._rmq_channel_pool = Pool(
            partial(get_rabbitmq_channel, self._rmq_connection_pool), max_size=20,
            loop=loop,
        )
        async with self._rmq_channel_pool.acquire() as channel:
            await channel.set_qos(self._qos)
            exchange = await channel.get_exchange(
                name=self._exchange_name,
            )
            q_obj = await channel.get_queue(
                name=self._q,
                ensure=False,
            )
            self._logger.debug(
                f'Consuming queue {self._q} with routing key {self._rmq_routing}...',
            )
            await q_obj.bind(exchange, routing_key=self._rmq_routing)
            await q_obj.consume(self._on_rabbitmq_message)

    async def _init_redis_pool(self):
        """
        Initializes the Redis pool for the worker.
        """
        self.aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
        await self.aioredis_pool.populate()
        self.reader_redis_pool = self.aioredis_pool.reader_redis_pool
        self.writer_redis_pool = self.aioredis_pool.writer_redis_pool

    async def _init_protocol_meta(self):
        with open('utils/static/pairs.json', 'r') as f:
            self._pairs = json.load(f)

        with open('utils/static/abi.json', 'r') as f:
            self._abi = json.load(f)

        self._w3 = AsyncWeb3(
            AsyncHTTPProvider(
                settings.anchor_chain.rpc.full_nodes[0].url,
            ),
        )
        self._protocol_state_contract = self._w3.eth.contract(
            address=Web3.to_checksum_address(settings.protocol_state_address), abi=self._abi,
        )
        self.slots_per_day = await self._protocol_state_contract.functions.SLOTS_PER_DAY().call()
        self.epoch_size = await self._protocol_state_contract.functions.EPOCH_SIZE().call()
        self.source_chain_block_time = (await self._protocol_state_contract.functions.SOURCE_CHAIN_BLOCK_TIME().call()) / 1e4
        self.epochs_in_a_day = 86400 // (
            self.epoch_size * self.source_chain_block_time
        )

    async def init(self):
        """
        Initializes the worker by initializing the Redis pool, HTTPX client, and RPC helper.
        """
        if not self._initialized:
            await self._init_protocol_meta()
            await self._init_redis_pool()
        self._initialized = True

    def run(self) -> None:
        """
        Runs the worker by setting resource limits, registering signal handlers, starting the RabbitMQ consumer, and
        running the event loop until it is stopped.
        """
        self._logger = logger.bind(module=self.name)
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        ev_loop = asyncio.get_event_loop()
        self._logger.debug(
            f'Starting asynchronous callback worker {self._unique_id}...',
        )
        self._core_rmq_consumer = asyncio.ensure_future(
            self._rabbitmq_consumer(ev_loop),
        )
        try:
            ev_loop.run_forever()
        finally:
            ev_loop.close()
