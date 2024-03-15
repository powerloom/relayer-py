import asyncio
import json
import multiprocessing
import resource
from functools import partial
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from typing import Any
from typing import Dict
from typing import List
from uuid import uuid4

import aiorwlock
import uvloop
from aio_pika import IncomingMessage
from aio_pika.pool import Pool
from eth_typing import ChecksumAddress
from eth_utils.crypto import keccak
from httpx import AsyncHTTPTransport
from web3 import AsyncHTTPProvider
from web3 import AsyncWeb3
from web3 import Web3

from init_rabbitmq import get_core_exchange_name
from settings.conf import settings
from utils.default_logger import logger
from utils.helpers import get_rabbitmq_channel
from utils.helpers import get_rabbitmq_robust_connection_async


class GenericAsyncWorker(multiprocessing.Process):
    _async_transport: AsyncHTTPTransport
    _rmq_connection_pool: Pool
    _rmq_channel_pool: Pool
    _w3: AsyncWeb3
    _signer_account: ChecksumAddress
    _signer_nonce: int
    _signer_pkey: str
    _abi: Dict[str, Any]
    _protocol_state_contract: Any
    _pairs: List[str]

    def __init__(self, name, worker_idx, **kwargs):
        """
        Initializes a GenericAsyncWorker instance.

        Args:
            name (str): The name of the worker.
            **kwargs: Additional keyword arguments to pass to the superclass constructor.
        """
        self._core_rmq_consumer: asyncio.Task
        self._exchange_name = get_core_exchange_name()
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        self._running_callback_tasks: Dict[str, asyncio.Task] = dict()
        super(GenericAsyncWorker, self).__init__(name=name, **kwargs)
        self._protocol_state_contract = None
        self._qos = 1
        self._rate_limiting_lua_scripts = None
        self.protocol_state_contract_address = settings.protocol_state_address
        self._worker_idx = worker_idx
        self._initialized = False

    def _signal_handler(self, signum, frame):
        """
        Signal handler function that cancels the core RMQ consumer when a SIGINT, SIGTERM or SIGQUIT signal is received.

        Args:
            signum (int): The signal number.
            frame (frame): The current stack frame at the time the signal was received.
        """
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._core_rmq_consumer.cancel()

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

    async def _on_rabbitmq_message(self, message: IncomingMessage):
        """
        Callback function that is called when a message is received from RabbitMQ.

        :param message: The incoming message from RabbitMQ.
        """
        pass

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
        self._signer_account = Web3.to_checksum_address(
            settings.signers[self._worker_idx].address,
        )
        self._signer_nonce = await self._w3.eth.get_transaction_count(self._signer_account)
        self._signer_pkey = settings.signers[self._worker_idx].private_key
        self._protocol_state_contract = self._w3.eth.contract(
            address=Web3.to_checksum_address(settings.protocol_state_address), abi=self._abi,
        )
        balance = await self._w3.eth.get_balance(self._signer_account)
        # convert to eth
        balance = self._w3.from_wei(balance, 'ether')
        if balance < settings.min_signer_balance_eth:
            logger.error(
                f'Signer {self._signer_account} has insufficient balance: {balance} ETH',
            )
            exit(1)
        self._rwlock = aiorwlock.RWLock()
        logger.info(
            f'Started worker {self._worker_idx}, with signer_account: {self._signer_account}, signer_nonce: {self._signer_nonce}',
        )

    async def init(self):
        """
        Initializes the worker by initializing the Redis pool, HTTPX client, and RPC helper.
        """
        if not self._initialized:
            await self._init_protocol_meta()
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
