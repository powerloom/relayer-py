import asyncio
import json
import multiprocessing
import os
import resource
from functools import partial
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from typing import Any
from typing import Dict
from uuid import uuid4

import aiorwlock
import sha3
import uvloop
from aio_pika import IncomingMessage
from aio_pika.pool import Pool
from eip712_structs import EIP712Struct
from eip712_structs import String
from eip712_structs import Uint
from eth_typing import ChecksumAddress
from eth_utils.crypto import keccak
from httpx import AsyncClient
from httpx import AsyncHTTPTransport
from httpx import Limits
from httpx import Timeout
from web3 import AsyncHTTPProvider
from web3 import AsyncWeb3
from web3 import Web3

from init_rabbitmq import get_core_exchange_name
from settings.conf import settings
from utils.default_logger import logger
from utils.helpers import get_rabbitmq_channel
from utils.helpers import get_rabbitmq_robust_connection_async
from utils.redis_conn import RedisPool

# Day buffer to account for chain migration or other issues
DAY_BUFFER = 36


class Request(EIP712Struct):
    """
    EIP712 struct for representing a request.
    """
    slotId = Uint()
    deadline = Uint()
    snapshotCid = String()
    epochId = Uint()
    projectId = String()


def keccak_hash(x):
    """
    Compute the Keccak-256 hash of the input.

    Args:
        x: The input to be hashed.

    Returns:
        bytes: The Keccak-256 hash of the input.
    """
    return sha3.keccak_256(x).digest()


class GenericAsyncWorker(multiprocessing.Process):
    """
    A generic asynchronous worker class that handles RabbitMQ messaging and blockchain interactions.
    """
    _async_transport: AsyncHTTPTransport
    _rmq_connection_pool: Pool
    _rmq_channel_pool: Pool
    _w3: AsyncWeb3
    _signer_account: ChecksumAddress
    _signer_nonce: int
    _signer_pkey: str
    _abi: Dict[str, Any]
    _protocol_state_contract: Any

    def __init__(self, name, **kwargs):
        """
        Initialize a GenericAsyncWorker instance.

        Args:
            name (str): The name of the worker.
            worker_idx (int): The index of the worker.
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
        self._worker_idx = int(os.environ['NODE_APP_INSTANCE'])
        self._initialized = False
        self.protocol_state_contract_instance_mapping = {}

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
        Get or create a Protocol State Contract instance.

        Args:
            contract_address (str): The address of the contract.

        Returns:
            Contract: The contract instance, or None if the address is invalid.
        """
        # Validate contract address
        if not self._w3.is_address(contract_address):
            return None

        # Get or create contract object
        checksum_address = self._w3.to_checksum_address(contract_address)
        if checksum_address not in self.protocol_state_contract_instance_mapping:
            contract = self._w3.eth.contract(
                address=contract_address, abi=self._abi,
            )
            self.protocol_state_contract_instance_mapping[checksum_address] = contract
            return contract
        else:
            return self.protocol_state_contract_instance_mapping[checksum_address]

    async def _rabbitmq_consumer(self, loop):
        """
        Set up and start consuming messages from a RabbitMQ queue.

        Args:
            loop (asyncio.AbstractEventLoop): The event loop to use for the consumer.

        Returns:
            None
        """
        # Initialize connection and channel pools
        self._rmq_connection_pool = Pool(
            get_rabbitmq_robust_connection_async, max_size=5, loop=loop,
        )
        self._rmq_channel_pool = Pool(
            partial(get_rabbitmq_channel, self._rmq_connection_pool), max_size=20,
            loop=loop,
        )

        # Set up queue and start consuming
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

        Args:
            message (IncomingMessage): The incoming message from RabbitMQ.
        """
        pass

    async def _init_redis_pool(self):
        """
        Initialize the Redis connection pools for the worker.
        """
        self.aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
        await self.aioredis_pool.populate()
        self.reader_redis_pool = self.aioredis_pool.reader_redis_pool
        self.writer_redis_pool = self.aioredis_pool.writer_redis_pool

    async def _init_protocol_meta(self):
        """
        Initialize protocol metadata, including Web3 connection, contracts, and other blockchain-related information.
        """
        with open('utils/static/abi.json', 'r') as f:
            self._abi = json.load(f)

        # Initialize Web3 connection
        self._w3 = AsyncWeb3(
            AsyncHTTPProvider(
                settings.anchor_chain.rpc.full_nodes[0].url,
            ),
        )

        # Set up signer account
        self._signer_account = Web3.to_checksum_address(
            settings.signers[self._worker_idx].address,
        )
        self._signer_nonce = await self._w3.eth.get_transaction_count(self._signer_account)
        self._signer_pkey = settings.signers[self._worker_idx].private_key

        # Initialize protocol state contract
        self._protocol_state_contract = self._w3.eth.contract(
            address=Web3.to_checksum_address(settings.protocol_state_address), abi=self._abi,
        )

        # Check signer account balance
        balance = await self._w3.eth.get_balance(self._signer_account)
        balance = self._w3.from_wei(balance, 'ether')
        if balance < settings.min_signer_balance_eth:
            logger.error(
                f'Signer {self._signer_account} has insufficient balance: {balance} ETH',
            )
            exit(1)

        # Initialize read-write lock
        self._rwlock = aiorwlock.RWLock(fast=True)

        logger.info(
            f'Started worker {self._worker_idx}, with signer_account: {self._signer_account}, signer_nonce: {self._signer_nonce}',
        )

        # Get current gas price
        self._last_gas_price = await self._w3.eth.gas_price

    async def _init_httpx_client(self):
        """
        Initializes the Telegram client.
        """
        self._http_client = AsyncClient(
            timeout=Timeout(timeout=30),
            follow_redirects=False,
            transport=AsyncHTTPTransport(
                limits=Limits(
                    max_connections=10,
                    max_keepalive_connections=5,
                    keepalive_expiry=None,
                ),
            ),
        )

    async def init(self):
        """
        Initialize the worker by setting up protocol metadata and Redis pools.
        """
        if not self._initialized:
            await self._init_protocol_meta()
            await self._init_redis_pool()
        self._initialized = True

    def run(self) -> None:
        """
        Run the worker process. This method sets up resource limits, signal handlers,
        starts the RabbitMQ consumer, and runs the event loop.
        """
        self._logger = logger.bind(module=self.name)
        self._logger.info(
            f'Starting worker {self.name} {self._worker_idx}...',
        )

        # Set resource limits
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )

        # Set up signal handlers
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)

        # Set up and run the event loop
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
