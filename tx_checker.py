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

import uvloop
from aio_pika import IncomingMessage
from aio_pika import Message
from aio_pika.pool import Pool
from eth_utils.crypto import keccak
from pydantic import ValidationError
from web3 import AsyncHTTPProvider
from web3 import AsyncWeb3
from web3 import Web3

from data_models import BatchSubmissionRequest
from init_rabbitmq import get_core_exchange_name
from init_rabbitmq import get_tx_check_q_routing_key
from init_rabbitmq import get_tx_send_q_routing_key
from settings.conf import settings
from utils.default_logger import logger
from utils.helpers import get_rabbitmq_channel
from utils.helpers import get_rabbitmq_robust_connection_async
from utils.redis_conn import RedisPool


class TxChecker(multiprocessing.Process):
    """
    A transaction checker process that consumes messages from RabbitMQ,
    validates them, and forwards them to the transaction sender queue.
    """

    _rmq_connection_pool: Pool
    _rmq_channel_pool: Pool

    def __init__(self, name, **kwargs):
        """
        Initialize a TxChecker instance.

        Args:
            name (str): The name of the worker.
            **kwargs: Additional keyword arguments to be passed to the Process constructor.
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
        Process incoming RabbitMQ messages.

        This method is called when a message is received from RabbitMQ.
        It validates the message, estimates gas for the transaction,
        and forwards the message to the transaction sender queue if successful.

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
            try:
                # Estimate gas for the transaction
                _ = await self._protocol_state_contract.functions.submitSubmissionBatch(
                    msg_obj.dataMarket, msg_obj.batchCid, msg_obj.batchId, msg_obj.epochId,
                    msg_obj.projectIds, msg_obj.snapshotCids, msg_obj.finalizedCidsRootHash,
                ).estimate_gas({'from': settings.signers[0].address})

                self._logger.info('Processing message: {}', msg_obj)

                # Forward the message to the transaction sender queue
                async with self._rmq_channel_pool.acquire() as channel:
                    exchange = await channel.get_exchange(name=get_core_exchange_name())
                    queue_name, routing_key = get_tx_send_q_routing_key()
                    await exchange.publish(
                        routing_key=routing_key,
                        message=Message(msg_obj.json().encode('utf-8')),
                    )
            except Exception as e:
                self._logger.opt(exception=True).error(
                    'Error processing message: {}, Error: {}',
                    msg_obj, e,
                )

    def _signal_handler(self, signum, frame):
        """
        Handle termination signals.

        This method cancels the core RMQ consumer when a SIGINT, SIGTERM,
        or SIGQUIT signal is received.

        Args:
            signum (int): The signal number.
            frame (frame): The current stack frame at the time the signal was received.
        """
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._core_rmq_consumer.cancel()

    async def _rabbitmq_consumer(self, loop):
        """
        Set up and run the RabbitMQ consumer.

        This method initializes the RabbitMQ connection and channel pools,
        sets up the exchange and queue, and starts consuming messages.

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

        async with self._rmq_channel_pool.acquire() as channel:
            await channel.set_qos(self._qos)
            exchange = await channel.get_exchange(name=self._exchange_name)
            q_obj = await channel.get_queue(name=self._q, ensure=False)

            self._logger.debug(
                'Consuming queue {} with routing key {}...',
                self._q, self._rmq_routing,
            )

            # Bind the queue and start consuming messages
            await q_obj.bind(exchange, routing_key=self._rmq_routing)
            await q_obj.consume(self._on_rabbitmq_message)

    async def _init_redis_pool(self):
        """
        Initialize the Redis connection pool.
        """
        self.aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
        await self.aioredis_pool.populate()
        self.reader_redis_pool = self.aioredis_pool.reader_redis_pool
        self.writer_redis_pool = self.aioredis_pool.writer_redis_pool

    async def _init_protocol_meta(self):
        """
        Initialize the Web3 contract and ABI.
        """
        # Load ABI from file
        with open('utils/static/abi.json', 'r') as f:
            self._abi = json.load(f)

        # Initialize Web3 instance and contract
        self._w3 = AsyncWeb3(
            AsyncHTTPProvider(settings.anchor_chain.rpc.full_nodes[0].url),
        )
        self._protocol_state_contract = self._w3.eth.contract(
            address=Web3.to_checksum_address(settings.protocol_state_address),
            abi=self._abi,
        )

    async def init(self):
        """
        Initialize the worker components.

        This method initializes the protocol metadata and Redis pool
        if they haven't been initialized yet.
        """
        if not self._initialized:
            await self._init_protocol_meta()
            await self._init_redis_pool()
        self._initialized = True

    def run(self) -> None:
        """
        Run the TxChecker process.

        This method sets up the environment, initializes signal handlers,
        and starts the event loop for processing messages.
        """
        self._logger = logger.bind(module=self.name)

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
            'Starting asynchronous callback worker {}...',
            self._unique_id,
        )
        self._core_rmq_consumer = asyncio.ensure_future(
            self._rabbitmq_consumer(ev_loop),
        )
        try:
            ev_loop.run_forever()
        finally:
            ev_loop.close()
