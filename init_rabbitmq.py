from typing import Tuple

import pika
from fastapi import routing

from settings.conf import settings
from utils.default_logger import logger

# setup logging
init_rmq_logger = logger.bind(module='Powerloom|RabbitMQ|Init')


def create_rabbitmq_conn() -> pika.BlockingConnection:
    """
    Creates a connection to RabbitMQ using the settings specified in the application configuration.

    Returns:
        A `pika.BlockingConnection` object representing the connection to RabbitMQ.
    """
    c = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=settings.rabbitmq.host,
            port=settings.rabbitmq.port,
            virtual_host='/',
            credentials=pika.PlainCredentials(
                username=settings.rabbitmq.user,
                password=settings.rabbitmq.password,
            ),
            heartbeat=30,
        ),
    )
    return c


def get_core_exchange_name() -> str:
    """
    Returns the name of the core exchange for the application.

    Returns:
        str: The name of the core exchange.
    """
    return 'powerloom-relayer'


def get_tx_send_q_routing_key() -> Tuple[str, str]:
    queue_name = 'txSendQueue'
    routing_key = 'txsend'
    return queue_name, routing_key


def init_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
    queue_name: str,
    routing_key: str,
    exchange_name: str,
    bind: bool = True,
) -> None:
    """
    Declare a queue and optionally bind it to an exchange with a routing key.

    Args:
        ch: A blocking channel object from a Pika connection.
        queue_name: The name of the queue to declare.
        routing_key: The routing key to use for binding the queue to an exchange.
        exchange_name: The name of the exchange to bind the queue to.
        bind: Whether or not to bind the queue to the exchange. Defaults to True.

    Returns:
        None
    """
    ch.queue_declare(queue_name)
    if bind:
        ch.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=routing_key,
        )
    init_rmq_logger.debug(
        (
            'Initialized RabbitMQ setup | Queue: {} | Exchange: {} | Routing'
            ' Key: {}'
        ),
        queue_name,
        exchange_name,
        routing_key,
    )


def init_exchanges_queues():
    """
    Initializes the RabbitMQ Direct exchange and queues required for snapshotter.
    """
    c = create_rabbitmq_conn()
    ch: pika.adapters.blocking_connection.BlockingChannel = c.channel()
    # core exchange remains same for multiple snapshotter instances
    #  in the namespace to share across different instance IDs
    exchange_name = get_core_exchange_name()
    ch.exchange_declare(
        exchange=exchange_name, exchange_type='direct', durable=True,
    )
    init_rmq_logger.debug(
        'Initialized RabbitMQ Direct exchange: {}', exchange_name,
    )
    queue_name, routing_key = get_tx_send_q_routing_key()
    init_queue(
        ch,
        exchange_name=exchange_name,
        queue_name=queue_name,
        routing_key=routing_key,
    )


if __name__ == '__main__':
    init_exchanges_queues()
