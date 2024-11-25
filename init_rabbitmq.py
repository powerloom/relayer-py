from typing import Tuple

import pika

from settings.conf import settings
from utils.default_logger import logger

# Setup logging for RabbitMQ initialization
init_rmq_logger = logger.bind(module='Powerloom|RabbitMQ|Init')


def create_rabbitmq_conn() -> pika.BlockingConnection:
    """
    Create a connection to RabbitMQ using the settings specified in the application configuration.

    Returns:
        pika.BlockingConnection: A connection object representing the connection to RabbitMQ.
    """
    connection = pika.BlockingConnection(
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
    return connection


def get_core_exchange_name() -> str:
    """
    Get the name of the core exchange for the application.

    Returns:
        str: The name of the core exchange.
    """
    return 'powerloom-relayer'


def get_tx_send_q_routing_key() -> Tuple[str, str]:
    """
    Get the queue name and routing key for transaction sending.

    Returns:
        Tuple[str, str]: A tuple containing the queue name and routing key.
    """
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
        ch (pika.adapters.blocking_connection.BlockingChannel): A blocking channel object from a Pika connection.
        queue_name (str): The name of the queue to declare.
        routing_key (str): The routing key to use for binding the queue to an exchange.
        exchange_name (str): The name of the exchange to bind the queue to.
        bind (bool, optional): Whether or not to bind the queue to the exchange. Defaults to True.

    Returns:
        None
    """
    # Declare the queue
    ch.queue_declare(queue_name)

    # Bind the queue to the exchange if specified
    if bind:
        ch.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=routing_key,
        )

    # Log the initialization of the queue
    init_rmq_logger.debug(
        'Initialized RabbitMQ setup | Queue: {} | Exchange: {} | Routing Key: {}',
        queue_name,
        exchange_name,
        routing_key,
    )


def init_exchanges_queues():
    """
    Initialize the RabbitMQ Direct exchange and queues required for the snapshotter.

    This function sets up the core exchange and initializes the necessary queues
    for transaction sending and checking.
    """
    # Create a connection to RabbitMQ
    connection = create_rabbitmq_conn()
    channel: pika.adapters.blocking_connection.BlockingChannel = connection.channel()

    # Set up the core exchange
    exchange_name = get_core_exchange_name()
    channel.exchange_declare(
        exchange=exchange_name, exchange_type='direct', durable=True,
    )
    init_rmq_logger.debug(
        'Initialized RabbitMQ Direct exchange: {}', exchange_name,
    )

    # Initialize the transaction send queue
    queue_name, routing_key = get_tx_send_q_routing_key()
    init_queue(
        channel,
        exchange_name=exchange_name,
        queue_name=queue_name,
        routing_key=routing_key,
    )


if __name__ == '__main__':
    # Initialize RabbitMQ exchanges and queues when the script is run directly
    init_exchanges_queues()
