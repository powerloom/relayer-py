import aio_pika
from aio_pika.pool import Pool
from settings.conf import settings
from utils.default_logger import logger
from functools import wraps
import os
import sys
import psutil
import signal


def cleanup_proc_hub_children(fn):
    """
    A decorator that wraps a function and handles cleanup of any child processes
    spawned by the function in case of an exception.

    Args:
        fn (function): The function to be wrapped.

    Returns:
        function: The wrapped function.
    """
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            fn(self, *args, **kwargs)
            logger.info('Finished running process hub core...')
        except Exception as e:
            logger.opt(exception=True).error(
                'Received an exception on process hub core run(): {}',
                e,
            )
            # logger.error('Initiating kill children....')
            # # silently kill all children
            # procs = psutil.Process().children()
            # for p in procs:
            #     p.terminate()
            # gone, alive = psutil.wait_procs(procs, timeout=3)
            # for p in alive:
            #     logger.error(f'killing process: {p.name()}')
            #     p.kill()
            self._kill_all_children()
            logger.error('Finished waiting for all children...now can exit.')
        finally:
            logger.error('Finished waiting for all children...now can exit.')
            self._reporter_thread.join()
            sys.exit(0)
            # sys.exit(0)
    return wrapper


async def get_rabbitmq_robust_connection_async():
    """
    Returns a robust connection to RabbitMQ server using the settings specified in the configuration file.
    """
    return await aio_pika.connect_robust(
        host=settings.rabbitmq.host,
        port=settings.rabbitmq.port,
        virtual_host='/',
        login=settings.rabbitmq.user,
        password=settings.rabbitmq.password,
    )


async def get_rabbitmq_basic_connection_async():
    """
    Returns an async connection to RabbitMQ using the settings specified in the config file.

    :return: An async connection to RabbitMQ.
    """
    return await aio_pika.connect(
        host=settings.rabbitmq.host,
        port=settings.rabbitmq.port,
        virtual_host='/',
        login=settings.rabbitmq.user,
        password=settings.rabbitmq.password,
    )


async def get_rabbitmq_channel(connection_pool) -> aio_pika.Channel:
    """
    Acquires a connection from the connection pool and returns a channel object for RabbitMQ communication.

    Args:
        connection_pool: An instance of `aio_pika.pool.Pool`.

    Returns:
        An instance of `aio_pika.Channel`.
    """
    async with connection_pool.acquire() as connection:
        return await connection.channel()

