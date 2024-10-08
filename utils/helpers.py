import asyncio
import sys
from functools import wraps

import aio_pika
from aio_pika.pool import Pool

from settings.conf import settings
from utils.default_logger import logger


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
            # Kill all child processes
            self._kill_all_children()
            logger.error('Finished waiting for all children...now can exit.')
        finally:
            logger.error('Finished waiting for all children...now can exit.')
            self._reporter_thread.join()
            sys.exit(0)
    return wrapper


def aiorwlock_aqcuire_release(fn):
    """
    A decorator that acquires and releases an asynchronous read-write lock around a function.

    This decorator ensures that the wrapped function is executed with exclusive write access,
    preventing concurrent execution of multiple instances of the decorated function.

    Args:
        fn (function): The asynchronous function to be wrapped.

    Returns:
        function: The wrapped asynchronous function.
    """
    @wraps(fn)
    async def wrapper(self, *args, **kwargs):
        self._logger.info(
            'Using signer {} for submission task. Acquiring lock {}', self._signer_account, self._rwlock,
        )
        await self._rwlock.writer_lock.acquire()

        self._logger.info(
            'Using signer {} for submission task. Acquired lock', self._signer_account,
        )

        try:
            # Execute the wrapped function
            result = await fn(self, *args, **kwargs)
        except Exception as e:
            self._logger.opt(exception=True).error(
                'Error in using signer {} for submission task: {}', self._signer_account, e,
            )
        finally:
            try:
                # Always attempt to release the lock, even if an exception occurred
                self._rwlock.writer_lock.release()
            except Exception as e:
                logger.trace(
                    'Error releasing rwlock: {}. But moving on regardless... | Context: '
                    'Using signer {} for submission task: {}.', e, self._signer_account, kwargs,
                )
        return result
    return wrapper


async def get_rabbitmq_robust_connection_async():
    """
    Establishes a robust connection to RabbitMQ server using the settings specified in the configuration file.

    This function creates a connection that automatically recovers from network failures.

    Returns:
        aio_pika.Connection: A robust connection to RabbitMQ.
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
    Establishes a basic asynchronous connection to RabbitMQ using the settings specified in the config file.

    This function creates a standard connection without automatic recovery features.

    Returns:
        aio_pika.Connection: A basic asynchronous connection to RabbitMQ.
    """
    return await aio_pika.connect(
        host=settings.rabbitmq.host,
        port=settings.rabbitmq.port,
        virtual_host='/',
        login=settings.rabbitmq.user,
        password=settings.rabbitmq.password,
    )


async def get_rabbitmq_channel(connection_pool: Pool) -> aio_pika.Channel:
    """
    Acquires a connection from the connection pool and returns a channel object for RabbitMQ communication.

    This function manages the lifecycle of the connection and channel, ensuring proper resource management.

    Args:
        connection_pool (aio_pika.pool.Pool): An instance of the connection pool.

    Returns:
        aio_pika.Channel: A channel object for communicating with RabbitMQ.
    """
    async with connection_pool.acquire() as connection:
        return await connection.channel()
