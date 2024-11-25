import contextlib
from functools import wraps
from typing import Optional

import redis
import redis.exceptions as redis_exc
from redis import asyncio as aioredis

from data_models import RedisConfig
from settings.conf import settings as settings_conf
from utils.default_logger import logger


def inject_retry_exception_conf(redis_conf: dict):
    """
    Inject retry configuration for Redis exceptions.

    Args:
        redis_conf (dict): Redis configuration dictionary.
    """
    redis_conf.update({'retry_on_error': [redis.exceptions.ReadOnlyError]})


# Redis connection configurations
REDIS_CONN_CONF = settings_conf.redis.dict()
REDIS_WRITER_CONN_CONF = settings_conf.redis.dict()
REDIS_READER_CONN_CONF = settings_conf.redis.dict()
# TODO: Remove if separate read connections won't be necessary. Presently this does nothing.


def construct_redis_url(redis_settings: RedisConfig) -> str:
    """
    Construct a Redis URL from the given settings.

    Args:
        redis_settings (RedisConfig): Redis configuration object.

    Returns:
        str: Constructed Redis URL.
    """
    if redis_settings.password:
        return f'redis://{redis_settings.password}@{redis_settings.host}:{redis_settings.port}/{redis_settings.db}'
    else:
        return f'redis://{redis_settings.host}:{redis_settings.port}/{redis_settings.db}'


@contextlib.contextmanager
def get_redis_conn_from_pool(connection_pool: redis.BlockingConnectionPool) -> redis.Redis:
    """
    Context manager that creates and tears down a Redis session.

    Args:
        connection_pool (redis.BlockingConnectionPool): Redis connection pool.

    Yields:
        redis.Redis: Redis connection object.

    Raises:
        redis_exc.RedisError: If a Redis-specific error occurs.
    """
    try:
        redis_conn = redis.Redis(connection_pool=connection_pool)
        yield redis_conn
    except redis_exc.RedisError:
        raise
    except KeyboardInterrupt:
        pass


def provide_redis_conn_repsawning_thread(fn):
    """
    Decorator to provide a Redis connection to a function, respawning the thread if necessary.

    Args:
        fn (callable): The function to be decorated.

    Returns:
        callable: The wrapped function.
    """
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        arg_conn = 'redis_conn'
        func_params = fn.__code__.co_varnames
        conn_in_args = arg_conn in func_params and func_params.index(
            arg_conn,
        ) < len(args)
        conn_in_kwargs = arg_conn in kwargs
        if conn_in_args or conn_in_kwargs:
            return fn(*args, **kwargs)
        else:
            connection_pool = redis.BlockingConnectionPool(**REDIS_CONN_CONF)
            while True:
                try:
                    with get_redis_conn_from_pool(connection_pool) as redis_obj:
                        kwargs[arg_conn] = redis_obj
                        logger.debug(
                            'Returning after populating redis connection object',
                        )
                        _ = fn(self, *args, **kwargs)
                except Exception:
                    continue
                else:
                    # If no exception was caught and the thread returns normally,
                    # it is the sign of a shutdown event being set
                    return _

    return wrapper


def provide_redis_conn(fn):
    """
    Decorator to provide a Redis connection to a function.

    Args:
        fn (callable): The function to be decorated.

    Returns:
        callable: The wrapped function.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        arg_conn = 'redis_conn'
        func_params = fn.__code__.co_varnames
        conn_in_args = arg_conn in func_params and func_params.index(
            arg_conn,
        ) < len(args)
        conn_in_kwargs = arg_conn in kwargs
        if conn_in_args or conn_in_kwargs:
            return fn(*args, **kwargs)
        else:
            inject_retry_exception_conf(redis_conf=REDIS_CONN_CONF)
            connection_pool = redis.BlockingConnectionPool(
                **REDIS_CONN_CONF, max_connections=20,
            )
            with get_redis_conn_from_pool(connection_pool) as redis_obj:
                kwargs[arg_conn] = redis_obj
                logger.debug(
                    'Returning after populating redis connection object',
                )
                return fn(*args, **kwargs)

    return wrapper


async def get_redis_pool(redis_settings: RedisConfig = settings_conf.redis, pool_size=200):
    """
    Get a Redis connection pool.

    Args:
        redis_settings (RedisConfig): Redis configuration object.
        pool_size (int): Maximum number of connections in the pool.

    Returns:
        aioredis.Redis: Redis connection pool.
    """
    return await aioredis.from_url(
        url=construct_redis_url(redis_settings),
        max_connections=pool_size,
        retry_on_error=[redis.exceptions.ReadOnlyError],
    )


# TODO: find references to usage and replace with pool interface
async def get_writer_redis_conn():
    """
    Get a Redis connection for writing.

    Returns:
        aioredis.Redis: Redis connection for writing.
    """
    out = await aioredis.Redis(
        host=REDIS_WRITER_CONN_CONF['host'],
        port=REDIS_WRITER_CONN_CONF['port'],
        db=REDIS_WRITER_CONN_CONF['db'],
        password=REDIS_WRITER_CONN_CONF['password'],
        retry_on_error=[redis.exceptions.ReadOnlyError],
        single_connection_client=True,
    )
    return out


# TODO: find references to usage and replace with pool interface
async def get_reader_redis_conn():
    """
    Get a Redis connection for reading.

    Returns:
        aioredis.Redis: Redis connection for reading.
    """
    out = await aioredis.Redis(
        host=REDIS_READER_CONN_CONF['host'],
        port=REDIS_READER_CONN_CONF['port'],
        db=REDIS_READER_CONN_CONF['db'],
        password=REDIS_READER_CONN_CONF['password'],
        retry_on_error=[redis.exceptions.ReadOnlyError],
        single_connection_client=True,
    )
    return out


class RedisPool:
    """
    A class to manage Redis connection pools for reading and writing.
    """

    reader_redis_pool: aioredis.Redis
    writer_redis_pool: aioredis.Redis

    def __init__(
            self,
            writer_redis_conf: RedisConfig = settings_conf.redis,
            reader_redis_conf: Optional[RedisConfig] = settings_conf.redis,
            pool_size=200,
            replication_mode=True,
    ):
        """
        Initialize the RedisPool.

        Args:
            writer_redis_conf (RedisConfig): Configuration for the writer Redis connection.
            reader_redis_conf (Optional[RedisConfig]): Configuration for the reader Redis connection.
            pool_size (int): Maximum number of connections in each pool.
            replication_mode (bool): If True, use the same pool for reading and writing.
        """
        self._writer_redis_conf = writer_redis_conf
        self._reader_redis_conf = reader_redis_conf
        self._pool_size = pool_size
        self._replication_mode = replication_mode

    async def populate(self):
        """
        Populate the Redis connection pools.
        """
        self.writer_redis_pool: aioredis.Redis = await get_redis_pool(self._writer_redis_conf, self._pool_size)
        if self._replication_mode:
            self.reader_redis_pool = self.writer_redis_pool
        else:
            self.reader_redis_pool: aioredis.Redis = await get_redis_pool(self._reader_redis_conf, self._pool_size)
