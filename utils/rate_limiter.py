import time
from typing import List

import redis.exceptions
from async_limits import RateLimitItem
from async_limits.storage import AsyncRedisStorage
from async_limits.strategies import AsyncFixedWindowRateLimiter
from redis import asyncio as aioredis

from utils.exceptions import RPCException


# Initialize rate limits when program starts
LUA_SCRIPT_SHAS = None

# # # RATE LIMITER LUA SCRIPTS

# Script to clear all keys matching a pattern
SCRIPT_CLEAR_KEYS = """
        local keys = redis.call('keys', KEYS[1])
        local res = 0
        for i=1,#keys,5000 do
            res = res + redis.call(
                'del', unpack(keys, i, math.min(i+4999, #keys))
            )
        end
        return res
        """

# Script to increment a key and set its expiry
SCRIPT_INCR_EXPIRE = """
        local current
        current = redis.call("incrby",KEYS[1],ARGV[2])
        if tonumber(current) == tonumber(ARGV[2]) then
            redis.call("expire",KEYS[1],ARGV[1])
        end
        return current
    """

# Script to set a key's value and expiry
# args = [value, expiry]
SCRIPT_SET_EXPIRE = """
    local keyttl = redis.call('TTL', KEYS[1])
    local current
    current = redis.call('SET', KEYS[1], ARGV[1])
    if keyttl == -2 then
        redis.call('EXPIRE', KEYS[1], ARGV[2])
    elseif keyttl ~= -1 then
        redis.call('EXPIRE', KEYS[1], keyttl)
    end
    return current
"""

# # # END RATE LIMITER LUA SCRIPTS


async def load_rate_limiter_scripts(redis_conn: aioredis.Redis):
    """
    Load rate limiter Lua scripts into Redis.

    This function needs to be run only once to load the scripts into Redis.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.

    Returns:
        dict: A dictionary containing the SHA1 hashes of the loaded scripts.
    """
    script_clear_keys_sha = await redis_conn.script_load(SCRIPT_CLEAR_KEYS)
    script_incr_expire = await redis_conn.script_load(SCRIPT_INCR_EXPIRE)
    return {
        'script_incr_expire': script_incr_expire,
        'script_clear_keys': script_clear_keys_sha,
    }


async def generic_rate_limiter(
        parsed_limits: List[RateLimitItem],
        key_bits: list,
        redis_conn: aioredis.Redis,
        rate_limit_lua_script_shas=None,
        limit_incr_by=1,
):
    """
    Generic rate limiter function.

    This function checks if a request can be made based on the given rate limits.

    Args:
        parsed_limits (List[RateLimitItem]): List of rate limit items to check against.
        key_bits (list): List of strings to form the rate limit key.
        redis_conn (aioredis.Redis): Redis connection object.
        rate_limit_lua_script_shas (dict, optional): Pre-loaded Lua script SHAs.
        limit_incr_by (int, optional): Amount to increment the limit by. Defaults to 1.

    Returns:
        tuple: (can_request, retry_after, violated_limit_string)
            - can_request (bool): Whether the request can be made.
            - retry_after (int): Seconds to wait before retrying if can_request is False.
            - violated_limit_string (str): String representation of the violated limit.
    """
    if not rate_limit_lua_script_shas:
        rate_limit_lua_script_shas = await load_rate_limiter_scripts(redis_conn)

    redis_storage = AsyncRedisStorage(rate_limit_lua_script_shas, redis_conn)
    custom_limiter = AsyncFixedWindowRateLimiter(redis_storage)

    for each_lim in parsed_limits:
        try:
            # Check if the limit has been hit
            if await custom_limiter.hit(each_lim, limit_incr_by, *[key_bits]) is False:
                window_stats = await custom_limiter.get_window_stats(each_lim, key_bits)
                reset_in = 1 + window_stats[0]
                retry_after = reset_in - int(time.time())
                return False, retry_after, str(each_lim)
        except (
                redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError,
                redis.exceptions.ResponseError,
        ) as exc:
            # Re-raise any Redis-related exceptions
            raise Exception from exc

    # If all checks pass, return True with no retry time and no violated limit
    return True, 0, ''


async def check_rpc_rate_limit(
    parsed_limits: list,
    app_id,
    redis_conn: aioredis.Redis,
    request_payload,
    error_msg,
    logger,
    rate_limit_lua_script_shas=None,
    limit_incr_by=1,
):
    """
    Check rate limit for RPC calls.

    This function applies rate limiting to RPC calls based on the given limits.

    Args:
        parsed_limits (list): List of rate limit items to check against.
        app_id: Identifier for the application making the request.
        redis_conn (aioredis.Redis): Redis connection object.
        request_payload: The payload of the RPC request.
        error_msg: Error message to use if rate limit is exceeded.
        logger: Logger object for logging messages.
        rate_limit_lua_script_shas (dict, optional): Pre-loaded Lua script SHAs.
        limit_incr_by (int, optional): Amount to increment the limit by. Defaults to 1.

    Returns:
        bool: True if the request can be made, False otherwise.

    Raises:
        RPCException: If the rate limit is exceeded.
        Exception: For any other errors during rate limiting.
    """
    key_bits = [
        app_id,
        'eth_call',
    ]  # TODO: add unique elements that can identify a request

    try:
        can_request, retry_after, violated_limit = await generic_rate_limiter(
            parsed_limits,
            key_bits,
            redis_conn,
            rate_limit_lua_script_shas,
            limit_incr_by,
        )
    except Exception as exc:
        logger.opt(exception=True).error(
            'Caught exception on rate limiter operations: {} | Bypassing rate limit check',
            exc,
        )
        raise

    if not can_request:
        exc = RPCException(
            request=request_payload,
            response={},
            underlying_exception=None,
            extra_info=error_msg,
        )
        logger.trace('Rate limit hit, raising exception {}', str(exc))
        raise exc

    return can_request
