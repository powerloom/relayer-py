import asyncio
import uuid
from functools import partial
from typing import Any
from typing import Dict
from typing import Optional

from aio_pika import Message
from aio_pika.pool import Pool
from fastapi import FastAPI
from fastapi import Request as FastAPIRequest
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential

from data_models import BatchSizeRequest
from data_models import BatchSubmissionRequest
from data_models import UpdateRewardsRequest
from helpers.redis_keys import epoch_batch_size
from init_rabbitmq import get_core_exchange_name
from init_rabbitmq import get_tx_check_q_routing_key
from settings.conf import settings
from utils.default_logger import logger
from utils.helpers import get_rabbitmq_channel
from utils.helpers import get_rabbitmq_robust_connection_async
from utils.redis_conn import RedisPool

# Initialize logger for the relayer service
service_logger = logger.bind(
    service='PowerLoom|OnChainConsensus|Relayer',
)

# Setup CORS origins
origins = ['*']

# Initialize FastAPI app
app = FastAPI()
app.logger = service_logger

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.middleware('http')
async def request_middleware(
    request: FastAPIRequest,
    call_next: Any,
) -> Optional[Dict]:
    """
    Middleware to handle incoming HTTP requests.

    This middleware function logs the start and end of each request,
    generates a unique request ID, and handles any exceptions that occur
    during request processing.

    Args:
        request (FastAPIRequest): The incoming request object.
        call_next (Any): The next function in the middleware chain.

    Returns:
        Optional[Dict]: The response dictionary, or None if an error occurs.
    """
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id

    with service_logger.contextualize(request_id=request_id):
        service_logger.info('Request started for: {}', request.url)
        try:
            response = await call_next(request)
        except Exception as ex:
            service_logger.opt(exception=True).error(f'Request failed: {ex}')
            response = JSONResponse(
                content={
                    'info': {
                        'success': False,
                        'response': 'Internal Server Error',
                    },
                    'request_id': request_id,
                },
                status_code=500,
            )
        finally:
            response.headers['X-Request-ID'] = request_id
            service_logger.info('Request ended')
            return response


@app.on_event('startup')
async def startup_boilerplate():
    """
    Initialize necessary components on application startup.

    This function sets up Redis pools, RabbitMQ connection pools,
    and RabbitMQ channel pools for the application to use.
    """
    # Initialize Redis pools
    app.state.aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
    await app.state.aioredis_pool.populate()
    app.state.reader_redis_pool = app.state.aioredis_pool.reader_redis_pool
    app.state.writer_redis_pool = app.state.aioredis_pool.writer_redis_pool

    # Initialize RabbitMQ connection pool
    app.state.rmq_connection_pool = Pool(
        get_rabbitmq_robust_connection_async,
        max_size=5,
        loop=asyncio.get_running_loop(),
    )

    # Initialize RabbitMQ channel pool
    app.state.rmq_channel_pool = Pool(
        partial(get_rabbitmq_channel, app.state.rmq_connection_pool),
        max_size=20,
        loop=asyncio.get_running_loop(),
    )


@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    wait=wait_random_exponential(multiplier=1, max=10),
    stop=stop_after_attempt(3),
)
async def submit_batch(
    request: FastAPIRequest,
    batch_payload: BatchSubmissionRequest,
):
    """
    Submit a snapshot to the RabbitMQ exchange.

    This function publishes a batch submission request to a RabbitMQ exchange.
    It uses a retry decorator to handle potential failures.

    Args:
        request (FastAPIRequest): The incoming request object.
        batch_payload (BatchSubmissionRequest): The batch payload to be submitted.

    Raises:
        Exception: If the submission fails after all retry attempts.
    """
    async with request.app.state.rmq_channel_pool.acquire() as channel:
        exchange = await channel.get_exchange(name=get_core_exchange_name())
        queue_name, routing_key = get_tx_check_q_routing_key()
        await exchange.publish(
            routing_key=routing_key,
            message=Message(batch_payload.json().encode('utf-8')),
        )


@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    wait=wait_random_exponential(multiplier=1, max=10),
    stop=stop_after_attempt(3),
)
async def submit_update_rewards(
    request: FastAPIRequest,
    update_rewards_payload: UpdateRewardsRequest,
):
    """
    Submit a rewards update request to the RabbitMQ exchange.

    This function publishes a rewards update request to a RabbitMQ exchange for processing.
    It uses a retry decorator to handle potential failures in the submission process.

    Args:
        request (FastAPIRequest): The incoming FastAPI request object containing app state
        update_rewards_payload (UpdateRewardsRequest): The payload containing rewards update data
            including slot IDs, submissions list, day and eligible nodes

    Raises:
        Exception: If the submission fails after all retry attempts
    """
    # Acquire a channel from the RabbitMQ channel pool
    async with request.app.state.rmq_channel_pool.acquire() as channel:
        # Get the core exchange to publish messages
        exchange = await channel.get_exchange(name=get_core_exchange_name())

        # Get queue name and routing key for transaction checking queue
        queue_name, routing_key = get_tx_check_q_routing_key()

        # Publish the encoded payload to the exchange
        await exchange.publish(
            routing_key=routing_key,
            message=Message(update_rewards_payload.json().encode('utf-8')),
        )


@app.post('/submitBatchSize')
async def submit_batch_size(
    request: FastAPIRequest,
    req_parsed: BatchSizeRequest,
    response: Response,
):
    """
    Submit batch size to Redis.

    This endpoint receives a batch size request and stores it in Redis.

    Args:
        request (FastAPIRequest): The incoming request object.
        req_parsed (BatchSizeRequest): The parsed batch size request.
        response (Response): The response object.

    Returns:
        JSONResponse: A JSON response indicating success or failure.
    """
    if req_parsed.authToken != settings.auth_token:
        return JSONResponse(
            status_code=401,
            content={'message': 'Unauthorized'},
        )
    service_logger.debug('Received batch size request: {}', req_parsed)
    try:
        # Store batch size in Redis
        await request.app.state.writer_redis_pool.set(
            epoch_batch_size(req_parsed.epochID),
            req_parsed.batchSize,
        )
        return JSONResponse(
            status_code=200,
            content={'message': 'Submitted Batch Size to relayer!'},
        )
    except Exception as e:
        service_logger.opt(exception=True).error(f'Exception: {e}')
        return JSONResponse(
            status_code=500,
            content={'message': 'Invalid request payload!'},
        )


@app.post('/submitSubmissionBatch')
async def submit_batch_submission(
    request: FastAPIRequest,
    req_parsed: BatchSubmissionRequest,
    response: Response,
):
    """
    Submit a snapshot batch.

    This endpoint receives a batch submission request and forwards it to the
    submit_batch function for processing.

    Args:
        request (FastAPIRequest): The incoming request object.
        req_parsed (BatchSubmissionRequest): The parsed batch submission request.
        response (Response): The response object.

    Returns:
        JSONResponse: A JSON response indicating success or failure.
    """
    if req_parsed.authToken != settings.auth_token:
        return JSONResponse(
            status_code=401,
            content={'message': 'Unauthorized'},
        )
    service_logger.debug(
        'Received batch submission request for epoch {} and data market {} and batch cid {}',
        req_parsed.epochID,
        req_parsed.dataMarketAddress,
        req_parsed.batchCID,
    )
    try:
        await submit_batch(request, req_parsed)
        return JSONResponse(
            status_code=200,
            content={'message': 'Submitted Snapshot to relayer!'},
        )
    except Exception as e:
        service_logger.opt(exception=True).error(f'Exception: {e}')
        return JSONResponse(
            status_code=500,
            content={'message': 'Invalid request payload!'},
        )


@app.post('/submitUpdateRewards')
async def submit_update_rewards_submission(
    request: FastAPIRequest,
    req_parsed: UpdateRewardsRequest,
    response: Response,
):
    """
    Submit an update rewards request.

    This endpoint receives an update rewards request and forwards it to the
    submit_update_rewards function for processing. It handles authentication
    and validation of the request.

    Args:
        request (FastAPIRequest): The incoming request object.
        req_parsed (UpdateRewardsRequest): The parsed update rewards request containing
            data market address, slot IDs, submissions list, day and eligible nodes.
        response (Response): The response object.

    Returns:
        JSONResponse: A JSON response indicating success (200) or failure (401/500).
    """
    # Verify authentication token
    if req_parsed.authToken != settings.auth_token:
        return JSONResponse(
            status_code=401,
            content={'message': 'Unauthorized'},
        )

    # Log the incoming request
    service_logger.debug('Received update rewards request: {}', req_parsed)

    try:
        # Process the update rewards request
        await submit_update_rewards(request, req_parsed)
        return JSONResponse(
            status_code=200,
            content={'message': 'Submitted Update Rewards to relayer!'},
        )
    except Exception as e:
        # Log any errors that occur during processing
        service_logger.opt(exception=True).error(f'Exception: {e}')
        return JSONResponse(
            status_code=500,
            content={'message': 'Invalid request payload!'},
        )
