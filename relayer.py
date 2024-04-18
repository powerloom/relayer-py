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

from data_models import TxnPayload
from init_rabbitmq import get_core_exchange_name
from init_rabbitmq import get_tx_check_q_routing_key
from utils.default_logger import logger
from utils.helpers import get_rabbitmq_channel
from utils.helpers import get_rabbitmq_robust_connection_async


service_logger = logger.bind(
    service='PowerLoom|OnChainConsensus|Relayer',
)

# setup CORS origins stuff
origins = ['*']

app = FastAPI()
app.logger = service_logger


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.middleware('http')
async def request_middleware(request: FastAPIRequest, call_next: Any) -> Optional[Dict]:
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
                    'info':
                        {
                            'success': False,
                            'response': 'Internal Server Error',
                        },
                    'request_id': request_id,
                }, status_code=500,
            )

        finally:
            response.headers['X-Request-ID'] = request_id
            service_logger.info('Request ended')
            return response


@app.on_event('startup')
async def startup_boilerplate():
    app.state.rmq_connection_pool = Pool(
        get_rabbitmq_robust_connection_async, max_size=5, loop=asyncio.get_running_loop(),
    )
    app.state.rmq_channel_pool = Pool(
        partial(get_rabbitmq_channel, app.state.rmq_connection_pool), max_size=20,
        loop=asyncio.get_running_loop(),
    )


# submitSnapshot
@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    wait=wait_random_exponential(multiplier=1, max=10),
    stop=stop_after_attempt(3),
)
async def submit_snapshot(request: FastAPIRequest, txn_payload: TxnPayload):
    """
    Submit Snapshot
    """
    async with request.app.state.rmq_channel_pool.acquire() as channel:
        exchange = await channel.get_exchange(name=get_core_exchange_name())
        queue_name, routing_key = get_tx_check_q_routing_key()
        await exchange.publish(
            routing_key=routing_key,
            message=Message(txn_payload.json().encode('utf-8')),
        )


@app.post('/submitSnapshot')
async def submit(
    request: FastAPIRequest,
    req_parsed: TxnPayload,
    response: Response,
):
    """
    Submit Snapshot
    """
    try:
        asyncio.ensure_future(
            submit_snapshot(
                request, req_parsed,
            ),
        )

        return JSONResponse(status_code=200, content={'message': 'Submitted Snapshot to relayer!'})

    except Exception as e:
        service_logger.opt(exception=True).error(f'Exception: {e}')
        return JSONResponse(status_code=500, content={'message': 'Invalid request payload!'})
