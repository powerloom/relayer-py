import asyncio
import json
import os
import time
import uuid
from typing import Any
from typing import Dict
from typing import Optional

import aiorwlock
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from web3 import AsyncHTTPProvider
from web3 import AsyncWeb3

from data_models import TxnPayload
from settings.conf import settings
from utils.rate_limiter import load_rate_limiter_scripts
from utils.redis_conn import RedisPool
from utils.transaction_utils import write_transaction


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
async def request_middleware(request: Request, call_next: Any) -> Optional[Dict]:
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
    app.state.aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
    await app.state.aioredis_pool.populate()
    app.state.reader_redis_pool = app.state.aioredis_pool.reader_redis_pool
    app.state.writer_redis_pool = app.state.aioredis_pool.writer_redis_pool
    app.state.rate_limit_lua_script_shas = await load_rate_limiter_scripts(app.state.writer_redis_pool)

    app.state._rwlock = aiorwlock.RWLock()
    # open pid.json and find the index of pid of the worker
    worker_pid = os.getpid()

    with open('pid.json', 'r') as pid_file:
        data = json.load(pid_file)

        # find the index of the worker in the list
        worker_idx = data.index(worker_pid)

        app.state.signer_account = settings.signers[worker_idx].address
        app.state.signer_pkey = settings.signers[worker_idx].private_key

        # load abi from json file and create contract object
        with open('utils/static/abi.json', 'r') as f:
            abi = json.load(f)
        app.state.w3 = AsyncWeb3(AsyncHTTPProvider(settings.anchor_chain.rpc.full_nodes[0].url))

        app.state.protocol_state_contract = app.state.w3.eth.contract(
            address=settings.protocol_state_address, abi=abi,
        )
        app.state.signer_nonce = await app.state.w3.eth.get_transaction_count(app.state.signer_account)

        # check if signer has enough balance
        balance = await app.state.w3.eth.get_balance(app.state.signer_account)
        # convert to eth
        balance = app.state.w3.from_wei(balance, 'ether')
        if balance < settings.min_signer_balance_eth:
            service_logger.error(f'Signer {app.state.signer_account} has insufficient balance: {balance} ETH')
            exit(1)

        service_logger.info(
            f'Started worker {worker_idx}, with signer_account: {app.state.signer_account}, signer_nonce: {app.state.signer_nonce}',
        )


# submitSnapshot
@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    wait=wait_random_exponential(multiplier=1, max=10),
    stop=stop_after_attempt(3),
)
async def submit_snapshot(request: Request, txn_payload: TxnPayload):
    """
    Submit Snapshot
    """
    async with request.app.state._rwlock.writer_lock:
        _nonce = request.app.state.signer_nonce
        try:
            tx_hash = await write_transaction(
                request.app.state.w3,
                request.app.state.signer_account,
                request.app.state.signer_pkey,
                request.app.state.protocol_state_contract,
                txn_payload.contractAddress,
                'submitSnapshot',
                _nonce,
                txn_payload.snapshotCid,
                txn_payload.epochId,
                txn_payload.projectId,
                (
                    txn_payload.request.deadline, txn_payload.request.snapshotCid,
                    txn_payload.request.epochId, txn_payload.request.projectId,
                ),
                txn_payload.signature,
            )

            request.app.state.signer_nonce += 1

            service_logger.info(f'tx_hash: {tx_hash}')

        except Exception as e:
            service_logger.error(f'Exception: {e}')

            if 'nonce' in str(e):
                # sleep for 10 seconds and reset nonce
                time.sleep(10)
                request.app.state.signer_nonce = await request.app.state.w3.eth.get_transaction_count(
                    request.app.state.signer_account,
                )
                service_logger.info(f'nonce reset to: {request.app.state.signer_nonce}')
                raise

    receipt = await request.app.state.w3.eth.wait_for_transaction_receipt(tx_hash)

    if receipt['status'] == 0:
        service_logger.info(f'tx_hash: {tx_hash} failed, receipt: {receipt}, txn_payload: {txn_payload}')
    else:
        service_logger.info(f'tx_hash: {tx_hash} succeeded')


@app.post('/submitSnapshot')
async def submit(
    request: Request,
    req_parsed: TxnPayload,
    response: Response,
):
    """
    Submit Snapshot
    """
    asyncio.ensure_future(submit_snapshot(request, req_parsed))

    return JSONResponse(status_code=200, content={'message': 'Submitted Snapshot to relayer!'})
