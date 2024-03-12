import asyncio
from functools import partial
import json
import os
import time
import uuid
from typing import Any
from typing import Dict
from typing import Optional
from aio_pika import Message
from aio_pika.pool import Pool
import sha3
from eip712_structs import EIP712Struct
from eip712_structs import make_domain
from eip712_structs import String
from eip712_structs import Uint
from fastapi import FastAPI
from fastapi import Request as FastAPIRequest
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from web3 import AsyncHTTPProvider
from web3 import AsyncWeb3

from data_models import TxnPayload
from init_rabbitmq import get_core_exchange_name, get_tx_send_q_routing_key
from settings.conf import settings
from utils.default_logger import logger
from utils.helpers import get_rabbitmq_channel, get_rabbitmq_robust_connection_async
from utils.rate_limiter import load_rate_limiter_scripts
from utils.redis_conn import RedisPool
from utils.transaction_utils import write_transaction


class Request(EIP712Struct):
    slotId = Uint()
    deadline = Uint()
    snapshotCid = String()
    epochId = Uint()
    projectId = String()


def keccak_hash(x):
    return sha3.keccak_256(x).digest()


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
    app.state.rmq_connection_pool = Pool(get_rabbitmq_robust_connection_async, max_size=5, loop=asyncio.get_running_loop())
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
async def submit_snapshot(request: FastAPIRequest, txn_payload: TxnPayload, protocol_state_contract: Any):
    """
    Submit Snapshot
    """
    async with request.app.state.rmq_channel_pool.acquire() as channel:
        exchange = await channel.get_exchange(name=get_core_exchange_name())
        queue_name, routing_key = get_tx_send_q_routing_key()
        await exchange.publish(
            routing_key=routing_key,
            message=Message(txn_payload.json().encode('utf-8')),
        )


async def get_protocol_state_contract(request: FastAPIRequest, contract_address: str):
    """
    Get Protocol State Contract
    """
    # validate contract address
    if not request.app.state.w3.is_address(contract_address):
        return None

    # get contract object
    if request.app.state.w3.to_checksum_address(contract_address) not in request.app.state.protocol_state_contract_instance_mapping:
        contract = request.app.state.w3.eth.contract(
            address=contract_address, abi=app.state.abi,
        )
        request.app.state.protocol_state_contract_instance_mapping[
            request.app.state.w3.to_checksum_address(contract_address)
        ] = contract
        return contract
    else:
        return request.app.state.protocol_state_contract_instance_mapping[
            request.app.state.w3.to_checksum_address(contract_address)
        ]


async def _get_signer_address(request: FastAPIRequest, txn_payload: TxnPayload):
    """
    Get Signer Address
    """

    request_ = txn_payload.request.dict()
    signature = bytes.fromhex(txn_payload.signature[2:])
    signature_request = Request(
        slotId=request_['slotId'],
        deadline=request_['deadline'],
        snapshotCid=request_['snapshotCid'],
        epochId=request_['epochId'],
        projectId=request_['projectId'],
    )

    domain_separator = make_domain(
        name='PowerloomProtocolContract',
        version='0.1', chainId=103,
        verifyingContract=request.app.state.w3.to_checksum_address(
            txn_payload.contractAddress,
        ),
    )

    signable_bytes = signature_request.signable_bytes(domain_separator)
    message_hash = keccak_hash(signable_bytes)
    signer_address = request.app.state.w3.eth.account._recover_hash(
        message_hash, signature=signature,
    )
    return signer_address


async def _check(request: FastAPIRequest, txn_payload: TxnPayload):

    current_epoch = txn_payload.epochId
    snapshotter_address = await _get_signer_address(request, txn_payload)
    snapshotter_hash = hash(int(snapshotter_address.lower(), 16))

    current_day = (current_epoch // 720)+1

    pair_idx = (
        current_epoch + snapshotter_hash + txn_payload.slotId +
        current_day
    ) % len(request.app.state.pairs)
    # projectId check
    if request.app.state.pairs[pair_idx].lower() not in txn_payload.projectId.lower():
        return False
    return True


@app.post('/submitSnapshot')
async def submit(
    request: FastAPIRequest,
    req_parsed: TxnPayload,
    response: Response,
):
    """
    Submit Snapshot
    """

    if not await _check(request, req_parsed):
        return JSONResponse(status_code=200, content={'message': 'Snapshot received but not submitted to chain because _check failed!'})

    # TODO: remove protocol state contract call
    try:
        protocol_state_contract = await get_protocol_state_contract(request, req_parsed.contractAddress)
        gas_estimate = await protocol_state_contract.functions.submitSnapshot(
            req_parsed.slotId,
            req_parsed.snapshotCid,
            req_parsed.epochId,
            req_parsed.projectId,
            (
                req_parsed.request.slotId, req_parsed.request.deadline,
                req_parsed.request.snapshotCid, req_parsed.request.epochId,
                req_parsed.request.projectId,
            ),
            req_parsed.signature,
        ).estimate_gas(
            {
                'from': request.app.state.signer_account,
            },
        )

        asyncio.ensure_future(
            submit_snapshot(
                request, req_parsed, protocol_state_contract,
            ),
        )

        return JSONResponse(status_code=200, content={'message': f'Submitted Snapshot to relayer, estimated gas usage is: {gas_estimate} wei'})

    except Exception as e:
        service_logger.opt(exception=True).error(f'Exception: {e}')
        return JSONResponse(status_code=500, content={'message': 'Invalid request payload!'})
