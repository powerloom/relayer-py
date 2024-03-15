import asyncio
import json
import uuid
from functools import partial
from typing import Any
from typing import Dict
from typing import Optional

import sha3
from aio_pika import Message
from aio_pika.pool import Pool
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
from helpers.redis_keys import timeslot_preference
from init_rabbitmq import get_core_exchange_name
from init_rabbitmq import get_tx_send_q_routing_key
from settings.conf import settings
from utils.default_logger import logger
from utils.helpers import get_rabbitmq_channel
from utils.helpers import get_rabbitmq_robust_connection_async
from utils.redis_conn import RedisPool

# day buffer due to chain migration or other issues
DAY_BUFFER = 23


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
    app.state.aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
    await app.state.aioredis_pool.populate()
    app.state.reader_redis_pool = app.state.aioredis_pool.reader_redis_pool
    app.state.writer_redis_pool = app.state.aioredis_pool.writer_redis_pool
    app.state.rmq_connection_pool = Pool(
        get_rabbitmq_robust_connection_async, max_size=5, loop=asyncio.get_running_loop(),
    )
    app.state.rmq_channel_pool = Pool(
        partial(get_rabbitmq_channel, app.state.rmq_connection_pool), max_size=20,
        loop=asyncio.get_running_loop(),
    )
    # load abi from json file and create contract object
    with open('utils/static/abi.json', 'r') as f:
        app.state.abi = json.load(f)

    # load pairs
    with open('utils/static/pairs.json', 'r') as f:
        app.state.pairs = json.load(f)

    app.state.w3 = AsyncWeb3(
        AsyncHTTPProvider(
            settings.anchor_chain.rpc.full_nodes[0].url,
        ),
    )
    app.state.protocol_state_contract = app.state.w3.eth.contract(
        address=settings.protocol_state_address, abi=app.state.abi,
    )

    app.state.slots_per_day = await app.state.protocol_state_contract.functions.SLOTS_PER_DAY().call()
    app.state.epoch_size = await app.state.protocol_state_contract.functions.EPOCH_SIZE().call()
    app.state.source_chain_block_time = (await app.state.protocol_state_contract.functions.SOURCE_CHAIN_BLOCK_TIME().call()) / 1e4
    app.state.epochs_in_a_day = 86400 // (
        app.state.epoch_size * app.state.source_chain_block_time
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
        version='0.1', chainId=settings.anchor_chain.chain_id,
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
    # get timeslot preference
    timeslot_pref = await request.app.state.reader_redis_pool.get(timeslot_preference(txn_payload.slotId))
    if timeslot_pref:
        timeslot_pref = int(timeslot_pref.decode('utf-8'))
    else:
        timeslot_pref = await request.app.state.protocol_state_contract.functions.getSnapshotterTimeSlot(
            txn_payload.slotId,
        ).call()
        if timeslot_pref:
            await request.app.state.writer_redis_pool.set(
                timeslot_preference(txn_payload.slotId), timeslot_pref,
            )
    if not timeslot_pref:
        return False
    if (txn_payload.epochId % request.app.state.epochs_in_a_day) // (request.app.state.epochs_in_a_day // request.app.state.slots_per_day) != timeslot_pref - 1:
        return False

    current_epoch = txn_payload.epochId
    snapshotter_address = await _get_signer_address(request, txn_payload)
    snapshotter_hash = hash(int(snapshotter_address.lower(), 16))

    current_day = (current_epoch // 720) + DAY_BUFFER

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
    try:
        await submit_snapshot(
            request, req_parsed,
        )

        return JSONResponse(status_code=200, content={'message': 'Submitted Snapshot to relayer!'})

    except Exception as e:
        service_logger.opt(exception=True).error(f'Exception: {e}')
        return JSONResponse(status_code=500, content={'message': 'Invalid request payload!'})
