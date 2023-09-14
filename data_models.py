from typing import List
from typing import Optional

from pydantic import BaseModel


class RedisConfig(BaseModel):
    host: str
    port: int
    db: int
    password: Optional[str]


class RelayerService(BaseModel):
    host: str
    port: str
    keepalive_secs: int
    keys_ttl: int = 86400


class NodeConfig(BaseModel):
    url: str


class RPCConfig(BaseModel):
    nodes: List[NodeConfig]
    retry: int
    request_timeout: int


class RPCNodeConfig(BaseModel):
    url: str
    rate_limit: str


class ConnectionLimits(BaseModel):
    max_connections: int = 100
    max_keepalive_connections: int = 50
    keepalive_expiry: int = 300


class RPCConfigBase(BaseModel):
    full_nodes: List[RPCNodeConfig]
    archive_nodes: Optional[List[RPCNodeConfig]]
    force_archive_blocks: Optional[int]
    retry: int
    request_time_out: int
    connection_limits: ConnectionLimits


class AnchorChainConfig(BaseModel):
    rpc: RPCConfigBase
    chain_id: int
    polling_interval: int


class RLimit(BaseModel):
    file_descriptors: int


class ReportingConfig(BaseModel):
    slack_url: str
    service_url: str


class Signer(BaseModel):
    address: str
    private_key: str


class SettingsConf(BaseModel):
    relayer_service: RelayerService
    redis: RedisConfig
    rate_limit: str
    anchor_chain: AnchorChainConfig
    reporting: ReportingConfig
    rlimit: RLimit
    protocol_state_address: str
    signers: List[Signer]
    min_signer_balance_eth: int


class GenericTxnIssue(BaseModel):
    accountAddress: str
    issueType: str
    projectId: Optional[str]
    epochBegin: Optional[str]
    epochId: Optional[str]
    extra: Optional[str] = ''


class SignRequest(BaseModel):
    deadline: int
    snapshotCid: str
    epochId: int
    projectId: str


class TxnPayload(BaseModel):
    projectId: str
    snapshotCid: str
    epochId: int
    request: SignRequest
    signature: str
    contractAddress: str
