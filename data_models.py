from typing import List
from typing import Optional
from typing import Union

from pydantic import BaseModel


class RedisConfig(BaseModel):
    """Configuration for Redis connection."""
    host: str
    port: int
    db: int
    password: Optional[str]


class RelayerService(BaseModel):
    """Configuration for Relayer Service."""
    host: str
    port: str
    keepalive_secs: int
    keys_ttl: int = 86400  # Default TTL for keys (1 day)


class NodeConfig(BaseModel):
    """Configuration for a single node."""
    url: str


class RPCConfig(BaseModel):
    """Configuration for RPC connections."""
    nodes: List[NodeConfig]
    retry: int
    request_timeout: int


class RPCNodeConfig(BaseModel):
    """Configuration for an RPC node with rate limiting."""
    url: str
    rate_limit: str


class ConnectionLimits(BaseModel):
    """Limits for connection pooling."""
    max_connections: int = 100
    max_keepalive_connections: int = 50
    keepalive_expiry: int = 300  # in seconds


class RPCConfigBase(BaseModel):
    """Base configuration for RPC nodes including full and archive nodes."""
    full_nodes: List[RPCNodeConfig]
    archive_nodes: Optional[List[RPCNodeConfig]]
    force_archive_blocks: Optional[int]
    retry: int
    request_time_out: int
    connection_limits: ConnectionLimits


class AnchorChainConfig(BaseModel):
    """Configuration for the anchor chain."""
    rpc: RPCConfigBase
    chain_id: int
    polling_interval: int


class RLimit(BaseModel):
    """Resource limits configuration."""
    file_descriptors: int


class ReportingConfig(BaseModel):
    """Configuration for reporting services."""
    slack_url: str
    service_url: str


class Signer(BaseModel):
    """Configuration for a signer."""
    address: str
    private_key: str


class RabbitMQConfig(BaseModel):
    """Configuration for RabbitMQ exchange."""
    exchange: str


class RabbitMQ(BaseModel):
    """Configuration for RabbitMQ connection."""
    user: str
    password: str
    host: str
    port: int


class SettingsConf(BaseModel):
    """Main configuration settings for the application."""
    relayer_service: RelayerService
    rabbitmq: RabbitMQ
    redis: RedisConfig
    rate_limit: str
    anchor_chain: AnchorChainConfig
    reporting: ReportingConfig
    rlimit: RLimit
    protocol_state_address: str
    signers: List[Signer]
    min_signer_balance_eth: int
    auth_token: str


class ProcessorWorkerDetails(BaseModel):
    """Details of a processor worker."""
    unique_name: str
    pid: Union[None, int] = None


class GenericTxnIssue(BaseModel):
    """Representation of a generic transaction issue."""
    accountAddress: str
    issueType: str
    projectId: Optional[str]
    epochBegin: Optional[str]
    epochId: Optional[str]
    extra: Optional[str] = ''


class SignRequest(BaseModel):
    """Request model for signing operations."""
    slotId: int
    deadline: int
    snapshotCid: str
    epochId: int
    projectId: str


class BatchSizeRequest(BaseModel):
    """Request model for batch size operations."""
    dataMarketAddress: str
    batchSize: int
    epochID: int
    authToken: str


class BatchSubmissionRequest(BaseModel):
    """Request model for batch submission operations."""
    dataMarketAddress: str
    batchCID: str
    epochID: int
    projectIDs: List[str]
    snapshotCIDs: List[str]
    finalizedCIDsRootHash: str
    authToken: str


class ErrorMessage(BaseModel):
    """Request model for error message."""
    error: str
    raw_payload: str


class UpdateRewardsRequest(BaseModel):
    """Request model for update rewards operations."""
    dataMarketAddress: str
    slotIDs: List[int]
    submissionsList: List[int]
    day: int
    eligibleNodes: int
    authToken: str


class EndBatchRequest(BaseModel):
    """Request model for ending a batch."""
    dataMarketAddress: str
    epochID: int


class WalletRegistrationRequest(BaseModel):
    """Request model for wallet registration."""
    walletAddress: str
    allowed: bool
    token: str
