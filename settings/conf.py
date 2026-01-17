import os
import json
from typing import List
from data_models import SettingsConf, RPCNodeConfig, ConnectionLimits, RPCConfigBase, RelayerService, RedisConfig, RabbitMQ, ReportingConfig, RLimit, AnchorChainConfig, Signer


def load_settings_from_env() -> SettingsConf:
    """Load settings from environment variables with fallback to JSON file."""

    # Try to load from JSON first if it exists
    if os.path.exists('settings/settings.json'):
        try:
            return SettingsConf.parse_file('settings/settings.json')
        except Exception as e:
            print(f"Warning: Could not parse settings.json, falling back to env vars: {e}")

    # Load from environment variables
    vpa_signer_addresses = os.getenv("VPA_SIGNER_ADDRESSES", "")
    vpa_signer_private_keys = os.getenv("VPA_SIGNER_PRIVATE_KEYS", "")
    powerloom_rpc_nodes = os.getenv("POWERLOOM_RPC_NODES", "")
    if not powerloom_rpc_nodes:
        raise ValueError(
            "POWERLOOM_RPC_NODES environment variable is required. "
            "Set it to your RPC endpoint URL(s)."
        )
    new_protocol_state_contract = os.getenv("NEW_PROTOCOL_STATE_CONTRACT", "0xC9e7304f719D35919b0371d8B242ab59E0966d63")

    # Parse RPC nodes
    rpc_nodes: List[RPCNodeConfig] = []
    if powerloom_rpc_nodes.startswith("["):
        try:
            nodes = json.loads(powerloom_rpc_nodes)
            rpc_nodes = [RPCNodeConfig(url=url, rate_limit="100000000/day;18000/minute;300/second") for url in nodes]
        except json.JSONDecodeError:
            rpc_nodes = [RPCNodeConfig(url=powerloom_rpc_nodes, rate_limit="100000000/day;18000/minute;300/second")]
    elif "," in powerloom_rpc_nodes:
        nodes = [url.strip() for url in powerloom_rpc_nodes.split(",")]
        rpc_nodes = [RPCNodeConfig(url=url, rate_limit="100000000/day;18000/minute;300/second") for url in nodes]
    else:
        rpc_nodes = [RPCNodeConfig(url=powerloom_rpc_nodes, rate_limit="100000000/day;18000/minute;300/second")]

    # Parse signers
    signers: List[Signer] = []
    if vpa_signer_addresses and vpa_signer_private_keys:
        addresses = [addr.strip() for addr in vpa_signer_addresses.split(",")]
        private_keys = [key.strip() for key in vpa_signer_private_keys.split(",")]

        if len(addresses) != len(private_keys):
            raise ValueError(f"Mismatch between {len(addresses)} addresses and {len(private_keys)} private keys")

        for addr, priv_key in zip(addresses, private_keys):
            if addr and priv_key and addr != "0x0000000000000000000000000000000000000000":
                signers.append(Signer(address=addr, private_key=priv_key))

    # If no signers found, raise error - signers must be configured
    if not signers:
        raise ValueError(
            "No signers configured. Either provide settings/settings.json with signers array, "
            "or set VPA_SIGNER_ADDRESSES and VPA_SIGNER_PRIVATE_KEYS environment variables."
        )

    return SettingsConf(
        relayer_service=RelayerService(
            host="0.0.0.0",
            port="8080",
            keepalive_secs=600,
            keys_ttl=86400
        ),
        rate_limit="100000/day;200/minute;20/second",
        redis=RedisConfig(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            password=os.getenv("REDIS_PASSWORD")
        ),
        rabbitmq=RabbitMQ(
            user="guest",
            password="guest",
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            port=int(os.getenv("RABBITMQ_PORT", "5672"))
        ),
        reporting=ReportingConfig(
            slack_url="",
            service_url=""
        ),
        anchor_chain=AnchorChainConfig(
            rpc=RPCConfigBase(
                full_nodes=rpc_nodes,
                archive_nodes=None,
                force_archive_blocks=None,
                retry=int(os.getenv("RPC_RETRY", "5")),
                request_time_out=int(os.getenv("RPC_REQUEST_TIMEOUT_S", "5")),
                connection_limits=ConnectionLimits()
            ),
            chain_id=int(os.getenv("ANCHOR_CHAIN_ID")),
            polling_interval=2
        ),
        rlimit=RLimit(file_descriptors=40960),
        protocol_state_address=new_protocol_state_contract,
        signers=signers,
        min_signer_balance_eth=float(os.getenv("MIN_SIGNER_BALANCE_ETH", "0")),  # Default 0 for devnet (disable check)
        auth_token=os.getenv("AUTH_TOKEN", ""),
        tx_queue_wait_for_receipt=False
    )


# Load settings from environment variables
settings: SettingsConf = load_settings_from_env()
