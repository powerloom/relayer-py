 # PowerLoom Relayer Service (Python)

A high-performance **EVM blockchain relayer service** for the PowerLoom Protocol ecosystem. This service handles batch submissions, transaction queuing, and on-chain consensus operations across multiple ecosystem components including decentralized sequencers, centralized sequencers, reward managers, identity managers, and other protocol services on Ethereum-compatible networks.

## Overview

The relayer service acts as a bridge between off-chain data processing and on-chain settlement. It provides REST APIs for submitting batch data and rewards updates, manages transaction queuing via RabbitMQ, and processes blockchain transactions using multiple concurrent workers.

### Key Features

- **FastAPI-based REST API** for batch submissions and rewards updates
- **Transaction queuing** with RabbitMQ for reliable message delivery
- **Multi-worker architecture** using PM2 process management
- **Redis integration** for caching and state management
- **Web3 integration** for EVM blockchain interactions
- **Comprehensive retry logic** with exponential backoff
- **Health monitoring** and logging with structured logs
- **Docker containerization** for easy deployment
- **Multi-component support** for sequencers, reward managers, identity managers, and other protocol services

## Architecture

```
┌─────────────────┐    ┌─────────────┐    ┌─────────────────┐
│   FastAPI App   │────│  RabbitMQ   │────│   Tx Workers    │
│                 │    │   Queue     │    │                 │
│ • /submitBatch  │    └─────────────┘    │ • Batch Txns    │
│ • /submitRewards│                       │ • Rewards Txns  │
└─────────────────┘                       └─────────────────┘
         │                                        │
         └──────────────────┬─────────────────────┘
                            │
                 ┌─────────────────┐
                 │  EVM Blockchain │
                 │   (Web3.py)     │
                 └─────────────────┘
```

### Components

- **API Layer** (`relayer.py`): FastAPI application handling HTTP requests
- **Transaction Workers** (`tx_worker.py`): Background workers processing queued transactions
- **Message Queue**: RabbitMQ for reliable transaction queuing
- **Cache/Storage**: Redis for temporary data storage and coordination
- **Process Management**: PM2 for managing multiple worker processes

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.8+ (for local development)
- Poetry (for dependency management)

### Using Docker Compose

1. **Clone and navigate to the project:**
   ```bash
   cd relayer-py
   ```

2. **Build and run:**
   ```bash
   ./build.sh
   ```

This will:
- Build the Docker image
- Start Redis, RabbitMQ, and the relayer service
- Expose the API on port 8080

### Local Development

1. **Install dependencies:**
   ```bash
   poetry install
   ```

2. **Configure settings:**
   ```bash
   cp settings/settings.example.json settings/settings.json
   # Edit settings.json with your configuration
   ```

   > [!TIP]
   > **Configuration**: The `settings/settings.json` file is required for the service to run. Copy from `settings.example.json` and update with your network-specific values, RPC endpoints, and signer credentials.

3. **Run the service:**
   ```bash
   # Start the API server
   poetry run python -m gunicorn_launcher

   # In another terminal, start transaction workers
   poetry run python -m tx_worker
   ```

## Configuration

The service is configured via `settings/settings.json`. Key configuration sections:

### Relayer Service
```json
{
  "relayer_service": {
    "host": "0.0.0.0",
    "port": 8080,
    "keys_ttl": 86400,
    "keepalive_secs": 600
  }
}
```

### Blockchain Configuration
```json
{
  "anchor_chain": {
    "rpc": {
      "full_nodes": [
        {
          "url": "<powerloom-chain-rpc-url>",
          "rate_limit": "100000000/day;18000/minute;300/second"
        }
      ],
      "retry": 5,
      "request_time_out": 5
    },
    "chain_id": 104,
    "polling_interval": 2
  }
}
```

### Signers Configuration
```json
{
  "signers": [
    {
      "address": "signer-address",
      "private_key": "signer-private-key"
    }
  ],
  "min_signer_balance_eth": 1
}
```

> [!WARNING]
> **Security Critical**: Never commit real private keys to version control. Use environment variables or secure secret management systems in production. The `private_key` field should only contain placeholder values in configuration files.

## Data Market API Endpoints for sequencers

### Submit Batch Size
```http
POST /submitBatchSize
Content-Type: application/json

{
  "epochID": "epoch_123",
  "batchSize": 100,
  "authToken": "your-auth-token"
}
```

### Submit Batch
```http
POST /submitSubmissionBatch
Content-Type: application/json

{
  "epochID": "epoch_123",
  "dataMarketAddress": "0x...",
  "batchCID": "Qm...",
  "authToken": "your-auth-token"
}
```

### Submit Rewards Update
```http
POST /submitUpdateRewards
Content-Type: application/json

{
  "dataMarketAddress": "0x...",
  "slotIds": [1, 2, 3],
  "submissions": [...],
  "day": 123,
  "eligibleNodes": [...],
  "authToken": "your-auth-token"
}
```

**Note**: This relayer service is used by multiple Powerloom Protocol components including sequencers, reward managers, and identity managers. The API endpoints handle various transaction types across the ecosystem.

> [!NOTE]
> **Authentication**: All API endpoints require a valid `authToken` that must match the `auth_token` configured in your settings. This token is shared across ecosystem components for secure communication.

## Deployment

### Production Deployment

1. **Configure environment variables:**
   ```bash
   export VPA_SIGNER_ADDRESSES="0xaddr1,0xaddr2"
   ```

   > [!IMPORTANT]
   > **Environment Variables**: Set `VPA_SIGNER_ADDRESSES` to a comma-separated list of all signer addresses. This determines the number of transaction worker processes that will be spawned.

2. **Build and deploy:**
   ```bash
   docker build -t powerloom-relayer .
   docker-compose up -d
   ```

### PM2 Process Management

The service uses PM2 for process management with the following configuration:
- **relayer-api**: FastAPI server instance
- **tx_worker**: Multiple transaction worker instances (one per signer)

Process count is automatically determined by the number of configured signers.

## Development

### Project Structure

```
relayer-py/
├── relayer.py              # Main FastAPI application
├── tx_worker.py            # Transaction processing workers
├── data_models.py          # Pydantic data models
├── settings/               # Configuration files
├── utils/                  # Utility functions
├── helpers/                # Helper modules
├── auth/                   # Authentication modules
├── Dockerfile              # Container definition
├── docker-compose.yaml     # Multi-service orchestration
├── pm2.config.js          # Process management config
└── pyproject.toml         # Python dependencies
```

### Testing

```bash
# Run tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=. --cov-report=html
```

### Code Quality

```bash
# Run linter
poetry run pylint relayer tx_worker

# Format code
poetry run black .
poetry run isort .
```

## Monitoring

### Logs

Logs are structured using Loguru and can be accessed via:

```bash
# Docker logs
docker logs relayer

# PM2 logs
pm2 logs
```

### Health Checks

The service includes built-in health monitoring:
- Redis connection health
- RabbitMQ connectivity
- Blockchain RPC node availability
- Transaction processing status

## Troubleshooting

> [!TIP]
> **Logs First**: When troubleshooting, always check the service logs first using `docker logs relayer` or `pm2 logs`. The structured logs will provide detailed error information and request IDs for correlation.

### Common Issues

1. **Connection refused to Redis/RabbitMQ:**
   - Ensure services are running: `docker-compose ps`
   - Check network connectivity between containers

2. **Transaction failures:**
   - Verify signer private keys and addresses
   - Check signer ETH balance (minimum 1 ETH required)
   - Review blockchain RPC configuration

3. **Authentication errors:**
   - Verify `auth_token` in settings matches API requests

### Debugging

Enable debug logging by setting environment variable:
```bash
export LOG_LEVEL=DEBUG
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

Apache License 2.0