# Troubleshooting Guide

## Missing Modules Error

If you see errors like:
```
ModuleNotFoundError: No module named 'httpx'
ModuleNotFoundError: No module named 'web3'
```

### Solution 1: Install Dependencies (Recommended)

```bash
cd tests/rpc_simulator

# Using pip
pip install -r requirements.txt

# Or using pip3
pip3 install -r requirements.txt

# Or if you have both Python 2 and 3
python3 -m pip install -r requirements.txt
```

### Solution 2: Use Setup Script

```bash
cd tests/rpc_simulator
./setup.sh
```

### Solution 3: Use Docker (No Setup Required)

```bash
cd tests/rpc_simulator
docker-compose up -d
```

## Python Version Issues

If you get compatibility errors:

```bash
# Check Python version
python3 --version

# Should be Python 3.8 or higher
# If not, install newer Python version
```

## Permission Denied

If you see "Permission denied" when running scripts:

```bash
chmod +x setup.sh
chmod +x run_tests.sh
```

## Port Already in Use

If port 8545 is already in use:

```bash
# Find what's using the port
lsof -i :8545

# Kill the process
kill -9 <PID>

# Or use Docker with different port
# Edit docker-compose.yml and change ports to "88545:8545"
```

## Virtual Environment Issues

If you're using a virtual environment:

```bash
# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt
```

## Import Errors When Running Integration Test

If `integration_test.py` has import errors:

```bash
# Make sure you're running from the relayer-py root directory
cd /path/to/relayer-py

# Not from the tests/rpc_simulator directory
python tests/rpc_simulator/integration_test.py
```

## Common Issues

### Issue: "Giant exception dump" / Unhandled timeout exceptions
**Cause**: httpx throws various timeout exceptions  
**Solution**: Test scenarios now catch all timeout exception types:
- `asyncio.TimeoutError`
- `httpx.ReadTimeout`
- `httpx.TimeoutException`
- `httpx.ConnectTimeout`
- `httpx.PoolTimeout`
- Generic `Exception` as fallback

### Issue: "No module named 'utils'"
**Cause**: Running from wrong directory  
**Solution**: Run from relayer-py root directory

### Issue: "Address already in use"
**Cause**: Port 8545 already in use  
**Solution**: Kill existing process or use different port

### Issue: "Permission denied"
**Cause**: Scripts not executable  
**Solution**: `chmod +x script_name.sh`

### Issue: "Docker not running"
**Cause**: Docker daemon not started  
**Solution**: Start Docker Desktop or use Python directly

## Still Having Issues?

1. Check Python version: `python3 --version`
2. Check installed packages: `pip3 list`
3. Verify file permissions: `ls -la`
4. Check Docker status: `docker ps`
5. Review error messages carefully

## Getting Help

Provide the following information:
- Python version
- Operating system
- Full error message
- What command you ran
- Contents of `requirements.txt`

