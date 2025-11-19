#!/usr/bin/env python3
"""
Test script to verify relayer-py environment variable parsing
"""

import os
import sys
import json
from pathlib import Path

# Add current directory to Python path to import our modules
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables from validator2 devnet
env_file = Path(__file__).parent.parent / "decentralized-sequencer" / "localenvs" / "env.validator2.devnet"

def load_env_file(filepath):
    """Load environment variables from a .env file"""
    env_vars = {}
    if filepath.exists():
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
    return env_vars

def test_env_parsing():
    """Test the environment parsing logic"""
    print("🧪 Testing relayer-py environment parsing...")

    # Load environment variables
    env_vars = load_env_file(env_file)
    print(f"📝 Loaded {len(env_vars)} environment variables from {env_file.name}")

    # Set environment variables for testing
    for key, value in env_vars.items():
        os.environ[key] = value

    # Import and test the settings loading
    try:
        from settings.conf import load_settings_from_env
        print("✅ Successfully imported load_settings_from_env function")
    except ImportError as e:
        print(f"❌ Failed to import: {e}")
        return False

    try:
        settings = load_settings_from_env()
        print("✅ Successfully loaded settings from environment")
    except Exception as e:
        print(f"❌ Failed to load settings: {e}")
        return False

    # Verify key settings
    print("\n📊 Settings Verification:")

    # Test RPC nodes parsing
    expected_rpc = "https://devnet-orbit-rpc.aws2.powerloom.io/rpc?uuid=9dff8954-24f0-11f0-b88f-devnet"
    if len(settings.anchor_chain.rpc.full_nodes) > 0:
        actual_rpc = settings.anchor_chain.rpc.full_nodes[0].url
        if actual_rpc == expected_rpc:
            print(f"✅ RPC URL: {actual_rpc}")
        else:
            print(f"❌ RPC URL mismatch. Expected: {expected_rpc}, Got: {actual_rpc}")
            return False
    else:
        print("❌ No RPC nodes found")
        return False

    # Test protocol state address
    expected_protocol_state = "0xC9e7304f719D35919b0371d8B242ab59E0966d63"
    if settings.protocol_state_address == expected_protocol_state:
        print(f"✅ Protocol State: {settings.protocol_state_address}")
    else:
        print(f"❌ Protocol State mismatch. Expected: {expected_protocol_state}, Got: {settings.protocol_state_address}")
        return False

    # Test signers
    expected_signers = [
        "0x164446B87AbbC0C1A389bD9bAF7F5aE813Cf6aB8",
        "0x0b56FCe7484dC23586bc015d72f9e6AAd3028c80"
    ]

    if len(settings.signers) == len(expected_signers):
        print(f"✅ Found {len(settings.signers)} signers")
        for i, signer in enumerate(settings.signers):
            if signer.address == expected_signers[i]:
                print(f"  ✅ Signer {i+1}: {signer.address}")
            else:
                print(f"  ❌ Signer {i+1} mismatch. Expected: {expected_signers[i]}, Got: {signer.address}")
                return False
    else:
        print(f"❌ Signers count mismatch. Expected: {len(expected_signers)}, Got: {len(settings.signers)}")
        return False

    # Test Redis configuration
    if settings.redis.host == "redis":
        print(f"✅ Redis host: {settings.redis.host}")
    else:
        print(f"❌ Redis host mismatch. Expected: redis, Got: {settings.redis.host}")
        return False

    if settings.redis.port == 6379:
        print(f"✅ Redis port: {settings.redis.port}")
    else:
        print(f"❌ Redis port mismatch. Expected: 6379, Got: {settings.redis.port}")
        return False

    # Test relayer service configuration
    if settings.relayer_service.host == "0.0.0.0" and settings.relayer_service.port == "8080":
        print(f"✅ Relayer service: {settings.relayer_service.host}:{settings.relayer_service.port}")
    else:
        print(f"❌ Relayer service mismatch. Expected: 0.0.0.0:8080, Got: {settings.relayer_service.host}:{settings.relayer_service.port}")
        return False

    print(f"\n✅ All settings verified successfully!")

    # Print the generated settings object (first 500 chars to avoid spam)
    try:
        settings_dict = settings.model_dump()  # Use model_dump instead of deprecated dict()
        settings_json = json.dumps(settings_dict, indent=2, default=str)
        print(f"\n📄 Generated Settings Object (first 500 chars):")
        print(settings_json[:500] + "..." if len(settings_json) > 500 else settings_json)
    except AttributeError:
        # Fallback to dict() for older Pydantic versions
        settings_dict = settings.dict()
        settings_json = json.dumps(settings_dict, indent=2, default=str)
        print(f"\n📄 Generated Settings Object (first 500 chars):")
        print(settings_json[:500] + "..." if len(settings_json) > 500 else settings_json)

    return True

if __name__ == "__main__":
    success = test_env_parsing()
    sys.exit(0 if success else 1)