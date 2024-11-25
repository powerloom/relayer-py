from settings.conf import settings
from eth_account import Account

# Chain ID for the anchor chain, retrieved from settings
CHAIN_ID = settings.anchor_chain.chain_id


async def write_transaction(
    w3, address, private_key, contract, function, nonce, gas_price, priority_gas_multiplier, *args,
):
    """
    Writes a transaction to the blockchain.

    This function constructs, signs, and sends a transaction to the blockchain.

    Args:
        w3 (web3.Web3): Web3 object for interacting with the Ethereum network.
        address (str): The address of the account sending the transaction.
        private_key (str): The private key of the account for signing the transaction.
        contract (web3.eth.contract): Web3 contract object to interact with.
        function (str): The name of the contract function to call.
        nonce (int): The nonce for the transaction.
        gas_price (int): The base gas price for the transaction.
        priority_gas_multiplier (float): Multiplier for the priority fee.
        *args: Variable length argument list for the contract function.

    Returns:
        str: The transaction hash in hexadecimal format.
    """
    from_account = Account.from_key(private_key)
    from_address = from_account.address
    # Get the contract function
    func = getattr(contract.functions, function)

    # Construct the transaction
    transaction = await func(*args).build_transaction({
        'from': from_address,
        'gas': 20000000,  # Fixed gas limit
        'maxFeePerGas': 2 * gas_price,  # Maximum fee per gas
        # Priority fee increases by 10% of the gas price on each retry
        'maxPriorityFeePerGas': int((priority_gas_multiplier + 1) * 0.1 * gas_price),
        'nonce': nonce,
        'chainId': CHAIN_ID,
    })
    
    # Sign the transaction
    signed_transaction = w3.eth.account.sign_transaction(
        transaction, private_key,
    )

    # Send the raw transaction
    tx_hash = await w3.eth.send_raw_transaction(signed_transaction.rawTransaction)

    # Return the transaction hash
    return tx_hash.hex()


async def write_transaction_with_receipt(w3, address, private_key, contract, function, nonce, gas_price, priority_gas_multiplier, *args):
    """
    Writes a transaction, waits for confirmation, and returns the receipt.

    This function calls write_transaction, waits for the transaction to be mined,
    and returns both the transaction hash and the receipt.

    Args:
        w3 (web3.Web3): Web3 object for interacting with the Ethereum network.
        address (str): The address of the account sending the transaction.
        private_key (str): The private key of the account for signing the transaction.
        contract (web3.eth.contract): Web3 contract object to interact with.
        function (str): The name of the contract function to call.
        nonce (int): The nonce for the transaction.
        gas_price (int): The base gas price for the transaction.
        priority_gas_multiplier (float): Multiplier for the priority fee.
        *args: Variable length argument list for the contract function.

    Returns:
        tuple: A tuple containing the transaction hash (str) and the transaction receipt (dict).
    """
    # Write the transaction and get the transaction hash
    tx_hash = await write_transaction(
        w3, address, private_key, contract, function, nonce, gas_price, priority_gas_multiplier, *args,
    )

    # Wait for the transaction to be mined and get the receipt
    receipt = await w3.eth.wait_for_transaction_receipt(tx_hash)

    # Return both the transaction hash and the receipt
    return tx_hash, receipt
