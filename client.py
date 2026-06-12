from py_clob_client_v2 import ApiCreds, ClobClient
import config


def get_client() -> ClobClient:
    """Initialize authenticated CLOB client."""
    if config.CLOB_API_KEY and config.CLOB_SECRET and config.CLOB_PASS_PHRASE:
        creds = ApiCreds(
            api_key=config.CLOB_API_KEY,
            api_secret=config.CLOB_SECRET,
            api_passphrase=config.CLOB_PASS_PHRASE,
        )
    else:
        # Derive creds from wallet
        tmp = ClobClient(host=config.CLOB_HOST, chain_id=config.CHAIN_ID, key=config.PRIVATE_KEY)
        creds = tmp.create_or_derive_api_key()
        print(f"Derived API creds. Save these to .env:")
        print(f"  CLOB_API_KEY={creds.api_key}")
        print(f"  CLOB_SECRET={creds.api_secret}")
        print(f"  CLOB_PASS_PHRASE={creds.api_passphrase}")

    return ClobClient(
        host=config.CLOB_HOST,
        chain_id=config.CHAIN_ID,
        key=config.PRIVATE_KEY,
        creds=creds,
    )
