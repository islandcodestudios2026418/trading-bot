"""
First-time setup: derives API credentials from your wallet.
Run this once, then copy the output into your .env file.
"""
from py_clob_client_v2 import ClobClient
import config


def main():
    print("Deriving Polymarket CLOB API credentials from wallet...")
    client = ClobClient(host=config.CLOB_HOST, chain_id=config.CHAIN_ID, key=config.PRIVATE_KEY)
    creds = client.create_or_derive_api_key()

    print("\n✅ Success! Add these to your .env file:\n")
    print(f"CLOB_API_KEY={creds.api_key}")
    print(f"CLOB_SECRET={creds.api_secret}")
    print(f"CLOB_PASS_PHRASE={creds.api_passphrase}")


if __name__ == "__main__":
    main()
