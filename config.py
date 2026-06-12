import os
from dotenv import load_dotenv

load_dotenv()

PRIVATE_KEY = os.environ["POLYMARKET_PRIVATE_KEY"]
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com")

# Optional — bot derives these if not set
CLOB_API_KEY = os.getenv("CLOB_API_KEY")
CLOB_SECRET = os.getenv("CLOB_SECRET")
CLOB_PASS_PHRASE = os.getenv("CLOB_PASS_PHRASE")

# Market making params
SPREAD_BPS = int(os.getenv("SPREAD_BPS", "300"))  # 3% spread each side
ORDER_SIZE_USDC = float(os.getenv("ORDER_SIZE_USDC", "10"))  # $10 per side
MAX_POSITION = float(os.getenv("MAX_POSITION", "100"))  # max $100 exposure per market
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "30"))  # seconds between quote refreshes

# Risk
CAPITAL = float(os.getenv("CAPITAL", "500"))  # total capital in USDC
MAX_LOSS_PCT = float(os.getenv("MAX_LOSS_PCT", "1.0"))  # max loss per position = 1% of capital
MAX_LOSS_USDC = CAPITAL * (MAX_LOSS_PCT / 100)  # e.g. $5 on $500
