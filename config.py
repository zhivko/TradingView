# Configuration constants and settings for the trading application

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Any, TypedDict
from pybit.unified_trading import HTTP
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Kline data type definition
class KlineData(TypedDict):
    time: int
    open: float
    high: float
    low: float
    close: float
    vol: float

# Security
SECRET_KEY = "super-secret"

# Email configuration
MAIL_SERVER = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
MAIL_PORT = int(os.getenv('MAIL_PORT', 587))
MAIL_USE_TLS = os.getenv('MAIL_USE_TLS', 'True').lower() == 'true'
MAIL_USERNAME = os.getenv('MAIL_USERNAME')
MAIL_PASSWORD = os.getenv('MAIL_PASSWORD')
MAIL_DEFAULT_SENDER = os.getenv('MAIL_DEFAULT_SENDER')

# Redis connection settings
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None
REDIS_TIMEOUT = 10
REDIS_RETRY_COUNT = 3
REDIS_RETRY_DELAY = 1

# Trading configuration
TRADING_SYMBOL = "BTCUSDT"
TRADING_TIMEFRAME = "5m"

# Supported symbols and resolutions
SUPPORTED_SYMBOLS = ["BTCUSDT", "XMRUSDT", "ETHUSDT", "SOLUSDT", "SUIUSDT", "PAXGUSDT", "BNBUSDT", "ADAUSDT", "BTCDOM", "APEXUSDT", "DOGEUSDT"]
SUPPORTED_RESOLUTIONS = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]

# Trade aggregator configuration
TRADE_AGGREGATION_RESOLUTION = "1m"

# Redis keys
REDIS_LAST_SELECTED_SYMBOL_KEY = "last_selected_symbol"

# Paths
PROJECT_ROOT = Path(__file__).parent
STATIC_DIR = PROJECT_ROOT / "static"
TEMPLATES_DIR = PROJECT_ROOT / "templates"

# Ensure directories exist
STATIC_DIR.mkdir(exist_ok=True)
TEMPLATES_DIR.mkdir(exist_ok=True)

# Dataclass for timeframe config
@dataclass(frozen=True)
class TimeframeConfig:
    supported_resolutions: tuple[str, ...] = field(default_factory=lambda: tuple(SUPPORTED_RESOLUTIONS))

timeframe_config = TimeframeConfig()

def get_timeframe_seconds(timeframe: str) -> int:
    multipliers = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800}
    return multipliers.get(timeframe, 3600)