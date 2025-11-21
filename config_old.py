# Configuration constants and settings for the trading application

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Any, TypedDict
from pybit.unified_trading import HTTP

# Kline data type definition
class KlineData(TypedDict):
    time: int
    open: float
    high: float
    low: float
    close: float
    vol: float

# Security
SECRET_KEY = "super-secret"  # Replace with a strong, randomly generated key

# Redis connection settings
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None
REDIS_TIMEOUT = 10  # Increased socket timeout
REDIS_RETRY_COUNT = 3
REDIS_RETRY_DELAY = 1

# Trading configuration
TRADING_SYMBOL = "BTCUSDT"  # Symbol to trade for background tasks and AI defaults
TRADING_TIMEFRAME = "5m"  # Timeframe for background tasks and AI defaults

# Supported symbols and resolutions
SUPPORTED_SYMBOLS = ["BTCUSDT", "XMRUSDT", "ETHUSDT", "SOLUSDT", "SUIUSDT", "PAXGUSDT", "BNBUSDT", "ADAUSDT", "BTCDOM", "APEXUSDT", "DOGEUSDT"]
SUPPORTED_RESOLUTIONS = ["1m", "5m", "1h", "4h", "1d", "1w"]

# Trade aggregator configuration
TRADE_AGGREGATION_RESOLUTION = "1m"  # Aggregate trades into 1-minute bars

# Supported exchanges for trade aggregation (CCXT exchange IDs + DEX APIs)
SUPPORTED_EXCHANGES = {
    "binance": {
        "name": "Binance",
        "type": "cex",
        "symbols": {"BTCUSDT": "btcusdt", "ETHUSDT": "ethusdt", "SOLUSDT": "solusdt", "ADAUSDT": "adausdt", "BNBUSDT": "bnbusdt"},
        "rate_limit": 1200,  # requests per minute
        "weight_limit": 60000,  # API weight per minute
        "websocket_ping_interval": 30  # Ping every 30 seconds to keep connection alive
    },
    "bybit": {
        "name": "Bybit",
        "type": "cex",
        "symbols": {"BTCUSDT": "BTCUSDT", "ETHUSDT": "ETHUSDT", "SOLUSDT": "SOLUSDT", "ADAUSDT": "ADAUSDT", "XMRUSDT": "XMRUSDT", "SUIUSDT": "SUIUSDT", "PAXGUSDT": "PAXGUSDT"},
        "rate_limit": 50,  # requests per second
        "weight_limit": 100  # API requests per second limit
    },
    "kucoin": {
        "name": "KuCoin",
        "type": "cex",
        "symbols": {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT", "SOLUSDT": "SOL-USDT", "ADAUSDT": "ADA-USDT"},
        "rate_limit": 30,  # requests per second
        "weight_limit": 30
    },
    "okx": {
        "name": "OKX",
        "type": "cex",
        "symbols": {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT", "SOLUSDT": "SOL-USDT", "ADAUSDT": "ADA/USDT"},
        "rate_limit": 20,  # requests per second
        "weight_limit": 20
    },
    "hyperliquid": {
        "name": "Hyperliquid",
        "type": "dex",
        "symbols": {"BTCUSDT": "BTC", "ETHUSDT": "ETH", "SOLUSDT": "SOL", "ADAUSDT": "ADA"},
        "rate_limit": 10,  # requests per second (conservative)
        "weight_limit": 10,
        "api_base_url": "https://api.hyperliquid.xyz"
    },
    "aster": {
        "name": "Aster",
        "type": "dex",
        "symbols": {"BTCUSDT": "BTC", "ETHUSDT": "ETH", "SOLUSDT": "SOL"},
        "rate_limit": 5,  # requests per second (conservative)
        "weight_limit": 5,
        "api_base_url": "https://api.aster.network"
    },
    "apex": {
        "name": "Apex",
        "type": "dex",
        "symbols": {"BTCUSDT": "BTC", "ETHUSDT": "ETH", "SOLUSDT": "SOL", "PAXGUSDT": "PAXGUSDT"},
        "rate_limit": 5,  # requests per second (conservative)
        "weight_limit": 5,
        "api_base_url": "https://api.apex.exchange"
    },
    "dxdy": {
        "name": "dYdX",
        "type": "dex",
        "symbols": {"BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD", "SOLUSDT": "SOL-USD", "ADAUSDT": "ADA-USD"},
        "rate_limit": 10,  # requests per second
        "weight_limit": 10,
        "api_base_url": "https://api.dydx.exchange"
    }
}

SUPPORTED_RANGES = [
    {"value": "1h", "label": "1h"},
    {"value": "8h", "label": "8h"},
    {"value": "24h", "label": "24h"},
    {"value": "3d", "label": "3d"},
    {"value": "7d", "label": "7d"},
    {"value": "30d", "label": "30d"},
    {"value": "3m", "label": "3M"},  # Approximately 3 * 30 days
    {"value": "6m", "label": "6M"},  # Approximately 6 * 30 days
    {"value": "1y", "label": "1Y"},  # Approximately 365 days
    {"value": "3y", "label": "3Y"},  # Approximately 3 * 365 days
]

# Bybit resolution mapping
BYBIT_RESOLUTION_MAP = {
    "1m": "1", "5m": "5", "1h": "60", "4h": "240", "1d": "D", "1w": "W"
}

# Redis keys
REDIS_LAST_SELECTED_SYMBOL_KEY = "last_selected_symbol"
REDIS_OPEN_INTEREST_KEY_PREFIX = f"zset:open_interest:{TRADING_SYMBOL}:{TRADING_TIMEFRAME}"

# Default symbol settings
DEFAULT_SYMBOL_SETTINGS = {
    'resolution': '1d',
    'range': '30d',
    'xAxisMin': None,
    'xAxisMax': None,
    'yAxisMin': None,
    'yAxisMax': None,
    'active_indicators': [],
    'liveDataEnabled': True,
    'streamDeltaTime': 1,  # New default for live stream update interval (seconds)
    'useLocalOllama': False,
    'localOllamaModelName': None,  # New default
    'showAgentTrades': False  # New default for showing agent trades
}

# Trading Service Configuration
TRADING_SERVICE_URL = os.getenv("TRADING_SERVICE_URL", "http://localhost:8000")

# AI Configuration
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_API_MODEL_NAME = "deepseek-reasoner"

LOCAL_OLLAMA_BASE_URL = "http://localhost:11434/v1"
LOCAL_OLLAMA_MODEL_NAME = "llama3"

LM_STUDIO_BASE_URL = "http://localhost:1234/v1"
LM_STUDIO_MODEL_NAME = "local-model"  # LM Studio uses "local-model" as default

MAX_DATA_POINTS_FOR_LLM = 100

# YouTube Configuration
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
YOUTUBE_CHANNELS = os.getenv("YOUTUBE_CHANNELS", "@MooninPapa")

# Paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
STATIC_DIR = PROJECT_ROOT / "static"
TEMPLATES_DIR = PROJECT_ROOT / "templates"
AUTH_CREDS_FILE = Path("c:/git/VidWebServer/authcreds.json")

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)
TEMPLATES_DIR.mkdir(exist_ok=True)

# Create symbol directories
for symbol in SUPPORTED_SYMBOLS:
    (DATA_DIR / symbol).mkdir(exist_ok=True)

# Available indicators configuration
AVAILABLE_INDICATORS = [
    {"id": "macd", "name": "MACD", "params": {"short_period": 12, "long_period": 26, "signal_period": 9}},
    {"id": "rsi", "name": "RSI", "params": {"period": 14}},
    {"id": "stochrsi_9_3", "name": "Stochastic RSI (9,3)", "params": {"rsi_period": 9, "stoch_period": 9, "k_period": 3, "d_period": 3}},
    {"id": "stochrsi_14_3", "name": "Stochastic RSI (14,3)", "params": {"rsi_period": 14, "stoch_period": 14, "k_period": 3, "d_period": 3}},
    {"id": "stochrsi_40_4", "name": "Stochastic RSI (40,4)", "params": {"rsi_period": 40, "stoch_period": 40, "k_period": 4, "d_period": 4}},
    {"id": "stochrsi_60_10", "name": "Stochastic RSI (60,10)", "params": {"rsi_period": 60, "stoch_period": 60, "k_period": 10, "d_period": 10}},
    {"id": "open_interest", "name": "Open Interest", "params": {}},
    {"id": "jma", "name": "Jurik MA", "params": {"length": 7, "phase": 50, "power": 2}},
    {"id": "cto_line", "name": "CTO Line (Larsson)", "params": {"v1_period": 15, "m1_period": 19, "m2_period": 25, "v2_period": 29}},
]

@dataclass(frozen=True)
class TimeframeConfig:
    supported_resolutions: tuple[str, ...] = field(default_factory=lambda: tuple(SUPPORTED_RESOLUTIONS))
    resolution_map: dict[str, str] = field(default_factory=lambda: BYBIT_RESOLUTION_MAP)

# Global instances
timeframe_config = TimeframeConfig()

# Utility functions
def get_timeframe_seconds(timeframe: str) -> int:
    """Convert timeframe string to seconds."""
    multipliers = {"1m": 60, "5m": 300, "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800}
    return multipliers.get(timeframe, 3600)

# Bybit API session - lazy initialization
def get_session():
    """Get or create Bybit API session."""
    from auth import creds
    return HTTP(
        api_key=creds.api_key,
        api_secret=creds.api_secret,
        testnet=False
    )

# For backward compatibility - session is now created lazily
# Use get_session() when needed
session = get_session()
