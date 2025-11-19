# AppTradingView2 - Single WS Endpoint Application
# Refactored FastAPI application using a single WebSocket endpoint for all client server communication
import os
import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus
import hashlib
import base64
from cryptography.fernet import Fernet
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Environment variables loaded from .env file")
except ImportError:
    print("⚠️ python-dotenv not installed. Environment variables must be set manually.")
    print("Install with: pip install python-dotenv")

import socket
import uvicorn
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, UploadFile, File, Form
from starlette.websockets import WebSocketState
import tempfile
import io

# Import configuration and utilities
from config import SECRET_KEY, STATIC_DIR, TEMPLATES_DIR, PROJECT_ROOT, SUPPORTED_RANGES, SUPPORTED_RESOLUTIONS, SUPPORTED_SYMBOLS, REDIS_LAST_SELECTED_SYMBOL_KEY
from logging_config import logger
from auth import creds, get_session
from background_tasks import fetch_and_publish_klines, fetch_and_aggregate_trades, fill_trade_data_gaps_background_task
from bybit_price_feed import start_bybit_price_feed

# Encryption utilities using kljuc from authcreds.json as seed
def get_encryption_key():
    """Generate encryption key from kljuc in authcreds.json"""
    kljuc = creds.api_key
    # Use SHA256 to derive a 32-byte key from kljuc
    key = hashlib.sha256(kljuc.encode()).digest()
    # Fernet requires base64-encoded 32-byte key
    return base64.urlsafe_b64encode(key)


# Authentication utilities
def authenticate_user_from_request(request: Request) -> tuple[bool, str]:
    """
    Centralized authentication function that checks for email cookie first,
    then falls back to other authentication methods.

    Returns: (authenticated: bool, email: str or None)
    """
    # Check for email cookie first (new email confirmation authentication)
    user_email = request.cookies.get('user_email')
    if user_email:
        logger.info(f"Email cookie authentication successful for {user_email}")
        return True, user_email

    # Check for SSL client certificate authentication
    ssl_client_verify = request.headers.get('X-SSL-Client-Verify')
    ssl_client_s_dn = request.headers.get('X-SSL-Client-S-DN')

    if ssl_client_verify == 'SUCCESS':
        # Extract email from certificate subject DN if available
        if ssl_client_s_dn:
            import re
            email_match = re.search(r'emailAddress=([^,]+)', ssl_client_s_dn)
            if email_match:
                cert_email = email_match.group(1)
                logger.info(f"Client certificate authentication successful for {cert_email}")
                return True, cert_email
        # Fallback: use a generic authenticated user
        logger.info("Client certificate authentication successful (no email in DN)")
        return True, "cert_authenticated@example.com"

    # Check for secret query parameter (fallback authentication)
    secret = request.query_params.get('secret')
    if secret:
        # Try to decrypt the secret first, fallback to direct comparison
        decrypted_secret = decrypt_secret(secret)
        current_date = datetime.now().strftime('%d.%m.%Y')
        if decrypted_secret == current_date or secret == current_date:
            logger.info(f"Secret authentication successful for date {current_date}")
            return True, "test@example.com"

    # Check for existing session authentication
    if request.session.get('email'):
        logger.info(f"Session authentication successful for {request.session.get('email')}")
        return True, request.session.get('email')

    logger.warning("No valid authentication method available")
    return False, None

def encrypt_secret(secret):
    """Encrypt a secret using kljuc as seed"""
    if not secret:
        return secret
    key = get_encryption_key()
    f = Fernet(key)
    return f.encrypt(secret.encode()).decode()

def decrypt_secret(encrypted_secret):
    """Decrypt a secret using kljuc as seed"""
    if not encrypted_secret:
        return encrypted_secret
    try:
        key = get_encryption_key()
        f = Fernet(key)
        return f.decrypt(encrypted_secret.encode()).decode()
    except Exception as e:
        logger.error(f"Failed to decrypt secret: {e}")
        return encrypted_secret  # Return as-is if decryption fails


# Email confirmation utilities
def generate_confirmation_token():
    """Generate a unique confirmation token"""
    return str(uuid.uuid4())


def get_smtp_config():
    """Get SMTP configuration from credentials file (same as email_alert_service.py)"""
    try:
        from auth import BybitCredentials
        from pathlib import Path
        creds = BybitCredentials.from_file(Path("c:/git/VidWebServer/authcreds.json"))
        return {
            'server': creds.SMTP_SERVER,
            'port': creds.SMTP_PORT,
            'username': creds.gmailEmail,
            'password': creds.gmailPwd,
            'from_email': creds.gmailEmail
        }
    except Exception as e:
        logger.error(f"Failed to load SMTP config: {e}")
        return None


async def send_confirmation_email(email: str, confirmation_token: str):
    """Send confirmation email with link"""
    try:
        # Get SMTP config (same as email_alert_service.py)
        smtp_config = get_smtp_config()
        if not smtp_config:
            logger.error("SMTP configuration not available")
            return False

        # Create message
        msg = MIMEMultipart()
        msg['From'] = smtp_config['from_email']
        msg['To'] = email
        msg['Subject'] = "Confirm your email address"

        # Confirmation link
        confirmation_url = f"https://crypto.zhivko.eu/confirm-email?token={confirmation_token}"

        html_body = f"""
        <html>
        <body>
            <h2>Welcome to Trading View!</h2>
            <p>Please click the link below to confirm your email address:</p>
            <p><a href="{confirmation_url}">Confirm Email</a></p>
            <p>If you didn't request this, please ignore this email.</p>
            <p>This link will expire in 24 hours.</p>
        </body>
        </html>
        """

        msg.attach(MIMEText(html_body, 'html'))

        # Send email (same logic as email_alert_service.py)
        if smtp_config['port'] == 465:
            server = smtplib.SMTP_SSL(smtp_config['server'], smtp_config['port'])
            server.login(smtp_config['username'], smtp_config['password'])
            server.send_message(msg)
            server.quit()
        else:
            server = smtplib.SMTP(smtp_config['server'], smtp_config['port'])
            server.starttls()
            server.login(smtp_config['username'], smtp_config['password'])
            server.send_message(msg)
            server.quit()

        logger.info(f"Confirmation email sent to {email}")
        return True

    except Exception as e:
        logger.error(f"Failed to send confirmation email to {email}: {e}")
        return False

# Import background tasks
from background_tasks import monitor_email_alerts

from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware import Middleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import RedirectResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware
from slowapi.errors import RateLimitExceeded
import whisper
import httpx

# Google OAuth imports - DISABLED
# from google_auth_oauthlib.flow import Flow
# from google.oauth2 import id_token
# from google.auth.transport import requests as google_requests

# Import legacy endpoint handlers for reuse
from endpoints.chart_endpoints import history_endpoint, initial_chart_config
from endpoints.drawing_endpoints import (
    get_drawings_api_endpoint, save_drawing_api_endpoint,
    delete_drawing_api_endpoint, update_drawing_api_endpoint,
    delete_all_drawings_api_endpoint, save_shape_properties_api_endpoint,
    get_shape_properties_api_endpoint
)
from endpoints.ai_endpoints import (
    ai_suggestion_endpoint, get_local_ollama_models_endpoint,
    get_available_indicators_endpoint
)
from endpoints.trading_endpoints import (
    get_agent_trades_endpoint, get_order_history_endpoint,
    get_buy_signals_endpoint
)
from endpoints.utility_endpoints import (
    settings_endpoint, set_last_selected_symbol,
    get_last_selected_symbol, get_live_price
)
from endpoints.indicator_endpoints import indicator_history_endpoint

# Import YouTube endpoints - commented out as file doesn't exist
# from endpoints.youtube_endpoints import router as youtube_router

# Import WebSocket helpers
from websocket_handlers import (
    calculate_volume_profile, calculate_trading_sessions,
    fetch_recent_trade_history, stream_klines,
    fetch_MY_recent_trade_history, calculate_indicators_for_data
)
from redis_utils import get_cached_klines, init_redis, get_redis_connection
from drawing_manager import save_drawing, delete_drawing, update_drawing, get_drawings, DrawingData, update_drawing_properties


# Import AI features for LLM processing
from ai_features import process_audio_with_llm

# Get local IP address as global variable
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    IP_ADDRESS = s.getsockname()[0]
    s.close()
    logger.info(f'Local IP address determined: {IP_ADDRESS}')
except Exception as e:
    logger.warning(f"Could not determine local IP address: {e}")
    IP_ADDRESS = "127.0.0.1"

# Lifespan manager for startup and shutdown events
@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    # Clean up old log file BEFORE any logging
    try:
        log_file_path = os.path.join(PROJECT_ROOT, "logs", "trading_view.log")
        if os.path.exists(log_file_path):
            os.remove(log_file_path)
            print("🗑️ Deleted old trading_view.log file")  # Use print instead of logger
    except Exception as e:
        print(f"Could not delete old log file: {e}")  # Use print instead of logger

    logger.info("AppTradingView2 startup...")

    try:
        await init_redis()

        # Store the task in the application state
        logger.info("🔧 STARTING BACKGROUND TASK: Creating fetch_and_publish_klines task...")
        app_instance.state.fetch_klines_task = asyncio.create_task(fetch_and_publish_klines(app_instance))

        # Start trade aggregator background task
        logger.info("🔧 STARTING BACKGROUND TASK: Creating fetch_and_aggregate_trades task...")
        app_instance.state.trade_aggregator_task = asyncio.create_task(fetch_and_aggregate_trades(app_instance))

        # Start trade data gap filler background task
        logger.info("🔧 STARTING BACKGROUND TASK: Creating fill_trade_data_gaps_background_task...")
        app_instance.state.trade_gap_filler_task = asyncio.create_task(fill_trade_data_gaps_background_task(app_instance))

        # Start Bybit price feed task (only if not disabled)
        if os.getenv("DISABLE_BYBIT_PRICE_FEED", "false").lower() != "true":
            logger.info("🔧 STARTING BACKGROUND TASK: Creating Bybit price feed task...")
            app_instance.state.price_feed_task = await start_bybit_price_feed()
        else:
            logger.info("🚫 Bybit price feed disabled via DISABLE_BYBIT_PRICE_FEED environment variable")
            app_instance.state.price_feed_task = None

        # Start WebSocket trade streams
        logger.info("🔧 STARTING WEBSOCKET TRADE STREAMS...")
        from websocket_trade_streams import start_websocket_trade_streams
        await start_websocket_trade_streams(app_instance)

        # Start email alert monitoring service
        logger.info("🔧 STARTING EMAIL ALERT MONITORING TASK...")
        app_instance.state.email_alert_task = asyncio.create_task(monitor_email_alerts())
        logger.info("✅ EMAIL ALERT MONITORING TASK started")

        # Preload Whisper model for audio transcription
        try:
            logger.info("🔧 PRELOADING WHISPER MODEL: Loading Whisper base model for audio transcription...")
            import torch
            # Force CPU usage and disable CUDA
            original_cuda_check = torch.cuda.is_available
            torch.cuda.is_available = lambda: False
            # Clear any cached models
            if hasattr(whisper, '_models'):
                whisper._models.clear()
            # Load model
            logger.info("Loading Whisper base model...")
            app_instance.state.whisper_model = whisper.load_model("base", device="cpu")
            logger.info("✅ WHISPER MODEL LOADED: Whisper base model successfully loaded and cached")
            # Restore original CUDA check
            torch.cuda.is_available = original_cuda_check
        except Exception as e:
            logger.error(f"❌ FAILED TO LOAD WHISPER MODEL: {e}", exc_info=True)
            app_instance.state.whisper_model = None

    except Exception as e:
        logger.error(f"Failed to initialize components: {e}", exc_info=True)
        app_instance.state.fetch_klines_task = None
        app_instance.state.trade_aggregator_task = None
        app_instance.state.trade_gap_filler_task = None
        app_instance.state.price_feed_task = None
        app_instance.state.email_alert_task = None
        app_instance.state.whisper_model = None
    yield
    logger.info("AppTradingView2 shutdown...")

    # Cleanup tasks (same as original)
    for task_name in ['trade_aggregator_task', 'trade_gap_filler_task', 'fetch_klines_task']:
        task = getattr(app_instance.state, task_name, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.info(f"✅ TASK CANCELLED: {task_name}")

    price_feed_task = getattr(app_instance.state, 'price_feed_task', None)
    if price_feed_task:
        price_feed_task.cancel()
        try:
            await price_feed_task
        except asyncio.CancelledError:
            logger.info("Bybit price feed task successfully cancelled.")

    email_alert_task = getattr(app_instance.state, 'email_alert_task', None)
    if email_alert_task:
        email_alert_task.cancel()
        try:
            await email_alert_task
        except asyncio.CancelledError:
            logger.info("Email alert monitoring task successfully cancelled.")


# Create FastAPI app
app = FastAPI(lifespan=lifespan, title="AppTradingView2", description="Single WebSocket endpoint trading application")
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# Add rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://crypto.zhivko.eu", "http://{IP_ADDRESS}:5000", "http://localhost:5000", "http://127.0.0.1:5000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files and templates
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Cache control middleware
@app.middleware("http")
async def add_no_cache_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    if os.getenv("FASTAPI_DEBUG", "False").lower() == "true" or app.extra.get("debug_mode", False):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "-1"
    return response

# Single unified WebSocket endpoint for all client-server communication
@app.websocket("/ws")
async def unified_websocket_endpoint(websocket: WebSocket):
    """Single WebSocket endpoint handling all client-server communication"""
    await websocket.accept()
    logger.info(f"Unified WebSocket connection established for client")

    client_id = str(uuid.uuid4())

    # Register this WebSocket connection in app state for trade updates
    if not hasattr(app.state, 'active_websockets'):
        app.state.active_websockets = {}
    # Get email from session if available
    session = websocket.scope.get('session', {})
    email = session.get('email')

    redis_conn = await get_redis_connection()
    last_symbol_key = f"user:{email}:global_settings:last_selected_symbol"
    symbol = await redis_conn.get(last_symbol_key)
    resolution_key = f"settings:{email}:{symbol}"
    resolution_json = await redis_conn.get(resolution_key)

    if resolution_json is None:
        # Set defaults with estimated values
        current_ts_ms = int(time.time() * 1000)
        range_str = "24h"  # Default range

        # Parse range to seconds
        if range_str.endswith('h'):
            range_hours = int(range_str[:-1])
            range_seconds = range_hours * 3600
        elif range_str.endswith('d'):
            range_days = int(range_str[:-1])
            range_seconds = range_days * 24 * 3600
        else:
            range_seconds = 24 * 3600  # Default to 24 hours

        from_ts_ms = current_ts_ms - (range_seconds * 1000)

        # Get yAxisMin and yAxisMax from klines
        try:
            klines = await get_cached_klines(symbol, "1h", int(from_ts_ms / 1000), int(current_ts_ms / 1000))
            if klines:
                prices = [k.get('close', 0) for k in klines if k.get('close')]
                yAxisMin = min(prices) if prices else None
                yAxisMax = max(prices) if prices else None
            else:
                yAxisMin = None
                yAxisMax = None
        except Exception as e:
            logger.warning(f"Failed to fetch klines for defaults: {e}")
            yAxisMin = None
            yAxisMax = None

        resolution_json = json.dumps({
            "resolution": "1h",
            "range": range_str,
            "xAxisMin": from_ts_ms,
            "xAxisMax": current_ts_ms,
            "yAxisMin": yAxisMin,
            "yAxisMax": yAxisMax,
            "replayFrom": "",
            "replayTo": "",
            "replaySpeed": "1",
            "useLocalOllama": False,
            "localOllamaModelName": "",
            "active_indicators": ["macd","rsi"],
            "liveDataEnabled": True,
            "showAgentTrades": False,
            "streamDeltaTime": 1,
            "last_selected_symbol": symbol,
            "minValueFilter": 0.5,
            "enableTradeTrace": True
        })

    resolution = json.loads(resolution_json).get('resolution')

    app.state.active_websockets[client_id] = {
        'websocket': websocket,
        'current_symbol': symbol,
        'current_resolution': resolution,
        'email': email,
        'from_ts': None,  # Will be set when client requests history
        'to_ts': None,    # Will be set when client requests history
        'last_sent_timestamp': None  # Track last OHLC timestamp sent to client
    }
    logger.debug(f"Registered WebSocket client {client_id} for trade updates")

    # Client state for live streaming
    client_state = {
        "current_symbol": None,
        "live_stream_task": None,
        "last_sent_live_price": None,
        "last_sent_timestamp": 0.0,
        "stream_delta_seconds": 1  # Default, will be updated from settings
    }

    async def send_live_update(live_price: float, symbol: str):
        """Send live price update to client if price changed"""
        try:
            current_time = time.time()

            # Check if we should send based on throttling
            if client_state["stream_delta_seconds"] == 0 or (current_time - client_state["last_sent_timestamp"]) >= client_state["stream_delta_seconds"]:

                # Only send if price actually changed
                if live_price != client_state.get("last_sent_live_price"):
                    live_data = {
                        "type": "live",
                        "symbol": symbol,
                        "data": {
                            "live_price": live_price,
                            "time": int(current_time)
                        }
                    }
                    await websocket.send_json(live_data)
                    client_state["last_sent_timestamp"] = current_time
                    client_state["last_sent_live_price"] = live_price
                    # logger.debug(f"📤 Sent live price update for {symbol}: {live_price}")

        except Exception as e:
            if websocket.client_state == WebSocketState.CONNECTED:
                logger.error(f"Error sending live price update for {symbol}: {e}")

    async def start_live_streaming(symbol: str, resolution: str):
        """Start live price streaming for the given symbol"""
        # Cancel existing task if symbol changed
        if client_state["live_stream_task"] is not None:
            if not client_state["live_stream_task"].done():
                client_state["live_stream_task"].cancel()
                try:
                    await client_state["live_stream_task"]
                except asyncio.CancelledError:
                    pass

        client_state["current_symbol"] = symbol
        # Update app state with current symbol and resolution
        if hasattr(app.state, 'active_websockets') and client_id in app.state.active_websockets:
            app.state.active_websockets[client_id]['current_symbol'] = symbol
            app.state.active_websockets[client_id]["email"] = session.get('email')
            # Copy time range from app state if available
            if 'from_ts' in app.state.active_websockets[client_id]:
                client_state["from_ts"] = app.state.active_websockets[client_id]['from_ts']
            if 'to_ts' in app.state.active_websockets[client_id]:
                client_state["to_ts"] = app.state.active_websockets[client_id]['to_ts']

        async def live_stream_worker():
            """Continuously streams live price updates from Redis"""
            logger.info(f"Started live streaming task for {symbol} on client {client_id}")

            # Send initial live price if available
            try:
                redis_conn = await get_redis_connection()
                live_price_key = f"live:{symbol}"
                price_str = await redis_conn.get(live_price_key)
                if price_str:
                    live_price = float(price_str)
                    await send_live_update(live_price, symbol)
                    logger.info(f"Sent initial live price for {symbol}: {live_price}")
            except Exception as e:
                logger.warning(f"Failed to send initial live price for {symbol}: {e}")

            while websocket.client_state == WebSocketState.CONNECTED and client_state["current_symbol"] == symbol:
                try:
                    # Get updated stream delta from settings periodically
                    redis_conn = await get_redis_connection()
                    session = websocket.scope.get('session', {})
                    email = session.get('email')
                    if email:
                        settings_key = f"settings:{email}:{symbol}"
                        settings_json = await redis_conn.get(settings_key)
                        if settings_json:
                            symbol_settings = json.loads(settings_json)
                            new_delta = int(symbol_settings.get('streamDeltaTime', 1))
                            client_state["stream_delta_seconds"] = new_delta

                    # Get live price from Redis
                    live_price_key = f"live:{symbol}"
                    price_str = await redis_conn.get(live_price_key)
                    if price_str:
                        live_price = float(price_str)
                        await send_live_update(live_price, symbol)

                    # Wait before checking again
                    await asyncio.sleep(2)

                except Exception as e:

                    # Get live price from Redis
                    live_price_key = f"live:{symbol}"
                    price_str = await redis_conn.get(live_price_key)
                    if price_str:
                        live_price = float(price_str)
                        await send_live_update(live_price, symbol)

                    # Wait before checking again
                    await asyncio.sleep(0.5)

                except Exception as e:
                    logger.error(f"Error in live streaming worker for {symbol}: {e}")
                    break

            logger.info(f"Live streaming task ended for {symbol} on client {client_id}")

        client_state["live_stream_task"] = asyncio.create_task(live_stream_worker())

    try:
        while True:
            try:
                # Receive message from client
                raw_message = await websocket.receive_json()
                logger.debug(f"Message received from client: {raw_message}")
                logger.debug(f"Received WS message: {raw_message.get('type')}")

                # Process the message
                response = await handle_websocket_message(raw_message, websocket, client_id)

                # Check if this is a config message that might change the symbol
                message_type = raw_message.get('type')
                if message_type == "config" and raw_message.get('data'):
                    config_symbol = raw_message['data'].get("symbol")
                    resolution = raw_message['data'].get("resolution")
                    # Store time range from config message
                    from_ts = raw_message['data'].get("xAxisMin")
                    to_ts = raw_message['data'].get("xAxisMax")
                    if from_ts is not None and to_ts is not None:
                        # Convert from milliseconds to seconds if needed
                        if from_ts > 1e12:
                            from_ts = int(from_ts / 1000)
                        if to_ts > 1e12:
                            to_ts = int(to_ts / 1000)
                        # Update client state with time range
                        if hasattr(app.state, 'active_websockets') and client_id in app.state.active_websockets:
                            app.state.active_websockets[client_id]['from_ts'] = from_ts
                            app.state.active_websockets[client_id]['to_ts'] = to_ts
                            # Reset last sent timestamp when time range changes
                            app.state.active_websockets[client_id]['last_sent_timestamp'] = None
                    if config_symbol and config_symbol != client_state.get("current_symbol"):
                        logger.info(f"Client {client_id} switching to symbol {config_symbol} - starting live streaming")
                        await start_live_streaming(config_symbol, resolution)
                        # Reset last sent timestamp on symbol change
                        if hasattr(app.state, 'active_websockets') and client_id in app.state.active_websockets:
                            app.state.active_websockets[client_id]['last_sent_timestamp'] = None
                elif message_type == "init" and raw_message.get('data'):
                    # Also handle init messages that might specify a symbol
                    init_symbol = raw_message['data'].get('symbol')
                    resolution = raw_message['data'].get('resolution')
                    if init_symbol and init_symbol != client_state.get("current_symbol"):
                        logger.info(f"Client {client_id} initializing with symbol {init_symbol} - starting live streaming")
                        await start_live_streaming(init_symbol, resolution)
                        # Reset last sent timestamp on symbol change
                        if hasattr(app.state, 'active_websockets') and client_id in app.state.active_websockets:
                            app.state.active_websockets[client_id]['last_sent_timestamp'] = None

                # Send response back to client
                if response:
                    await websocket.send_json(response)

            except WebSocketDisconnect:
                logger.info(f"WebSocket client {client_id} disconnected")
                break
            except json.JSONDecodeError:
                logger.warn(f"Invalid JSON received from client {client_id}")
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON format",
                    "request_id": None
                })
            except Exception as e:
                logger.error(f"Error processing WebSocket message for client {client_id}: {e}", exc_info=True)
                try:
                    await websocket.send_json({
                        "type": "error",
                        "message": str(e),
                        "request_id": raw_message.get('request_id') if 'raw_message' in locals() else None
                    })
                except Exception:
                    break  # Connection probably closed

    except Exception as e:
        logger.error(f"Fatal error in unified WebSocket endpoint: {e}")
    finally:
        # Clean up live streaming task
        if client_state.get("live_stream_task") is not None:
            client_state["live_stream_task"].cancel()
            try:
                await client_state["live_stream_task"]
            except asyncio.CancelledError:
                pass

        logger.info(f"Cleaned up WebSocket connection for client {client_id}")

        # Unregister this WebSocket connection from app state
        if hasattr(app.state, 'active_websockets') and client_id in app.state.active_websockets:
            del app.state.active_websockets[client_id]
            logger.debug(f"Unregistered WebSocket client {client_id} from trade updates")


async def handle_websocket_message(message: dict, websocket: WebSocket, client_id: str = None) -> dict:
    """
    Handle incoming WebSocket messages and route them to appropriate handlers.
    Returns a response message dict that will be sent back to the client.
    """
    message_type = message.get('type')
    action = message.get('action')
    request_id = message.get('request_id')

    logger.info(f"WS message:\n" + str(message))

    try:
        # Handle ping messages
        if message_type == "ping":
            return {
                "type": "pong",
                "request_id": request_id,
                "timestamp": int(datetime.now().timestamp() * 1000)
            }

        # Handle request messages
        # if message_type == "request":
        #    return await handle_request_action(action, message.get('data', {}), websocket, request_id)

        # Handle initialization messages
        if message_type == "init":
            return await handle_init_message(message.get('data', {}), websocket, request_id)

        elif message_type == "history":
            return await handle_history_message(message.get('data', {}), websocket, request_id, client_id)

        elif message_type == "trade_history":
            return await handle_trade_history_message(message.get('data', {}), websocket, request_id)

        elif message_type in ["config"]:
            logger.info(f"WS CONFIG MESSAGE RECEIVED: data={message.get('data', {})}")
            return await handle_config_message(message.get('data', {}), websocket, request_id)

        elif message_type == "shape":
            return await handle_shape_message(message.get('data', {}), websocket, request_id, message.get('action'))

        elif message_type == "get_volume_profile":
            return await handle_get_volume_profile_direct(message, websocket, request_id)

        elif message_type == "get_trading_sessions":
            return await handle_get_trading_sessions(message.get('data', {}), websocket, request_id)

        # Handle unknown message types
        logger.warn(f"Unknown WS message type: {message_type}")
        return {
            "type": "error",
            "message": f"Unknown message type: {message_type}",
            "request_id": request_id
        }

    except Exception as e:
        logger.error(f"Error handling WS message: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_config_message(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle config message - save user configuration settings to Redis"""
    try:
        # Get session and email
        session = websocket.scope.get('session', {})
        email = data.get('email') or session.get('email')

        if not email:
            return {
                "type": "error",
                "message": "Authentication required - no email found",
                "request_id": request_id
            }

        # Get symbol from config data
        symbol = data.get('symbol', 'BTCUSDT').upper()
        logger.info(f"💾 Saving config for {email}:{symbol}")
        logger.info(f"📊 CONFIG: Received data keys: {list(data.keys())}")
        logger.info(f"📊 CONFIG: xAxisMin value: {data.get('xAxisMin')}, xAxisMax value: {data.get('xAxisMax')}")

        # Prepare settings data from config message
        # Check for None values (fields not sent by client) and log them
        missing_fields = []
        settings_data = {}

        # Define all expected fields with their defaults
        expected_fields = {
            'resolution': ('1h', str),
            'range': ('24h', str),
            'xAxisMin': (None, (int, float, type(None))),
            'xAxisMax': (None, (int, float, type(None))),
            'yAxisMin': (None, (int, float, type(None))),
            'yAxisMax': (None, (int, float, type(None))),
            'replayFrom': ('', str),
            'replayTo': ('', str),
            'replaySpeed': ('1', str),
            'useLocalOllama': (False, bool),
            'localOllamaModelName': ('', str),
            'active_indicators': ([], list),
            'liveDataEnabled': (True, bool),
            'showAgentTrades': (False, bool),
            'streamDeltaTime': (1, (int, float)),
            'minValueFilter': (0, (int, float)),
            'enableTradeTrace': (True, bool)
        }

        for field_name, (default_value, expected_type) in expected_fields.items():
            field_value = data.get(field_name)
            if field_value is None:
                # Field not sent by client - log it and use default
                missing_fields.append(field_name)
                settings_data[field_name] = default_value
            else:
                # Field sent by client - use it
                settings_data[field_name] = field_value

        # Log missing fields
        if missing_fields:
            logger.warning(f"⚠️ CONFIG: Client sent partial config message - missing fields: {missing_fields}. Using defaults for {email}:{symbol}")
            if 'active_indicators' in missing_fields:
                logger.warning(f"⚠️ CONFIG: 'active_indicators' not sent by client - defaulting to empty list [] for {email}:{symbol}")
            if 'xAxisMin' in missing_fields or 'xAxisMax' in missing_fields:
                logger.warning(f"⚠️ CONFIG: xAxisMin/xAxisMax missing - xAxisMin: {data.get('xAxisMin')}, xAxisMax: {data.get('xAxisMax')} for {email}:{symbol}")

        x_axis_min = settings_data.get('xAxisMin')
        x_axis_max = settings_data.get('xAxisMax')
        x_axis_min_iso = datetime.fromtimestamp(x_axis_min / 1000 if x_axis_min and x_axis_min > 1e12 else x_axis_min, timezone.utc).isoformat() if x_axis_min else None
        x_axis_max_iso = datetime.fromtimestamp(x_axis_max / 1000 if x_axis_max and x_axis_max > 1e12 else x_axis_max, timezone.utc).isoformat() if x_axis_max else None
        logger.info(f"💾 CONFIG: Settings data to save: xAxisMin={x_axis_min} ({x_axis_min_iso}), xAxisMax={x_axis_max} ({x_axis_max_iso})")

        # Save settings to Redis
        redis_conn = await get_redis_connection()
        settings_key = f"settings:{email}:{symbol}"
        settings_json = json.dumps(settings_data)
        await redis_conn.set(settings_key, settings_json)
        logger.info(f"✅ CONFIG: Saved settings to Redis key {settings_key} with xAxisMin={settings_data.get('xAxisMin')}, xAxisMax={settings_data.get('xAxisMax')}")

        # Also update last selected symbol if provided
        last_symbol_key = f"user:{email}:{REDIS_LAST_SELECTED_SYMBOL_KEY}"
        await redis_conn.set(last_symbol_key, data['symbol'].upper())

        # Return the same response format as handle_config_message_old with actual data
        # Get timestamp values for data fetching
        from_ts_val = data.get("xAxisMin")
        to_ts_val = data.get("xAxisMax")
        
        # Convert timestamps from milliseconds to seconds if needed
        config_from_ts = None
        if isinstance(from_ts_val, (int, float)):
            if from_ts_val > 1e12:  # > 1 trillion, likely milliseconds
                config_from_ts = int(from_ts_val / 1000)
            else:
                config_from_ts = int(from_ts_val)
                
        config_to_ts = None
        if isinstance(to_ts_val, (int, float)):
            if to_ts_val > 1e12:  # > 1 trillion, likely milliseconds
                config_to_ts = int(to_ts_val / 1000)
            else:
                config_to_ts = int(to_ts_val)
        
        # Clamp to_ts to current time to prevent fetching future data
        current_ts = int(time.time())
        if config_to_ts is not None and config_to_ts > current_ts:
            config_to_ts = current_ts
        
        # Fetch historical data
        klines = []
        try:
            klines = await get_cached_klines(symbol, data.get('resolution', '1h'), config_from_ts, config_to_ts)
            logger.info(f"Fetched {len(klines) if klines else 0} klines for config")
        except NameError:
            logger.warning(f"get_cached_klines not available - Redis connection unavailable, returning empty list")
            klines = []
        
        # Calculate indicators
        indicators_data = {}
        try:
            config_indicators = settings_data.get('active_indicators', [])
            indicators_data = await calculate_indicators_for_data(klines, config_indicators)
            logger.info(f"Calculated indicators for config: {list(indicators_data.keys()) if indicators_data else 'none'}")
        except NameError:
            logger.warning(f"calculate_indicators_for_data not available - Redis connection unavailable, returning empty dict")
            indicators_data = {}
        
        # Skip trade fetching for performance - trades will be requested separately by client
        trades = []
        logger.info(f"PERF: Skipping trade fetching in config message - trades will be requested separately")
        
        # Fetch mytrades from trading service - get open positions only for specific email
        mytrades = []
        if email == "klemenzivkovic@gmail.com":
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    response = await client.get("http://localhost:8000/positions")
                    if response.status_code == 200:
                        positions_data = response.json()
                        mytrades = positions_data.get("positions", [])
                        # Add exchange field to identify these as mytrades
                        for trade in mytrades:
                            trade['exchange'] = 'APEXPRO'  # Use APEXPRO to distinguish from regular trades
                            # Convert position data to trade format for chart display
                            trade['price'] = float(trade.get('entry_price', trade.get('entryPrice', 0)))
                            trade['amount'] = float(trade.get('size', 0))
                            # Map LONG/SHORT to BUY/SELL for JavaScript compatibility
                            side = trade.get('side', 'BUY').upper()
                            if side == 'LONG':
                                side = 'BUY'
                            elif side == 'SHORT':
                                side = 'SELL'
                            trade['side'] = side
                            trade['timestamp'] = datetime.fromtimestamp(trade.get('updatedAt', time.time()) / 1000).isoformat()
                            trade['pnl'] = float(trade.get('unrealized_pnl', 0))
                            trade['symbol'] = symbol
                        logger.info(f"CONFIG: Fetched {len(mytrades)} open positions as mytrades for symbol {symbol}")
                    else:
                        logger.warning(f"Failed to fetch mytrades: HTTP {response.status_code}")
            except Exception as e:
                logger.error(f"Error fetching mytrades from trading service: {e}")
        else:
            logger.info(f"Mytrades not fetched for email {email} - only available for klemenzivkovic@gmail.com")
        
        # Fetch drawings
        drawings = []
        try:
            drawings = await get_drawings(symbol, None, None, email)
            logger.info(f"Fetched {len(drawings) if drawings else 0} drawings for config")
        except NameError:
            logger.warning(f"get_drawings not available - Redis connection unavailable, returning empty list")
            drawings = []
        
        # Prepare combined data like in handle_config_message_old
        combined_data = []
        if klines:
            for kline in klines:
                data_point = {
                    "time": kline["time"],
                    "ohlc": {
                        "open": kline["open"],
                        "high": kline["high"],
                        "low": kline["low"],
                        "close": kline["close"],
                        "volume": kline["vol"]
                    },
                    "indicators": {}
                }
                
                # Add indicator values for this timestamp
                for indicator_id, indicator_values in indicators_data.items():
                    if "t" in indicator_values and kline["time"] in indicator_values["t"]:
                        idx = indicator_values["t"].index(kline["time"])
                        temp_indicator = {}
                        for key, values in indicator_values.items():
                            if key != "t" and idx < len(values):
                                temp_indicator[key] = values[idx]
                        if temp_indicator:
                            data_point["indicators"][indicator_id] = temp_indicator
                
                combined_data.append(data_point)
        
        # Return the same response format as handle_config_message_old with actual data
        return {
            "type": "config_success",
            "symbol": symbol,
            "email": email,
            "data": {
                "ohlcv": combined_data,  # Actual OHLC data
                "trades": (trades or [])[:10000],  # Actual trades data
                "mytrades": (mytrades or [])[:1000],  # Actual mytrades data
                "active_indicators": list(indicators_data.keys()),  # Actual indicators
                "drawings": drawings or []  # Actual drawings data
            },
            "timestamp": int(time.time()),
            "request_id": request_id
        }

    except Exception as e:
        logger.error(f"❌ Error saving config: {e}")
        return {
            "type": "error",
            "message": f"Failed to save configuration: {str(e)}",
            "request_id": request_id
        }


async def handle_config_message_old(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle initialization/config message - get data from WebSocket session"""
    session = websocket.scope.get('session', {})

    # Get authentication info from session or config data
    authenticated = session.get('authenticated', False)
    email = data.get('email') or session.get('email')

    active_symbol = data.get("symbol", "BTCUSDT")
    logger.info(f"🔧 Processing config message for symbol {active_symbol}")

    # Parse data from request
    indicators = data.get("active_indicators", [])
    resolution = data.get("resolution", "1h")
    min_value_percentage = data.get("minValuePercentage", 0)

    # Convert timestamp values from xAxisMin/xAxisMax (milliseconds) to seconds
    from_ts_val = data.get("xAxisMin")
    to_ts_val = data.get("xAxisMax")
    logger.debug(f"Raw timestamps: xAxisMin={from_ts_val}, xAxisMax={to_ts_val}")

    # Convert to human readable format for logging
    try:
        if from_ts_val is not None:
            from_ts_readable = datetime.fromtimestamp(from_ts_val / 1000 if from_ts_val > 1e12 else from_ts_val).strftime('%Y-%m-%d %H:%M:%S UTC')
        else:
            from_ts_readable = "None"
        if to_ts_val is not None:
            to_ts_readable = datetime.fromtimestamp(to_ts_val / 1000 if to_ts_val > 1e12 else to_ts_val).strftime('%Y-%m-%d %H:%M:%S UTC')
        else:
            to_ts_readable = "None"
        logger.info(f"Human readable timestamps: xAxisMin={from_ts_readable}, xAxisMax={to_ts_readable}")
    except Exception as e:
        logger.warning(f"Error converting timestamps to readable format: {e}")

    # Convert from_ts
    from_ts = None
    if isinstance(from_ts_val, (int, float)):
        # xAxisMin/xAxisMax are in milliseconds, convert to seconds
        if from_ts_val > 1e12:  # > 1 trillion, likely milliseconds
            from_ts = int(from_ts_val / 1000)
        else:
            from_ts = int(from_ts_val)

    # Convert to_ts
    to_ts = None
    if isinstance(to_ts_val, (int, float)):
        # xAxisMax is in milliseconds, convert to seconds
        if to_ts_val > 1e12:  # > 1 trillion, likely milliseconds
            to_ts = int(to_ts_val / 1000)
        else:
            to_ts = int(to_ts_val)

    logger.info(f"Client initialization: indicators={indicators}, resolution={resolution}, from_ts={from_ts}, to_ts={to_ts}")

    # Update config timestamps if provided in the message
    if from_ts is not None:
        config_from_ts = from_ts
    if to_ts is not None:
        config_to_ts = to_ts

    # Load settings from Redis for stream delta
    stream_delta_seconds = 1  # Default
    try:
        if email:
            redis_conn = await get_redis_connection()
            settings_key = f"settings:{email}:{active_symbol}"
            settings_json = await redis_conn.get(settings_key)
            logger.info(f"DEBUG CONFIG: Redis settings key '{settings_key}' - data exists: {settings_json is not None}")
            if settings_json:
                symbol_settings = json.loads(settings_json)
                stream_delta_seconds = int(symbol_settings.get('streamDeltaTime', 1))
                logger.info(f"Updated stream delta from settings: {stream_delta_seconds} seconds")
    except NameError:
        logger.error(f"get_redis_connection not defined - Redis connection unavailable")
    except Exception as e:
        logger.error(f"Error loading settings for {active_symbol}: {e}")

    # Get the last selected symbol from Redis for config.symbol
    config_symbol = active_symbol  # Default fallback
    config_trade_value_filter = 0  # Default
    config_from_ts = None
    config_to_ts = None
    config_active_indicators = []
    try:
        redis_conn = await get_redis_connection()
        if email:
            last_selected_symbol_key = f"user:{email}:{REDIS_LAST_SELECTED_SYMBOL_KEY}"
            last_symbol = await redis_conn.get(last_selected_symbol_key)
            if last_symbol and last_symbol in SUPPORTED_SYMBOLS:
                config_symbol = last_symbol
                logger.debug(f"Using last selected symbol from Redis: {config_symbol}")
            else:
                logger.debug(f"No last selected symbol found in Redis, using URL symbol: {active_symbol}")

            # Also get config values from Redis settings
            settings_key = f"settings:{email}:{config_symbol}"
            settings_json = await redis_conn.get(settings_key)
            if settings_json:
                symbol_settings = json.loads(settings_json)
                config_trade_value_filter = symbol_settings.get('minValueFilter', 0)
                config_indicators = symbol_settings.get('active_indicators', [])

                # Convert axis timestamps from milliseconds to seconds if needed
                x_axis_min = symbol_settings.get('xAxisMin')
                x_axis_max = symbol_settings.get('xAxisMax')

                if x_axis_min is not None:
                    # Convert to seconds if stored as milliseconds
                    if x_axis_min > 1e12:  # > 1 trillion, likely milliseconds
                        config_from_ts = int(x_axis_min / 1000)
                    else:
                        config_from_ts = int(x_axis_min)

                if x_axis_max is not None:
                    # Convert to seconds if stored as milliseconds
                    if x_axis_max > 1e12:  # > 1 trillion, likely milliseconds
                        config_to_ts = int(x_axis_max / 1000)
                    else:
                        config_to_ts = int(x_axis_max)

                logger.debug(f"Config from Redis - trade_value_filter: {config_trade_value_filter}, from_ts: {config_from_ts}, to_ts: {config_to_ts}, indicators: {config_indicators}")

    except NameError:
        logger.warning(f"get_redis_connection not defined - Redis connection unavailable, using defaults")
    except Exception as redis_e:
        logger.warning(f"Error fetching config data from Redis: {redis_e}, using defaults")

    # Save the updated settings to Redis
    try:
        if email:
            redis_conn = await get_redis_connection()
            settings_key = f"settings:{email}:{active_symbol}"

            # Prepare settings data from the config message
            settings_data = {
                'resolution': data.get('resolution', '1h'),
                'range': data.get('range'),
                'xAxisMin': data.get('xAxisMin'),
                'xAxisMax': data.get('xAxisMax'),
                'yAxisMin': data.get('yAxisMin'),
                'yAxisMax': data.get('yAxisMax'),
                'replayFrom': data.get('replayFrom', ''),
                'replayTo': data.get('replayTo', ''),
                'replaySpeed': data.get('replaySpeed', '1'),
                'useLocalOllama': data.get('useLocalOllama', False),
                'localOllamaModelName': data.get('localOllamaModelName', ''),
                'active_indicators': data.get('active_indicators', []),
                'liveDataEnabled': data.get('liveDataEnabled', True),
                'showAgentTrades': data.get('showAgentTrades', False),
                'streamDeltaTime': data.get('streamDeltaTime', 0),
                'last_selected_symbol': data.get('last_selected_symbol', active_symbol),
                'minValueFilter': data.get('minValueFilter', 0),
                'enableTradeTrace': data.get('enableTradeTrace', True) # New setting
            }

            settings_json = json.dumps(settings_data)
            await redis_conn.set(settings_key, settings_json)
            logger.info(f"Saved settings for {email}:{active_symbol} from config message")
    except NameError:
        logger.error(f"get_redis_connection not defined - Redis connection unavailable")
    except Exception as e:
        logger.error(f"Error saving settings from config message: {e}")

    # Clamp to_ts to current time to prevent fetching future data
    current_ts = int(time.time())
    if config_to_ts is not None and config_to_ts > current_ts:
        logger.info(f"Clamping config_to_ts from {config_to_ts} to current time {current_ts} to prevent fetching future data")
        config_to_ts = current_ts

    # Fetch historical data like in history_success
    try:
        klines = await get_cached_klines(config_symbol, resolution, config_from_ts, config_to_ts)
        logger.info(f"Fetched {len(klines) if klines else 0} klines for config")
    except NameError:
        logger.warning(f"get_cached_klines not available - Redis connection unavailable, returning empty list")
        klines = []

    # Calculate indicators
    try:
        # Fetch OI data if open_interest indicator is requested
        oi_data = None
        if "open_interest" in config_indicators:
            try:
                oi_data = await get_cached_open_interest(config_symbol, resolution, config_from_ts, config_to_ts)
                logger.info(f"Fetched {len(oi_data) if oi_data else 0} OI entries for config")
            except Exception as oi_e:
                logger.warning(f"Failed to fetch OI data for config: {oi_e}")
                oi_data = None

        indicators_data = await calculate_indicators_for_data(klines, config_indicators, oi_data)
        logger.info(f"Calculated indicators for config: {list(indicators_data.keys()) if indicators_data else 'none'}")
    except NameError:
        logger.warning(f"calculate_indicators_for_data not available - Redis connection unavailable, returning empty dict")
        indicators_data = {}

    # Skip trade fetching for performance - trades will be requested separately by client
    trades = []
    logger.info(f"PERF: Skipping trade fetching in config message - trades will be requested separately")

    # Fetch mytrades from trading service - get open positions only for specific email
    mytrades = []
    if email == "klemenzivkovic@gmail.com":
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get("http://localhost:8000/positions")
                if response.status_code == 200:
                    positions_data = response.json()
                    mytrades = positions_data.get("positions", [])
                    # Add exchange field to identify these as mytrades
                    for trade in mytrades:
                        trade['exchange'] = 'APEXPRO'  # Use APEXPRO to distinguish from regular trades
                        # Convert position data to trade format for chart display
                        trade['price'] = float(trade.get('entry_price', trade.get('entryPrice', 0)))
                        trade['amount'] = float(trade.get('size', 0))
                        # Map LONG/SHORT to BUY/SELL for JavaScript compatibility
                        side = trade.get('side', 'BUY').upper()
                        if side == 'LONG':
                            side = 'BUY'
                        elif side == 'SHORT':
                            side = 'SELL'
                        trade['side'] = side
                        trade['timestamp'] = datetime.fromtimestamp(trade.get('updatedAt', time.time()) / 1000).isoformat()
                        trade['pnl'] = float(trade.get('unrealized_pnl', 0))
                        trade['symbol'] = config_symbol
                    logger.info(f"CONFIG: Fetched {len(mytrades)} open positions as mytrades for symbol {config_symbol}")
                else:
                    logger.warning(f"Failed to fetch mytrades: HTTP {response.status_code}")
        except Exception as e:
            logger.error(f"Error fetching mytrades from trading service: {e}")
    else:
        logger.info(f"Mytrades not fetched for email {email} - only available for klemenzivkovic@gmail.com")

    if trades and len(trades) > 0:
        logger.info(f"HISTORY: First trade sample: {json.dumps(trades[0])}")

    # Fetch drawings
    try:
        drawings = await get_drawings(config_symbol, None, None, email)
        logger.info(f"Fetched {len(drawings) if drawings else 0} drawings for config")
    except NameError:
        logger.warning(f"get_drawings not available - Redis connection unavailable, returning empty list")
        drawings = []

    # Calculate volume profile for rectangle drawings if we have a time range
    if drawings and config_from_ts is not None and config_to_ts is not None:
        logger.info(f"Processing {len(drawings)} drawings for volume profile calculation in config")
        for drawing in drawings:
            drawing_id = drawing.get('id')
            drawing_type = drawing.get('type')
            logger.info(f"Processing drawing {drawing_id}, type: {drawing_type}")
            if drawing_type == 'rect':
                start_time_val = drawing.get('start_time')
                end_time_val = drawing.get('end_time')
                start_price = drawing.get('start_price')
                end_price = drawing.get('end_price')
                logger.info(f"Rectangle {drawing_id}: start_time={start_time_val}, end_time={end_time_val}, start_price={start_price}, end_price={end_price}")
                if all([start_time_val, end_time_val, start_price is not None, end_price is not None]):
                    # Convert timestamps from milliseconds to seconds if needed
                    if start_time_val > 1e12:  # > 1 trillion, likely milliseconds
                        start_time = int(start_time_val / 1000)
                        logger.info(f"Converted start_time from ms to s: {start_time}")
                    else:
                        start_time = int(start_time_val)
                    if end_time_val > 1e12:  # > 1 trillion, likely milliseconds
                        end_time = int(end_time_val / 1000)
                        logger.info(f"Converted end_time from ms to s: {end_time}")
                    else:
                        end_time = int(end_time_val)

                    logger.info(f"Rectangle {drawing_id} time range: {start_time} to {end_time}")

                    price_min = min(float(start_price), float(end_price))
                    price_max = max(float(start_price), float(end_price))
                    logger.info(f"Rectangle {drawing_id} price range: {price_min} to {price_max}")

                    # Only calculate volume profile if rectangle time range intersects with config time range
                    intersects = start_time <= config_to_ts and end_time >= config_from_ts
                    logger.info(f"Rectangle {drawing_id} intersects with config range ({config_from_ts} to {config_to_ts}): {intersects}")
                    if intersects:
                        # Fetch klines for rectangle time range
                        rect_klines = await get_cached_klines(config_symbol, resolution, start_time, end_time)
                        logger.info(f"Fetched {len(rect_klines) if rect_klines else 0} klines for rectangle {drawing_id}")
                        if rect_klines:
                            # Filter klines within price range
                            filtered_klines = [
                                k for k in rect_klines
                                if k.get('high', 0) >= price_min and k.get('low', 0) <= price_max
                            ]
                            logger.info(f"Filtered to {len(filtered_klines)} klines within price range for rectangle {drawing_id}")
                            if filtered_klines:
                                volume_profile_data = calculate_volume_profile(filtered_klines, drawing_id)
                                drawing['volume_profile'] = volume_profile_data
                                logger.info(f"Added volume profile to rectangle {drawing_id} with {len(volume_profile_data.get('volume_profile', []))} levels")
                            else:
                                logger.info(f"No klines in price range for rectangle {drawing_id}")
                        else:
                            logger.info(f"No klines for rectangle {drawing_id} time range")
                else:
                    logger.info(f"Incomplete rectangle data for drawing {drawing_id}")
            else:
                logger.info(f"Skipping non-rectangle drawing {drawing_id}, type: {drawing_type}")

    # Prepare combined data
    combined_data = []
    if klines:
        for kline in klines:
            data_point = {
                "time": kline["time"],
                "ohlc": {
                    "open": kline["open"],
                    "high": kline["high"],
                    "low": kline["low"],
                    "close": kline["close"],
                    "volume": kline["vol"]
                },
                "indicators": {}
            }

            # Add indicator values for this timestamp
            for indicator_id, indicator_values in indicators_data.items():
                if "t" in indicator_values and kline["time"] in indicator_values["t"]:
                    idx = indicator_values["t"].index(kline["time"])
                    temp_indicator = {}
                    for key, values in indicator_values.items():
                        if key != "t" and idx < len(values):
                            temp_indicator[key] = values[idx]
                    if temp_indicator:
                        data_point["indicators"][indicator_id] = temp_indicator

            combined_data.append(data_point)

    # Log data range comparison before returning
    if combined_data:
        first_data = combined_data[0] if combined_data else None
        last_data = combined_data[-1] if combined_data else None
        logger.info(f"CONFIG DATA SUMMARY: Requested range {config_from_ts} to {config_to_ts} ({datetime.fromtimestamp(config_from_ts, timezone.utc).isoformat() if config_from_ts else 'None'} to {datetime.fromtimestamp(config_to_ts, timezone.utc).isoformat() if config_to_ts else 'None'})")
        logger.info(f"CONFIG DATA SUMMARY: Returned {len(combined_data)} data points, first: time={first_data['time']} ({datetime.fromtimestamp(first_data['time'], timezone.utc).isoformat()}), last: time={last_data['time']} ({datetime.fromtimestamp(last_data['time'], timezone.utc).isoformat()})")
        logger.info(f"CONFIG DATA SUMMARY: First OHLC: {first_data['ohlc']}, Last OHLC: {last_data['ohlc']}")
    else:
        logger.info(f"CONFIG DATA SUMMARY: No data returned for requested range {config_from_ts} to {config_to_ts}")

    # Return configuration success response
    return {
        "type": "config_success",
        "symbol": config_symbol,
        "email": email,
        "data": {
            "ohlcv": combined_data,
            "trades": (trades or [])[:10000],
            "mytrades": (mytrades or [])[:1000],
            "active_indicators": list(indicators_data.keys()),
            "drawings": drawings or []
        },
        "timestamp": int(time.time()),
        "request_id": request_id
    }
    

async def handle_init_message(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle initialization/setup messages - includes settings delivery"""
    try:
        # Get session data from WebSocket
        session = websocket.scope.get('session', {})

        # Use init data from request (not storing in session since we're stateless)
        symbol = data.get('symbol', 'BTCUSDT')
        indicators = data.get('indicators', [])
        from_ts = data.get('from_ts')
        to_ts = data.get('to_ts')

        logger.info(f"Client initialized: symbol={symbol}, authenticated={bool(session.get('email'))}")

        # Load user config from Redis
        config_data = {}
        last_selected_symbol = symbol  # Default fallback
        try:
            redis = await get_redis_connection()
            email = session.get('email')
            if email:
                # Get last selected symbol from global user key
                last_selected_symbol_key = f"user:{email}:{REDIS_LAST_SELECTED_SYMBOL_KEY}"
                last_symbol = await redis.get(last_selected_symbol_key)
                if last_symbol and last_symbol in SUPPORTED_SYMBOLS:
                    last_selected_symbol = last_symbol
                    logger.debug(f"Using last selected symbol from Redis: {last_selected_symbol}")
                else:
                    logger.debug(f"No last selected symbol found in Redis, using init symbol: {symbol}")

                # Load symbol-specific config
                if symbol:
                    config_key = f"settings:{email}:{symbol}"
                    config_json = await redis.get(config_key)
                    if config_json:
                        config_data = json.loads(config_json)
                        logger.debug(f"Loaded config for {email}:{symbol} from Redis: {config_data}")
                    else:
                        logger.info(f"No config found in Redis for {email}:{symbol}, using empty config")
                        config_data = {}
        except Exception as config_error:
            logger.error(f"Error loading config for {session.get('email')}:{symbol}: {config_error}")
            config_data = {}

        # Extract config values from config_data or use defaults
        config_from_ts = config_data.get('xAxisMin') if config_data else None
        config_to_ts = config_data.get('xAxisMax') if config_data else None
        config_trade_value_filter = config_data.get('minValueFilter', 0) if config_data else 0
        config_indicators = config_data.get('active_indicators', []) if config_data else []

        # Convert timestamps from milliseconds to seconds if needed
        if config_from_ts is not None:
            try:
                config_from_ts = float(config_from_ts)  # Ensure it's a number
                if config_from_ts > 1e12:  # > 1 trillion, likely milliseconds
                    config_from_ts = int(config_from_ts / 1000)
            except (ValueError, TypeError):
                logger.warning(f"Invalid config_from_ts value: {config_from_ts}, setting to None")
                config_from_ts = None
        if config_to_ts is not None:
            try:
                config_to_ts = float(config_to_ts)  # Ensure it's a number
                if config_to_ts > 1e12:  # > 1 trillion, likely milliseconds
                    config_to_ts = int(config_to_ts / 1000)
            except (ValueError, TypeError):
                logger.warning(f"Invalid config_to_ts value: {config_to_ts}, setting to None")
                config_to_ts = None

        # Build the complete config object with all required fields and defaults
        config_obj = {
            "symbol": symbol,
            "resolution": config_data.get('resolution', '1h'), 
            "range": config_data.get('range', '24h'),
            "xAxisMin": config_data.get('xAxisMin'),
            "xAxisMax": config_data.get('xAxisMax'),
            "yAxisMin": config_data.get('yAxisMin'),
            "yAxisMax": config_data.get('yAxisMax'),
            "replayFrom": config_data.get('replayFrom', ''),
            "replayTo": config_data.get('replayTo', ''),
            "replaySpeed": config_data.get('replaySpeed', '1'),
            "useLocalOllama": config_data.get('useLocalOllama', False),
            "localOllamaModelName": config_data.get('localOllamaModelName', ''),
            "active_indicators": config_data.get('active_indicators', []),
            "liveDataEnabled": config_data.get('liveDataEnabled', True),
            "showAgentTrades": config_data.get('showAgentTrades', False),
            "streamDeltaTime": config_data.get('streamDeltaTime', 0),
            "last_selected_symbol": last_selected_symbol,
            "minValueFilter": config_data.get('minValueFilter', 0),
            "enableTradeTrace": config_data.get('enableTradeTrace', True), # New setting
            "email": session.get('email')
        }

        return {
            "type": "init_success",
            "data": {
                "authenticated": bool(session.get('email')),
                "config": config_obj,
                "symbols": SUPPORTED_SYMBOLS
            },
            "request_id": request_id
        }

    except Exception as e:
        logger.error(f"Error handling init message: {e}", exc_info=True)
        return {
            "type": "error",
            "message": f"Initialization failed: {str(e)}",
            "request_id": request_id
        }


async def handle_request_action(action: str, data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Route request actions to appropriate handlers"""
    session = websocket.scope.get('session', {})


    # Drawing actions
    if action == "get_drawings":
        return await handle_get_drawings(data, websocket, request_id)
    elif action == "save_drawing":
        return await handle_save_drawing(data, websocket, request_id)
    elif action == "update_drawing":
        return await handle_update_drawing(data, websocket, request_id)
    elif action == "delete_drawing":
        return await handle_delete_drawing(data, websocket, request_id)
    elif action == "delete_all_drawings":
        return await handle_delete_all_drawings(data, websocket, request_id)

    # AI actions
    elif action == "ai_suggestion":
        return await handle_ai_suggestion(data, websocket, request_id)
    elif action == "get_local_ollama_models":
        return await handle_get_local_ollama_models(data, websocket, request_id)
    elif action == "get_available_indicators":
        return await handle_get_available_indicators(data, websocket, request_id)

    # Trading actions
    elif action == "get_agent_trades":
        return await handle_get_agent_trades(data, websocket, request_id)
    elif action == "get_order_history":
        return await handle_get_order_history(data, websocket, request_id)
    elif action == "get_buy_signals":
        return await handle_get_buy_signals(data, websocket, request_id)

    # Utility actions
    elif action == "get_settings":
        return await handle_get_settings(data, websocket, request_id)
    elif action == "set_settings":
        return await handle_set_settings(data, websocket, request_id)
    elif action == "set_last_symbol":
        return await handle_set_last_symbol(data, websocket, request_id)
    elif action == "get_last_symbol":
        return await handle_get_last_symbol(data, websocket, request_id)
    elif action == "get_live_price":
        return await handle_get_live_price(data, websocket, request_id)

    # Data actions
    elif action == "get_history":
        return await handle_get_history(data, websocket, request_id)
    elif action == "get_initial_chart_config":
        return await handle_get_initial_chart_config(data, websocket, request_id)
    elif action == "get_symbols":
        return await handle_get_symbols(data, websocket, request_id)
    elif action == "get_symbols_list":
        return await handle_get_symbols_list(data, websocket, request_id)
    elif action == "get_trade_history":
        return await handle_get_trade_history(data, websocket, request_id)

    # Volume profile and analysis
    elif action == "get_volume_profile":
        return await handle_get_volume_profile(data, websocket, request_id)
    elif action == "get_trading_sessions":
        return await handle_get_trading_sessions(data, websocket, request_id)

    # Auth and user actions
    elif action == "authenticate":
        return await handle_authenticate(data, websocket, request_id)

    else:
        logger.warn(f"Unknown WS request action: {action}")
        return {
            "type": "error",
            "message": f"Unknown action: {action}",
            "request_id": request_id
        }


# Handler functions for each action type

async def handle_get_drawings(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_drawings request"""
    try:
        # Get email from WebSocket session
        session = websocket.scope.get('session', {})
        email = session.get('email')

        symbol = data.get('symbol', 'BTCUSDT')  # Default fallback
        resolution = data.get('resolution', '1h')  # Default fallback

        drawings = await get_drawings(symbol, None, None, email)

        return {
            "type": "response",
            "action": "get_drawings",
            "success": True,
            "data": {"drawings": drawings},
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_drawings: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_save_drawing(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle save_drawing request"""
    session = websocket.scope.get('session', {})

    try:
        # Reuse existing endpoint logic
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        # Call the existing endpoint logic
        result = await save_drawing_api_endpoint(
            symbol=data.get('symbol'),
            drawing_data=data,
            request=fake_request
        )

        return {
            "type": "response",
            "action": "save_drawing",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_save_drawing: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_update_drawing(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle update_drawing request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await update_drawing_api_endpoint(
            symbol=data.get('symbol'),
            drawing_id=str(data.get('drawing_id')),
            drawing_data=data,
            request=fake_request
        )

        return {
            "type": "response",
            "action": "update_drawing",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_update_drawing: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_delete_drawing(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle delete_drawing request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await delete_drawing_api_endpoint(
            symbol=data.get('symbol'),
            drawing_id=str(data.get('drawing_id')),
            request=fake_request
        )

        return {
            "type": "response",
            "action": "delete_drawing",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_delete_drawing: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_delete_all_drawings(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle delete_all_drawings request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await delete_all_drawings_api_endpoint(
            symbol=data.get('symbol'),
            request=fake_request
        )

        return {
            "type": "response",
            "action": "delete_all_drawings",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_delete_all_drawings: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_ai_suggestion(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle AI suggestion request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await ai_suggestion_endpoint(
            ai_request=data,
            request=fake_request
        )

        return {
            "type": "response",
            "action": "ai_suggestion",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_ai_suggestion: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_local_ollama_models(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_local_ollama_models request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await get_local_ollama_models_endpoint(request=fake_request)

        return {
            "type": "response",
            "action": "get_local_ollama_models",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_local_ollama_models: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_available_indicators(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_available_indicators request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await get_available_indicators_endpoint(request=fake_request)

        return {
            "type": "response",
            "action": "get_available_indicators",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_available_indicators: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_agent_trades(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_agent_trades request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await get_agent_trades_endpoint(request=fake_request)

        return {
            "type": "response",
            "action": "get_agent_trades",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_agent_trades: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_order_history(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_order_history request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await get_order_history_endpoint(
            symbol=data.get('symbol'),
            request=fake_request
        )

        return {
            "type": "response",
            "action": "get_order_history",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_order_history: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_buy_signals(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_buy_signals request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await get_buy_signals_endpoint(
            symbol=data.get('symbol'),
            request=fake_request
        )

        return {
            "type": "response",
            "action": "get_buy_signals",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_buy_signals: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_settings(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_settings request - load directly from Redis"""
    session = websocket.scope.get('session', {})

    try:
        symbol = data.get('symbol', session.get('symbol'))
        email = session.get('email')

        if not email:
            return {
                "type": "error",
                "message": "Not authenticated",
                "request_id": request_id
            }

        # Load settings directly from Redis
        redis_conn = await get_redis_connection()
        settings_key = f"settings:{email}:{symbol}"
        settings_json = await redis_conn.get(settings_key)

        if settings_json:
            settings_data = json.loads(settings_json)
            logger.info(f"Retrieved settings for {email}:{symbol} from Redis")
        else:
            # Return defaults if no settings found
            settings_data = {
                "active_indicators": session.get('active_indicators', []),
                "resolution": session.get('resolution', "1h"),
                "from_ts": session.get('from_ts'),
                "to_ts": session.get('to_ts'),
                "xAxisMin": None,
                "xAxisMax": None,
                "yAxisMin": None,
                "yAxisMax": None,
                "streamDeltaTime": 0
            }
            logger.info(f"No settings found for {email}:{symbol}, using defaults")

        return {
            "type": "response",
            "action": "get_settings",
            "success": True,
            "data": settings_data,
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_settings: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_set_settings(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle set_settings request - save directly to Redis"""
    session = websocket.scope.get('session', {})
    logger.info(f"DRAWING: {json.dumps(data, indent=2)}")
    logger.info(f"DRAWING: {json.dumps(data, indent=2)}")

    try:
        symbol = data.get('symbol', session.get('symbol'))
        email = session.get('email')

        if not email:
            return {
                "type": "error",
                "message": "Not authenticated",
                "request_id": request_id
            }

        # Extract settings data from the request
        settings_data = {
            k: v for k, v in data.items() if k not in ['symbol', 'request_id']
        }

        # Ensure required fields are set
        if 'last_selected_symbol' not in settings_data:
            settings_data['last_selected_symbol'] = symbol

        # Save settings directly to Redis
        redis_conn = await get_redis_connection()
        settings_key = f"settings:{email}:{symbol}"
        settings_json = json.dumps(settings_data)
        await redis_conn.set(settings_key, settings_json)

        logger.info(f"Saved settings for {email}:{symbol} to Redis: {list(settings_data.keys())}")

        return {
            "type": "response",
            "action": "set_settings",
            "success": True,
            "data": settings_data,
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_set_settings: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_set_last_symbol(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle set_last_symbol request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await set_last_selected_symbol(
            symbol=data.get('symbol'),
            request=fake_request
        )

        return {
            "type": "response",
            "action": "set_last_symbol",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_set_last_symbol: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_last_symbol(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_last_symbol request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await get_last_selected_symbol(request=fake_request)

        return {
            "type": "response",
            "action": "get_last_symbol",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_last_symbol: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_live_price(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_live_price request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await get_live_price(request=fake_request)

        return {
            "type": "response",
            "action": "get_live_price",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_live_price: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_history(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_history request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await history_endpoint(
            symbol=data.get('symbol'),
            resolution=data.get('resolution'),
            from_ts=data.get('from_ts'),
            to_ts=data.get('to_ts'),
            request=fake_request
        )

        return {
            "type": "response",
            "action": "get_history",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_history: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_initial_chart_config(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_initial_chart_config request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await initial_chart_config(symbol=data.get('symbol'), request=fake_request)

        return {
            "type": "response",
            "action": "get_initial_chart_config",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_initial_chart_config: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_symbols(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_symbols request"""
    session = websocket.scope.get('session', {})

    try:
        from endpoints.chart_endpoints import symbols_endpoint
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await symbols_endpoint(request=fake_request)

        return {
            "type": "response",
            "action": "get_symbols",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_symbols: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_symbols_list(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_symbols_list request"""
    session = websocket.scope.get('session', {})

    try:
        from endpoints.chart_endpoints import symbols_list_endpoint
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        result = await symbols_list_endpoint(request=fake_request)

        return {
            "type": "response",
            "action": "get_symbols_list",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_symbols_list: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_trade_history(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_trade_history request"""
    session = websocket.scope.get('session', {})

    try:
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {}})

        # Use indicator_history_endpoint for now - may need to adjust
        result = await indicator_history_endpoint(
            symbol=data.get('symbol'),
            resolution=data.get('resolution'),
            from_ts=data.get('from_ts'),
            to_ts=data.get('to_ts'),
            request=fake_request
        )

        return {
            "type": "response",
            "action": "get_trade_history",
            "success": result.status_code == 200,
            "data": result.body.decode('utf-8') if hasattr(result, 'body') else str(result),
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_trade_history: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_volume_profile(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_volume_profile request"""
    session = websocket.scope.get('session', {})

    try:
        # Implement volume profile calculation
        klines = await get_cached_klines(
            symbol=data.get('symbol', session.get('symbol')),
            resolution=data.get('resolution', session.get('resolution')),
            from_ts=data.get('from_ts'),
            to_ts=data.get('to_ts')
        )

        if klines:
            volume_data = calculate_volume_profile(klines)
            return {
                "type": "response",
                "action": "get_volume_profile",
                "success": True,
                "data": volume_data,
                "request_id": request_id
            }
        else:
            return {
                "type": "response",
                "action": "get_volume_profile",
                "success": False,
                "data": {"error": "No klines data available"},
                "request_id": request_id
            }
    except Exception as e:
        logger.error(f"Error in handle_get_volume_profile: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_trading_sessions(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle get_trading_sessions request"""
    session = websocket.scope.get('session', {})

    try:
        sessions = await calculate_trading_sessions(
            symbol=data.get('symbol', session.get('symbol')),
            from_ts=data.get('from_ts'),
            to_ts=data.get('to_ts')
        )

        return {
            "type": "get_trading_sessions_response",
            "success": True,
            "data": {"sessions": sessions},
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_get_trading_sessions: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_history_message(data: dict, websocket: WebSocket, request_id: str, client_id: str = None) -> dict:
    """Handle history request from client"""
    session = websocket.scope.get('session', {})

    function_start_time = time.time()

    try:
        # Get parameters from message data
        param_start = time.time()
        symbol = data.get("symbol", "BTCUSDT")
        email = data.get("email") or session.get('email')
        min_value_percentage = data.get("minValuePercentage", 0)
        from_ts_val = data.get("from_ts")
        to_ts_val = data.get("to_ts")
        resolution = data.get("resolution", "1h")
        indicators = data.get("active_indicators", [])

        # Convert timestamps from milliseconds to seconds if needed (same logic as config message)
        from_ts = None
        if isinstance(from_ts_val, (int, float)):
            # from_ts/to_ts are in milliseconds, convert to seconds
            if from_ts_val > 1e12:  # > 1 trillion, likely milliseconds
                from_ts = int(from_ts_val / 1000)
            else:
                from_ts = int(from_ts_val)

        to_ts = None
        if isinstance(to_ts_val, (int, float)):
            # to_ts is in milliseconds, convert to seconds
            if to_ts_val > 1e12:  # > 1 trillion, likely milliseconds
                to_ts = int(to_ts_val / 1000)
            else:
                to_ts = int(to_ts_val)

        logger.info(f"DEBUG: Raw timestamps from client: from_ts_val={from_ts_val}, to_ts_val={to_ts_val}")
        logger.info(f"DEBUG: Converted timestamps: from_ts={from_ts}, to_ts={to_ts}")
        if from_ts:
            logger.info(f"DEBUG: from_ts ISO:   {datetime.fromtimestamp(from_ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        if to_ts:
            logger.info(f"DEBUG: to_ts ISO:     {datetime.fromtimestamp(to_ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

        param_elapsed = time.time() - param_start
        logger.info(f"PERF: Parameter extraction took {param_elapsed:.3f} seconds")

        # Adjust from_ts to avoid sending duplicate data - use last_sent_timestamp if available
        adjust_start = time.time()
        if hasattr(app.state, 'active_websockets') and client_id in app.state.active_websockets:
            last_sent_ts = app.state.active_websockets[client_id].get('last_sent_timestamp')
            if last_sent_ts is not None and from_ts is not None and last_sent_ts >= from_ts:
                # Client already has data up to last_sent_ts, so start from the next timestamp
                from_ts = last_sent_ts + 1
                logger.info(f"Adjusted from_ts to {from_ts} to avoid sending duplicate data (last_sent_ts was {last_sent_ts})")
        adjust_elapsed = time.time() - adjust_start
        logger.info(f"PERF: Timestamp adjustment took {adjust_elapsed:.3f} seconds")

        logger.info(f"Processing history request for symbol {symbol}, email {email}, minValuePercentage {min_value_percentage}, from_ts={from_ts}, to_ts={to_ts}")

        # Validation section
        validation_start = time.time()
        if not from_ts or not to_ts:
            validation_elapsed = time.time() - validation_start
            logger.info(f"PERF: Validation (early exit) took {validation_elapsed:.3f} seconds")
            return {
                "type": "error",
                "message": "No time range specified for history request",
                "request_id": request_id
            }

        # Clamp to_ts to current time to prevent fetching future data
        current_ts = int(time.time())
        if to_ts is not None and to_ts > current_ts:
            logger.info(f"Clamping to_ts from {to_ts} to current time {current_ts} to prevent fetching future data")
            to_ts = current_ts
        validation_elapsed = time.time() - validation_start
        logger.info(f"PERF: Validation took {validation_elapsed:.3f} seconds")

        # Fetch historical klines
        klines_start = time.time()
        logger.info(f"DEBUG: Calling get_cached_klines with symbol={symbol}, resolution={resolution}, from_ts={from_ts}, to_ts={to_ts}")
        klines = await get_cached_klines(symbol, resolution, from_ts, to_ts)
        klines_elapsed = time.time() - klines_start
        logger.info(f"PERF: Fetching klines took {klines_elapsed:.3f} seconds - Fetched {len(klines) if klines else 0} klines for history")
        logger.info(f"DEBUG: First few klines timestamps: {[k.get('time') for k in klines[:3]] if klines else 'No klines'}")

        # Calculate indicators
        indicators_start = time.time()
        # Fetch OI data if open_interest indicator is requested
        oi_data = None
        if "open_interest" in indicators:
            try:
                oi_data = await get_cached_open_interest(symbol, resolution, from_ts, to_ts)
                logger.info(f"PERF: Fetched {len(oi_data) if oi_data else 0} OI entries for history")
            except Exception as oi_e:
                logger.warning(f"Failed to fetch OI data for history: {oi_e}")
                oi_data = None

        indicators_data = await calculate_indicators_for_data(klines, indicators, oi_data)
        indicators_elapsed = time.time() - indicators_start
        logger.info(f"PERF: Calculating indicators took {indicators_elapsed:.3f} seconds - Calculated indicators: {list(indicators_data.keys()) if indicators_data else 'none'}")

        # Skip trade fetching for performance - trades will be requested separately by client
        trades = []
        logger.info(f"PERF: Skipping trade fetching in history message - trades will be requested separately")

        # Fetch mytrades from trading service - get open positions only for specific email
        mytrades = []
        if email == "klemen.zivkovic@gmail.com":
            mytrades_start = time.time()
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    response = await client.get("http://localhost:8000/positions")
                    if response.status_code == 200:
                        positions_data = response.json()
                        mytrades = positions_data.get("positions", [])
                        # Add exchange field to identify these as mytrades
                        for trade in mytrades:
                            trade['exchange'] = 'APEXPRO'  # Use APEXPRO to distinguish from regular trades
                            # Convert position data to trade format for chart display
                            trade['price'] = float(trade.get('entry_price', trade.get('entryPrice', 0)))
                            trade['amount'] = float(trade.get('size', 0))
                            # Map LONG/SHORT to BUY/SELL for JavaScript compatibility
                            side = trade.get('side', 'BUY').upper()
                            if side == 'LONG':
                                side = 'BUY'
                            elif side == 'SHORT':
                                side = 'SELL'
                            trade['side'] = side
                            trade['timestamp'] = datetime.fromtimestamp(trade.get('updatedTime', time.time()) / 1000).isoformat()
                            trade['pnl'] = float(trade.get('unrealized_pnl', 0))
                            trade['symbol'] = symbol
                        mytrades_elapsed = time.time() - mytrades_start
                        logger.info(f"PERF: Fetching mytrades took {mytrades_elapsed:.3f} seconds - HISTORY: Fetched {len(mytrades)} open positions as mytrades for symbol {symbol}")
                    else:
                        logger.warning(f"Failed to fetch mytrades: HTTP {response.status_code}")
                        mytrades_elapsed = time.time() - mytrades_start
                        logger.info(f"PERF: Fetching mytrades took {mytrades_elapsed:.3f} seconds (failed)")
            except Exception as e:
                logger.error(f"Error fetching mytrades from trading service: {e}")
                mytrades_elapsed = time.time() - mytrades_start
                logger.info(f"PERF: Fetching mytrades took {mytrades_elapsed:.3f} seconds (error)")
        else:
            logger.info(f"Mytrades not fetched for email {email} - only available for klemenzivkovic@gmail.com")

        # Fetch drawings
        drawings_start = time.time()
        drawings = await get_drawings(symbol, None, None, email)
        drawings_elapsed = time.time() - drawings_start
        logger.info(f"PERF: Fetching drawings took {drawings_elapsed:.3f} seconds - Fetched {len(drawings) if drawings else 0} drawings for history")

        # Calculate volume profile for rectangle drawings
        volume_profile_start = time.time()
        if drawings:
            logger.info(f"Processing {len(drawings)} drawings for volume profile calculation")
            for drawing in drawings:
                drawing_id = drawing.get('id')
                drawing_type = drawing.get('type')
                logger.info(f"Processing drawing {drawing_id}, type: {drawing_type}")
                if drawing_type == 'rect':
                    start_time_val = drawing.get('start_time')
                    end_time_val = drawing.get('end_time')
                    start_price = drawing.get('start_price')
                    end_price = drawing.get('end_price')
                    logger.info(f"Rectangle {drawing_id}: start_time={start_time_val}, end_time={end_time_val}, start_price={start_price}, end_price={end_price}")
                    if all([start_time_val, end_time_val, start_price is not None, end_price is not None]):
                        # Convert timestamps from milliseconds to seconds if needed
                        if start_time_val > 1e12:  # > 1 trillion, likely milliseconds
                            start_time = int(start_time_val / 1000)
                            logger.info(f"Converted start_time from ms to s: {start_time}")
                        else:
                            start_time = int(start_time_val)
                        if end_time_val > 1e12:  # > 1 trillion, likely milliseconds
                            end_time = int(end_time_val / 1000)
                            logger.info(f"Converted end_time from ms to s: {end_time}")
                        else:
                            end_time = int(end_time_val)

                        logger.info(f"Rectangle {drawing_id} time range: {start_time} to {end_time}")

                        price_min = min(float(start_price), float(end_price))
                        price_max = max(float(start_price), float(end_price))
                        logger.info(f"Rectangle {drawing_id} price range: {price_min} to {price_max}")

                        # Only calculate volume profile if rectangle time range intersects with history time range
                        intersects = start_time <= to_ts and end_time >= from_ts
                        logger.info(f"Rectangle {drawing_id} intersects with history range ({from_ts} to {to_ts}): {intersects}")
                        if intersects:
                            # Fetch klines for rectangle time range
                            rect_klines = await get_cached_klines(symbol, resolution, start_time, end_time)
                            logger.info(f"Fetched {len(rect_klines) if rect_klines else 0} klines for rectangle {drawing_id}")
                            if rect_klines:
                                # Filter klines within price range
                                filtered_klines = [
                                    k for k in rect_klines
                                    if k.get('high', 0) >= price_min and k.get('low', 0) <= price_max
                                ]
                                logger.info(f"Filtered to {len(filtered_klines)} klines within price range for rectangle {drawing_id}")
                                if filtered_klines:
                                    volume_profile_data = calculate_volume_profile(filtered_klines, drawing['id'])
                                    drawing['volume_profile'] = volume_profile_data
                                    logger.info(f"Added volume profile to rectangle {drawing_id} with {len(volume_profile_data.get('volume_profile', []))} levels")
                                else:
                                    logger.info(f"No klines in price range for rectangle {drawing_id}")
                            else:
                                logger.info(f"No klines for rectangle {drawing_id} time range")
                    else:
                        logger.info(f"Incomplete rectangle data for drawing {drawing_id}")
                else:
                    logger.info(f"Skipping non-rectangle drawing {drawing_id}, type: {drawing_type}")
        volume_profile_elapsed = time.time() - volume_profile_start
        logger.info(f"PERF: Volume profile calculation took {volume_profile_elapsed:.3f} seconds")

        # Prepare combined data
        data_prep_start = time.time()
        combined_data = []
        if klines:
            for kline in klines:
                data_point = {
                    "time": kline["time"],
                    "ohlc": {
                        "open": kline["open"],
                        "high": kline["high"],
                        "low": kline["low"],
                        "close": kline["close"],
                        "volume": kline["vol"]
                    },
                    "indicators": {}
                }

                # Add indicator values for this timestamp
                for indicator_id, indicator_values in indicators_data.items():
                    if "t" in indicator_values and kline["time"] in indicator_values["t"]:
                        idx = indicator_values["t"].index(kline["time"])
                        temp_indicator = {}
                        for key, values in indicator_values.items():
                            if key != "t" and idx < len(values):
                                temp_indicator[key] = values[idx]
                        if temp_indicator:
                            data_point["indicators"][indicator_id] = temp_indicator

                combined_data.append(data_point)
        data_prep_elapsed = time.time() - data_prep_start
        logger.info(f"PERF: Data preparation took {data_prep_elapsed:.3f} seconds - Prepared {len(combined_data)} data points")

        # Debug: output drawings JSON
        # logger.info(f"HISTORY DRAWINGS: {json.dumps(drawings or [], default=str)}")

        # Update client's last sent timestamp to the latest OHLC data timestamp
        timestamp_update_start = time.time()
        if combined_data and hasattr(app.state, 'active_websockets') and client_id in app.state.active_websockets:
            latest_timestamp = max(kline['time'] for kline in combined_data) if combined_data else None
            app.state.active_websockets[client_id]['last_sent_timestamp'] = latest_timestamp
            logger.debug(f"Updated last_sent_timestamp for client {client_id} to {latest_timestamp}")
        timestamp_update_elapsed = time.time() - timestamp_update_start
        logger.info(f"PERF: Timestamp update took {timestamp_update_elapsed:.3f} seconds")

        # Calculate total function execution time
        total_elapsed = time.time() - function_start_time
        logger.info(f"PERF: Total handle_history_message execution took {total_elapsed:.3f} seconds")

        # Return history_success message
        return {
            "type": "history_success",
            "symbol": symbol,
            "email": email,
            "data": {
                "ohlcv": combined_data,
                "trades": (trades or [])[:10000],
                "mytrades": (mytrades or [])[:1000],
                "active_indicators": list(indicators_data.keys()),
                "drawings": drawings or []
            },
            "timestamp": int(time.time()),
            "request_id": request_id
        }

    except Exception as e:
        logger.error(f"Error in handle_history_message: {e}", exc_info=True)
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_trade_history_message(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle trade_history request from client (for filtering)"""
    session = websocket.scope.get('session', {})

    try:
        # Get parameters from message data
        symbol = data.get("symbol", "BTCUSDT")
        email = data.get("email") or session.get('email')
        value_filter = data.get("minValuePercentage", 0)
        from_ts = data.get("from_ts")
        to_ts = data.get("to_ts")

        # Convert timestamps from milliseconds to seconds if needed (same logic as other functions)
        if from_ts and from_ts > 1e12:  # > 1 trillion, likely milliseconds
            from_ts = int(from_ts / 1000)
        if to_ts and to_ts > 1e12:  # > 1 trillion, likely milliseconds
            to_ts = int(to_ts / 1000)

        logger.info(f"Processing trade_history request for symbol {symbol}, email {email}, valueFilter {value_filter}, from_ts={datetime.fromtimestamp(from_ts, timezone.utc).isoformat() if from_ts else None}, to_ts={datetime.fromtimestamp(to_ts, timezone.utc).isoformat() if to_ts else None}")

        # Persist minValueFilter to user settings in Redis
        if email:
            redis_conn = await get_redis_connection()
            settings_key = f"settings:{email}:{symbol}"
            settings_json = await redis_conn.get(settings_key)
            if settings_json:
                settings_data = json.loads(settings_json)
            else:
                settings_data = {}

            # Update minValueFilter
            settings_data['minValueFilter'] = value_filter

            # Save back to Redis
            updated_settings_json = json.dumps(settings_data)
            await redis_conn.set(settings_key, updated_settings_json)
            logger.info(f"Persisted minValueFilter {value_filter} to settings for {email}:{symbol}")

        if not from_ts or not to_ts:
            return {
                "type": "error",
                "message": "No time range specified for trade_history request",
                "request_id": request_id
            }

        # Clamp to_ts to current time to prevent fetching future data
        current_ts = int(time.time())
        if to_ts is not None and to_ts > current_ts:
            logger.info(f"Clamping trade_history to_ts from {to_ts} to current time {current_ts} to prevent fetching future data")
            to_ts = current_ts

        # Fetch trades (without filtering)
        trades_result = await fetch_recent_trade_history(symbol, from_ts, to_ts)
        trades = trades_result.get("trades", [])
        logger.info(f"TRADE_HISTORY: Fetched {len(trades)} trades for symbol {symbol}, from_ts={from_ts}, to_ts={to_ts}, value_filter={value_filter}")

        if trades and len(trades) > 0:
            logger.info(f"HISTORY: First trade sample: {json.dumps(trades[0])}")
        if trades and len(trades) > 0 and value_filter > 0:
            # Calculate max trade value for filtering
            trade_values = [trade['price'] * trade['amount'] for trade in trades if 'price' in trade and 'amount' in trade]
            if trade_values:
                max_trade_value = max(trade_values)
                min_value = value_filter * max_trade_value
                trades = [trade for trade in trades if (trade.get('price', 0) * trade.get('amount', 0)) >= min_value]
                logger.info(f"HISTORY: Filtered trades: {len(trades)} remain after filtering with {value_filter*100}% min value")

        # Return trade_history_success message
        return {
            "type": "trade_history_success",
            "symbol": symbol,
            "email": email,
            "data": {
                "trades": (trades or [])[:10000]
            },
            "timestamp": int(time.time()),
            "request_id": request_id
        }

    except Exception as e:
        logger.error(f"Error in handle_trade_history_message: {e}", exc_info=True)
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_shape_message(data: dict, websocket: WebSocket, request_id: str, action: str = None) -> dict:
    """Handle shape save/update/delete message from client"""
    session = websocket.scope.get('session', {})

    try:
        # Get email from WebSocket session
        email = session.get('email')
        if not email:
            return {
                "type": "error",
                "message": "Not authenticated",
                "request_id": request_id
            }

        symbol = data.get('symbol', 'BTCUSDT')

        logger.info(f"Processing shape message for {symbol}:{email}")

        # Create a fake request object for compatibility with existing functions
        from fastapi import Request as FastAPIRequest
        fake_request = FastAPIRequest(scope={"type": "http", "session": {"email": email}})

        # Check the action type - use parameter if provided, otherwise get from data
        if not action:
            action = data.get('action')
        if not action:
            return {
                "type": "error",
                "message": "Action is required for shape message",
                "request_id": request_id
            }

        if action == 'delete':
            # Handle delete operation
            drawing_id = data.get('drawing_id')
            if not drawing_id:
                return {
                    "type": "error",
                    "message": "Drawing ID is required for delete operation",
                    "request_id": request_id
                }

            logger.info(f"Deleting drawing {drawing_id} for {symbol}:{email}")
            success = await delete_drawing(symbol, drawing_id, fake_request, email)

            # Delete is considered successful even if drawing doesn't exist (idempotent operation)
            return {
                "type": "shape_success",
                "symbol": symbol,
                "email": email,
                "data": {"success": True, "id": drawing_id},
                "timestamp": int(time.time()),
                "request_id": request_id
            }

        elif action == 'save' or action == 'update':
            # Handle save/update operations
            resolution = data.get('resolution', '1h')

            # Prepare drawing data from the shape message
            drawing_data = {
                'symbol': symbol,
                'type': data.get('type'),  # Include type from client data
                'start_time': data.get('start_time'),
                'end_time': data.get('end_time'),
                'start_price': data.get('start_price'),
                'end_price': data.get('end_price'),
                'subplot_name': data.get('subplot_name', symbol),
                'resolution': resolution,
                'properties': data.get('properties')
            }

            # Check if this is an update (has drawing_id) or new save
            drawing_id = data.get('drawing_id')
            if drawing_id:
                # Update existing drawing
                logger.info(f"Updating drawing {drawing_id} for {symbol}:{email}")
                # Remove drawing_id from drawing_data for update
                update_data = {k: v for k, v in drawing_data.items() if k != 'drawing_id'}
                success = await update_drawing(symbol, drawing_id, DrawingData(**update_data), fake_request, email)
                result = {"success": success, "id": drawing_id}
            else:
                # Save new drawing
                logger.info(f"Saving new drawing for {symbol}:{email}")
                drawing_id = await save_drawing(drawing_data, fake_request)
                result = {"success": True, "id": drawing_id}

            if result and result.get('success'):
                # Send volume profile request for rectangles
                if drawing_data.get('type') == 'rect' and drawing_id:
                    try:
                        # Get rectangle data to calculate volume profile
                        drawings = await get_drawings(symbol, None, None, email)
                        rect_drawing = next((d for d in drawings if d.get('id') == drawing_id), None)

                        if rect_drawing:
                            start_time = rect_drawing.get("start_time")
                            end_time = rect_drawing.get("end_time")
                            start_price = rect_drawing.get("start_price")
                            end_price = rect_drawing.get("end_price")

                            if all([start_time, end_time, start_price is not None, end_price is not None]):
                                # Calculate volume profile directly and send success message
                                try:
                                    # Convert timestamps from milliseconds to seconds if needed
                                    if start_time > 1e12:  # > 1 trillion, likely milliseconds
                                        start_time = int(start_time / 1000)
                                    if end_time > 1e12:  # > 1 trillion, likely milliseconds
                                        end_time = int(end_time / 1000)

                                    # Fetch klines for rectangle time range
                                    rect_klines = await get_cached_klines(symbol, resolution, start_time, end_time)

                                    if rect_klines:
                                        # Filter klines within price range
                                        price_min = min(float(start_price), float(end_price))
                                        price_max = max(float(start_price), float(end_price))

                                        filtered_klines = [
                                            k for k in rect_klines
                                            if k.get('high', 0) >= price_min and k.get('low', 0) <= price_max
                                        ]

                                        if filtered_klines:
                                            # Calculate volume profile
                                            volume_profile_data = calculate_volume_profile(filtered_klines, drawing_id)

                                            # Send volume_profile_success message
                                            volume_profile_success_message = {
                                                "type": "volume_profile_success",
                                                "rectangle_id": drawing_id,
                                                "symbol": symbol,
                                                "rectangle": {
                                                    "start_time": start_time,
                                                    "end_time": end_time,
                                                    "start_price": start_price,
                                                    "end_price": end_price
                                                },
                                                "data": volume_profile_data,
                                                "timestamp": int(time.time()),
                                                "request_id": f"vp_{request_id}"
                                            }

                                            await websocket.send_json(volume_profile_success_message)
                                            logger.info(f"📊 Sent volume profile success for rectangle {drawing_id}")
                                        else:
                                            logger.warning(f"No klines within price range for rectangle {drawing_id}")
                                    else:
                                        logger.warning(f"No klines available for rectangle {drawing_id} time range")

                                except Exception as calc_error:
                                    logger.error(f"Error calculating volume profile for rectangle {drawing_id}: {calc_error}")

                    except Exception as vp_error:
                        logger.error(f"Error sending volume profile request for rectangle {drawing_id}: {vp_error}")

                return {
                    "type": "shape_success",
                    "symbol": symbol,
                    "email": email,
                    "data": result,
                    "timestamp": int(time.time()),
                    "request_id": request_id
                }
            else:
                return {
                    "type": "error",
                    "message": "Failed to save/update shape",
                    "request_id": request_id
                }

        elif action == 'get_properties':
            # Handle get properties operation
            drawing_id = data.get('drawing_id')
            if not drawing_id:
                return {
                    "type": "error",
                    "message": "Drawing ID is required for get_properties operation",
                    "request_id": request_id
                }

            logger.info(f"Getting properties for drawing {drawing_id} for {symbol}:{email}")

            # Get drawing data to retrieve properties
            drawings = await get_drawings(symbol, None, None, email)
            drawing = next((d for d in drawings if d.get('id') == drawing_id), None)
            if not drawing:
                return {
                    "type": "error",
                    "message": f"Drawing {drawing_id} not found",
                    "request_id": request_id
                }

            properties = drawing.get('properties', {})

            return {
                "type": "shape_properties_response",
                "symbol": symbol,
                "email": email,
                "data": {
                    "id": drawing_id,
                    "type": drawing.get('type'),  # Include 
                    "properties": properties
                },
                "timestamp": int(time.time()),
                "request_id": request_id
            }

        elif action == 'save_properties':
            # Handle save properties operation
            drawing_id = data.get('drawing_id')
            properties = data.get('properties', {})

            if not drawing_id:
                logger.error("Drawing ID is required for save_properties operation")
                return {
                    "type": "error",
                    "message": "Drawing ID is required for save_properties operation",
                    "request_id": request_id
                }

            logger.info(f"📝 SAVE_PROPERTIES: Attempting to save properties for drawing {drawing_id} for {symbol}:{email}")
            logger.info(f"📝 SAVE_PROPERTIES: Properties to save: {properties}")

            # Update drawing properties using existing function
            success = await update_drawing_properties(symbol, drawing_id, properties, email)
            logger.info(f"📝 SAVE_PROPERTIES: Update result - success: {success}")

            if success:
                logger.info(f"✅ SAVE_PROPERTIES: Successfully saved properties for drawing {drawing_id}")
                return {
                    "type": "shape_properties_success",
                    "symbol": symbol,
                    "email": email,
                    "id": drawing_id,
                    "data": {"success": True, "id": drawing_id},
                    "timestamp": int(time.time()),
                    "request_id": request_id
                }
            else:
                logger.error(f"❌ SAVE_PROPERTIES: Failed to save properties for drawing {drawing_id}")
                return {
                    "type": "error",
                    "message": "Failed to save shape properties",
                    "request_id": request_id
                }

        else:
            return {
                "type": "error",
                "message": f"Unknown shape action: {action}",
                "request_id": request_id
            }

    except Exception as e:
        logger.error(f"Error in handle_shape_message: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_get_volume_profile_direct(message: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle direct get_volume_profile message from client"""
    session = websocket.scope.get('session', {})

    try:
        # Handle volume profile calculation request from client
        rectangle_id = message.get("rectangle_id")
        rectangle_symbol = message.get("symbol", "BTCUSDT")
        resolution = message.get("resolution", "1h")
    
        logger.info(f"Processing get_volume_profile request for rectangle {rectangle_id}, symbol {rectangle_symbol}")

        # Get rectangle data from database
        email = session.get('email')
        if not email:
            return {
                "type": "error",
                "message": "Not authenticated",
                "request_id": request_id
            }

        # Get drawing data for the rectangle directly from Redis by ID
        redis_conn = await get_redis_connection()
        drawing_key = f"drawing:{email}:{rectangle_symbol}:{rectangle_id}"
        drawing_data_str = await redis_conn.get(drawing_key)

        if not drawing_data_str:
            return {
                "type": "error",
                "message": f"No rectangle data found for id {rectangle_id}",
                "request_id": request_id
            }

        rect_drawing = json.loads(drawing_data_str)
        start_time = rect_drawing.get("start_time")
        end_time = rect_drawing.get("end_time")
        start_price = rect_drawing.get("start_price")
        end_price = rect_drawing.get("end_price")

        if not all([start_time, end_time, start_price is not None, end_price is not None]):
            return {
                "type": "error",
                "message": f"Incomplete rectangle data for drawing {rectangle_id}",
                "request_id": request_id
            }

        price_min = min(float(start_price), float(end_price))
        price_max = max(float(start_price), float(end_price))

        # Fetch klines for this rectangle's time range
        rect_klines = await get_cached_klines(rectangle_symbol, resolution, start_time, end_time)
        logger.debug(f"Fetched {len(rect_klines)} klines for rectangle {rectangle_id} time range")

        if not rect_klines:
            return {
                "type": "error",
                "message": f"No klines available for rectangle {rectangle_id}",
                "request_id": request_id
            }

        # Filter klines that intersect with the rectangle's price range
        filtered_klines = [
            k for k in rect_klines
            if k.get('high', 0) >= price_min and k.get('low', 0) <= price_max
        ]
        logger.debug(f"Filtered to {len(filtered_klines)} klines within price range [{price_min}, {price_max}] for rectangle {rectangle_id}")

        if not filtered_klines:
            return {
                "type": "error",
                "message": f"No klines within price range for rectangle {rectangle_id}",
                "request_id": request_id
            }

        # Calculate volume profile for the filtered data
        volume_profile_data = calculate_volume_profile(filtered_klines, rectangle_id)
        logger.info(f"Calculated volume profile for rectangle {rectangle_id} with {len(volume_profile_data.get('volume_profile', []))} price levels")

        # Return volume profile data for this rectangle
        return {
            "type": "volume_profile_success",
            "symbol": rectangle_symbol,
            "rectangle_id": rectangle_id,
            "rectangle": {
                "start_time": start_time,
                "end_time": end_time,
                "start_price": start_price,
                "end_price": end_price
            },
            "data": volume_profile_data,
            "timestamp": int(time.time()),
            "request_id": request_id
        }

    except Exception as e:
        logger.error(f"Failed to process get_volume_profile request for rectangle {rectangle_id}: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }


async def handle_authenticate(data: dict, websocket: WebSocket, request_id: str) -> dict:
    """Handle authentication request"""
    session = websocket.scope.get('session', {})

    try:
        # Basic authentication check
        email = data.get('email')
        authenticated = session.get('authenticated', False)

        return {
            "type": "response",
            "action": "authenticate",
            "success": authenticated,
            "data": {
                "authenticated": authenticated,
                "email": session.get('email')
            },
            "request_id": request_id
        }
    except Exception as e:
        logger.error(f"Error in handle_authenticate: {e}")
        return {
            "type": "error",
            "message": str(e),
            "request_id": request_id
        }



# Audio transcription endpoint
@app.post("/transcribe_audio")
@limiter.limit("10/minute")
async def transcribe_audio_endpoint(
    request: Request,
    audio_file: UploadFile = File(...),
    symbol: str = Form("BTCUSDT"),
    resolution: str = Form("1h"),
    xAxisMin: str = Form(None),
    xAxisMax: str = Form(None),
    activeIndicatorIds: str = Form("[]"),
    use_local_ollama: bool = Form(False),
    use_gemini: bool = Form(False)
):
    """
    Transcribe audio file to text using Whisper.
    Accepts audio files and returns transcribed text.
    Requires user authentication.
    """
    # Check authentication
    '''
    if not request.session.get('email'):
        raise HTTPException(
            status_code=403,
            detail="Authentication required. Please log in with Google OAuth."
        )
    '''
    logger.info("transcribe_audio_endpoint request received")
    try:
        # Validate file type
        allowed_extensions = ['.wav', '.mp3', '.m4a', '.flac', '.ogg', '.webm']
        file_extension = os.path.splitext(audio_file.filename)[1].lower()

        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type. Allowed types: {', '.join(allowed_extensions)}"
            )

        # Read audio file content first
        audio_content = await audio_file.read()

        # Log file details for debugging
        logger.info(f"Audio file received: {audio_file.filename}, size: {len(audio_content)} bytes, type: {file_extension}")

        # Special handling for WebM files - convert to WAV for Whisper compatibility
        if file_extension == '.webm':
            try:
                from pydub import AudioSegment
                logger.info("Converting WebM to WAV for Whisper compatibility")

                # Save WebM content to temporary file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.webm') as webm_temp:
                    webm_temp.write(audio_content)
                    webm_path = webm_temp.name

                # Convert WebM to WAV using pydub
                audio = AudioSegment.from_file(webm_path, format="webm")
                wav_buffer = io.BytesIO()
                audio.export(wav_buffer, format="wav")
                audio_content = wav_buffer.getvalue()
                file_extension = '.wav'

                # Clean up WebM temp file
                os.unlink(webm_path)
                logger.info("WebM to WAV conversion completed")

            except ImportError:
                logger.error("pydub not available for WebM conversion - install with: pip install pydub")
                raise HTTPException(
                    status_code=500,
                    detail="WebM conversion not available. Please use WAV, MP3, or other supported formats."
                )
            except Exception as e:
                logger.error(f"WebM conversion failed: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Audio conversion failed: {str(e)}"
                )

        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
            temp_file.write(audio_content)
            temp_file_path = temp_file.name

        try:
            # Check if Whisper model is preloaded
            model = getattr(app.state, 'whisper_model', None)
            if model is None:
                logger.error("Whisper model not available - failed to load at startup")
                raise HTTPException(
                    status_code=503,
                    detail="Audio transcription service is temporarily unavailable. Please try again later."
                )

            logger.info("Transcribing audio file using preloaded model")
            result = model.transcribe(temp_file_path)

            # Extract transcribed text
            transcribed_text = result["text"].strip()

            logger.info(f"Audio transcription completed. Text length: {len(transcribed_text)}")

            # Process transcribed text with local LLM via LM Studio
            from ai_features import AIRequest

            # Parse activeIndicatorIds from JSON string to list
            try:
                parsed_indicators = json.loads(activeIndicatorIds) if activeIndicatorIds else []
            except json.JSONDecodeError:
                parsed_indicators = []

            ai_request = AIRequest(
                symbol=symbol,  # Use symbol from client
                resolution=resolution,   # Use resolution from client
                xAxisMin=str(xAxisMin) if xAxisMin else str(int(datetime.now().timestamp()) - 3600),  # Convert to string
                xAxisMax=str(xAxisMax) if xAxisMax else str(int(datetime.now().timestamp())),  # Convert to string
                activeIndicatorIds=parsed_indicators,  # Use parsed indicators from client
                question=transcribed_text,  # Put transcribed text in question field
                use_local_ollama=use_local_ollama,
                use_gemini=use_gemini
            )
            llm_analysis = await process_audio_with_llm(ai_request)
            logger.info(f"LLM analysis completed. Analysis length: {len(llm_analysis)}")

            return {
                "status": "success",
                "transcribed_text": transcribed_text,
                "llm_analysis": llm_analysis,
                "language": result.get("language", "unknown"),
                "confidence": result.get("confidence", 0.0)
            }

        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_file_path)
            except Exception as e:
                logger.warning(f"Failed to clean up temporary file: {e}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error during audio transcription: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Audio transcription failed: {str(e)}"
        )



# Legacy HTTP endpoints (for backward compatibility during transition)



@app.get("/")
@limiter.limit("30/minute")
async def chart_page(request: Request):
    """Legacy HTTP endpoint for main chart page - redirects to use WebSocket"""
    client_host = request.client.host if request.client else "Unknown"

    logger.info(f"Legacy chart page request from {client_host}. Current session state: authenticated={request.session.get('authenticated')}, email={request.session.get('email')}")

    # Use centralized authentication function
    authenticated, user_email = authenticate_user_from_request(request)

    # Update session if authenticated
    if authenticated and user_email:
        request.session["authenticated"] = True
        request.session["email"] = user_email

    # Check authentication
    is_local_test = client_host == IP_ADDRESS

    # client_id: 655872926127-9fihq5a2rdsiakqltvmt6urj7saanhhd.apps.googleusercontent.com
    if request.session.get('email') is None:
        # Certificate authentication is already handled above
        # If no certificate and no session, use local test authentication
        if is_local_test:
            request.session["authenticated"] = True
            request.session["email"] = "test@example.com"
            authenticated = True
            logger.info("Using local test authentication")
        else:
            # No certificate and not local test - deny access
            logger.warning("No valid authentication method available")
            # Could return an error response here, but for now allow local test
            request.session["authenticated"] = True
            request.session["email"] = "test@example.com"
            authenticated = True
            logger.info("Fallback to local test authentication (certificate auth failed)")
    else:
        # Check for last selected symbol and redirect if found
        try:
            from endpoints.utility_endpoints import get_last_selected_symbol
            last_symbol_response = await get_last_selected_symbol(request)
            if last_symbol_response.status_code == 200:
                import json
                response_data = json.loads(last_symbol_response.body.decode('utf-8'))
                if response_data.get('status') == 'success' and response_data.get('symbol'):
                    last_symbol = response_data['symbol']
                    logger.info(f"Redirecting user {request.session.get('email')} to last selected symbol: {last_symbol}")
                    return RedirectResponse(f"/{last_symbol}", status_code=302)
        except Exception as e:
            logger.error(f"Error checking last selected symbol for redirect: {e}")

    response = templates.TemplateResponse("index.html", {
        "request": request,
        "authenticated": authenticated,
        "supported_resolutions": SUPPORTED_RESOLUTIONS,
        "supported_ranges": SUPPORTED_RANGES,
        "supported_symbols": SUPPORTED_SYMBOLS
    })
    return response






@app.get("/logout")
async def logout(request: Request):
    """Clear the user session and redirect to home."""
    logger.info(f"Logout request from {request.client.host if request.client else 'Unknown'}. Clearing session.")
    request.session.clear()
    logger.info("Session cleared.")
    return RedirectResponse("/", status_code=302)


@app.get("/health/background-tasks")
async def background_tasks_health():
    """Health check for background tasks - same as original"""
    fetch_task = getattr(app.state, 'fetch_klines_task', None)
    trade_aggregator_task = getattr(app.state, 'trade_aggregator_task', None)
    trade_gap_filler_task = getattr(app.state, 'trade_gap_filler_task', None)
    email_task = getattr(app.state, 'email_alert_task', None)
    price_feed_task = getattr(app.state, 'price_feed_task', None)
    youtube_monitor_task = getattr(app.state, 'youtube_monitor_task', None)

    health_status = {
        "timestamp": datetime.now().isoformat(),
        "background_tasks": {
            "fetch_klines_task": {
                "exists": fetch_task is not None,
                "running": fetch_task is not None and not fetch_task.done(),
                "done": fetch_task.done() if fetch_task else None,
                "cancelled": fetch_task.cancelled() if fetch_task else None
            },
            "trade_aggregator_task": {
                "exists": trade_aggregator_task is not None,
                "running": trade_aggregator_task is not None and not trade_aggregator_task.done(),
                "done": trade_aggregator_task.done() if trade_aggregator_task else None,
                "cancelled": trade_aggregator_task.cancelled() if trade_aggregator_task else None
            },
            "trade_gap_filler_task": {
                "exists": trade_gap_filler_task is not None,
                "running": trade_gap_filler_task is not None and not trade_gap_filler_task.done(),
                "done": trade_gap_filler_task.done() if trade_gap_filler_task else None,
                "cancelled": trade_gap_filler_task.cancelled() if trade_gap_filler_task else None,
                "exception": str(trade_gap_filler_task.exception()) if trade_gap_filler_task and trade_gap_filler_task.exception() else None
            },
            "price_feed_task": {
                "exists": price_feed_task is not None,
                "running": price_feed_task is not None and not price_feed_task.done(),
                "done": price_feed_task.done() if price_feed_task else None,
                "cancelled": price_feed_task.cancelled() if price_feed_task else None,
                "exception": str(price_feed_task.exception()) if price_feed_task and price_feed_task.exception() else None
            },
            "email_alert_task": {
                "exists": email_task is not None,
                "running": email_task is not None and not email_task.done(),
                "done": email_task.done() if email_task else None,
                "cancelled": email_task.cancelled() if email_task else None,
                "exception": str(email_task.exception()) if email_task and email_task.exception() else None
            },
            "youtube_monitor_task": {
                "exists": youtube_monitor_task is not None,
                "running": youtube_monitor_task is not None and not youtube_monitor_task.done(),
                "done": youtube_monitor_task.done() if youtube_monitor_task else None,
                "cancelled": youtube_monitor_task.cancelled() if youtube_monitor_task else None,
                "exception": str(youtube_monitor_task.exception()) if youtube_monitor_task and youtube_monitor_task.exception() else None
            }
        }
    }

    logger.info(f"📊 BACKGROUND TASK HEALTH CHECK: {health_status}")
    return health_status


@app.post("/api/encrypt-secret")
async def encrypt_secret_endpoint(request: Request):
    """API endpoint to encrypt a secret for sharing"""
    try:
        data = await request.json()
        secret = data.get('secret')
        if not secret:
            raise HTTPException(status_code=400, detail="Secret is required")

        encrypted = encrypt_secret(secret)
        return {"encrypted_secret": encrypted}
    except Exception as e:
        logger.error(f"Error encrypting secret: {e}")
        raise HTTPException(status_code=500, detail="Failed to encrypt secret")


@app.post("/api/send-confirmation-email")
async def send_confirmation_email_endpoint(request: Request):
    """Send confirmation email to user"""
    try:
        data = await request.json()
        email = data.get('email')
        if not email:
            raise HTTPException(status_code=400, detail="Email is required")

        # Generate confirmation token
        confirmation_token = generate_confirmation_token()

        # Store token in Redis with expiration (24 hours)
        redis_conn = await get_redis_connection()
        token_key = f"email_confirmation:{confirmation_token}"
        token_data = json.dumps({
            "email": email,
            "created_at": datetime.now().isoformat(),
            "expires_at": (datetime.now() + timedelta(hours=24)).isoformat()
        })
        await redis_conn.setex(token_key, 86400, token_data)  # 24 hours

        # Send confirmation email
        success = await send_confirmation_email(email, confirmation_token)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to send confirmation email")

        return {"message": "Confirmation email sent successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending confirmation email: {e}")
        raise HTTPException(status_code=500, detail="Failed to send confirmation email")


@app.get("/confirm-email")
async def confirm_email(request: Request, token: str):
    """Handle email confirmation"""
    try:
        redis_conn = await get_redis_connection()
        token_key = f"email_confirmation:{token}"

        # Get token data
        token_data_str = await redis_conn.get(token_key)
        if not token_data_str:
            raise HTTPException(status_code=400, detail="Invalid or expired confirmation token")

        token_data = json.loads(token_data_str)
        email = token_data.get('email')
        expires_at = datetime.fromisoformat(token_data.get('expires_at'))

        # Check if token has expired
        if datetime.now() > expires_at:
            await redis_conn.delete(token_key)
            raise HTTPException(status_code=400, detail="Confirmation token has expired")

        # Set email cookie
        response = RedirectResponse("/", status_code=302)
        response.set_cookie(
            key="user_email",
            value=email,
            max_age=365*24*60*60,  # 1 year in seconds
            httponly=False,  # Allow JavaScript access for debugging
            secure=False,  # Set to True in production with HTTPS
            samesite="lax",
            path="/",  # Available on all paths
            domain="crypto.zhivko.eu"  # Explicit domain
        )
        logger.info(f"Set email cookie for {email}: {response.headers.get('set-cookie', 'NOT SET')}")
        logger.info(f"Response headers: {dict(response.headers)}")

        # Clean up token
        await redis_conn.delete(token_key)

        logger.info(f"Email confirmed for {email}")
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error confirming email: {e}")
        raise HTTPException(status_code=500, detail="Failed to confirm email")

# @app.get("/OAuthCallback") - DISABLED
# async def oauth_callback(request: Request, code: str):
#     """
#     Handles the OAuth 2.0 callback from Google.
#     This is where Google redirects the user after they authenticate.
#     """
#     logger.info(f"OAuthCallback: Received callback from Google with code: {code[:50]}...") # Truncate code for logging
#
#     # Determine redirect URI based on client host (same logic as in chart_page)
#     client_host = request.client.host if request.client else "Unknown"
#     if client_host == IP_ADDRESS:
#         redirect_uri = f'http://{IP_ADDRESS}:5000/OAuthCallback'
#     else:
#         redirect_uri = 'https://crypto.zhivko.eu/OAuthCallback'
#
#     try:
#         # Run the synchronous OAuth operations in a thread to avoid blocking the ASGI event loop
#         user_email = await asyncio.to_thread(process_google_oauth, code, redirect_uri)
#
#         logger.info(f"OAuthCallback: Before setting session - authenticated={request.session.get('authenticated')}, email={request.session.get('email')}")
#         request.session["authenticated"] = True
#         request.session["email"] = user_email  # Store the user's email in the session
#         logger.info(f"Login user in by google account. Session variable set: session[\"authenticated\"] = True, session[\"email\"] = {user_email}")
#         logger.info(f"OAuthCallback: After setting session - authenticated={request.session.get('authenticated')}, email={request.session.get('email')}")
#
#         # 2. [Placeholder] Create or update user in your database
#         # You would typically check if the user_id exists, if not, create a new user.
#         # For now, just log the user information.
#         logger.info(f"OAuthCallback: [Placeholder] Creating or updating user in database for email: {user_email}")
#
#         # 3. [Placeholder] Establish a session for the user
#         # This is a placeholder.  In a real app, you'd set a cookie or use some other session management.
#         logger.info(f"OAuthCallback: [Placeholder] Establishing session for email: {user_email}")
#
#         return RedirectResponse("/", status_code=302) # Redirect to the main chart
#
#     except Exception as e:
#         logger.error(f"OAuthCallback: Error processing Google login: {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail=f"Failed to process Google login: {e}")


# def process_google_oauth(code: str, redirect_uri: str) -> str: - DISABLED
#     """
#     Synchronous function to handle Google OAuth flow.
#     This runs in a thread to avoid blocking the ASGI event loop.
#     """
#     GOOGLE_CLIENT_ID = creds.GOOGLE_CLIENT_ID
#     GOOGLE_CLIENT_SECRET = creds.GOOGLE_CLIENT_SECRET
#
#     client_secrets_file = 'c:/git/VidWebServer/client_secret_655872926127-9fihq5a2rdsiakqltvmt6urj7saanhhd.apps.googleusercontent.com.json'
#
#     # Load client secrets from file
#     with open(client_secrets_file, 'r') as f:
#         client_config = json.load(f)
#
#     web_config = client_config.get('web', {})
#     scopes = client_config.get('web', {}).get('scopes')
#     if not scopes:
#         logger.warning(f"No scopes found in client_secret.json at {client_secrets_file}. Check file structure.")
#         scopes = ['https://www.googleapis.com/auth/userinfo.email','openid','https://www.googleapis.com/auth/userinfo.profile'] # Use default scopes if not found in file
#     client_id = web_config.get('client_id')
#     client_secret = web_config.get('client_secret')
#     if not client_id or not client_secret:
#         logger.error(f"Client ID or secret not found in {client_secrets_file}")
#         raise ValueError("Client ID or secret not found in client_secret.json")
#
#     flow = Flow.from_client_secrets_file(
#         client_secrets_file=client_secrets_file,  # Replace with the path to your client_secret.json file
#         scopes=scopes,
#         redirect_uri=redirect_uri)
#
#     # 2. Exchange the authorization code for credentials
#     flow.fetch_token(code=code)
#     credentials = flow.credentials
#
#     # 3. Verify the ID token
#     id_token_jwt = credentials.id_token
#
#     if id_token_jwt is None:
#         raise ValueError("ID token is missing in the credentials.")
#
#     try:
#         request = google_requests.Request()
#         id_info = id_token.verify_oauth2_token(id_token_jwt, request, GOOGLE_CLIENT_ID)
#     except ValueError as e:
#         logger.error(f"OAuthCallback: Invalid ID token: {e}")
#         raise ValueError(f"Invalid ID token: {e}")
#
#     # 4. Check the token's claims
#     if id_info['iss'] not in ['accounts.google.com', 'https://accounts.google.com']:
#         raise ValueError("Wrong issuer.")
#
#     user_id = id_info['sub'] # The unique user ID
#     user_email = id_info['email']
#     logger.info(f"OAuthCallback: Successfully verified Google ID token for user {user_id} (email: {user_email})")
#
#     return user_email
@app.get("/{symbol}")
@limiter.limit("1000/minute")
async def symbol_chart_page(symbol: str, request: Request):
    """Legacy HTTP endpoint for symbol-specific chart page"""
    client_host = request.client.host if request.client else "Unknown"

    logger.info(f"Legacy symbol chart page request for {symbol} from {client_host}. Current session state: authenticated={request.session.get('authenticated')}, email={request.session.get('email')}")

    # Skip if API route
    if symbol in ["static", "ws", "health", "logout", "OAuthCallback"]:
        raise HTTPException(status_code=404, detail="Not found")

    # Use centralized authentication function
    authenticated, user_email = authenticate_user_from_request(request)

    # Update session if authenticated
    if authenticated and user_email:
        request.session["authenticated"] = True
        request.session["email"] = user_email

    # Authentication logic (same as original)
    is_local_test = client_host == IP_ADDRESS

    if request.session.get('email') is None:
        # Certificate authentication is already handled above
        # If no certificate and no session, use local test authentication
        if is_local_test:
            request.session["authenticated"] = True
            request.session["email"] = "test@example.com"
            authenticated = True
            logger.info("Using local test authentication")
        else:
            # No certificate and not local test - deny access
            logger.warning("No valid authentication method available")
            # Could return an error response here, but for now allow local test
            request.session["authenticated"] = True
            request.session["email"] = "test@example.com"
            authenticated = True
            logger.info("Fallback to local test authentication (certificate auth failed)")
    else:
        authenticated = True

        if symbol in SUPPORTED_SYMBOLS:
            # Persist last selected symbol per user in Redis
            try:
                email = request.session.get("email")
                if email:
                    redis_conn = await get_redis_connection()
                    last_selected_symbol_key = f"user:{email}:{REDIS_LAST_SELECTED_SYMBOL_KEY}"
                    await redis_conn.set(last_selected_symbol_key, symbol.upper())
                    logger.debug(f"Persisted last selected symbol '{symbol.upper()}' for user {email}")
            except Exception as e:
                logger.error(f"Error persisting last selected symbol for {symbol}: {e}")

    # Process chart view parameters from URL query string
    start_ts = request.query_params.get('start_ts')
    end_ts = request.query_params.get('end_ts')
    y_min = request.query_params.get('y_min')
    y_max = request.query_params.get('y_max')

    # Convert ISO timestamp strings to Unix timestamps if needed
    if start_ts:
        try:
            # Try to parse as ISO timestamp string first
            if 'T' in start_ts and 'Z' in start_ts:
                start_ts = int(datetime.fromisoformat(start_ts.replace('Z', '+00:00')).timestamp())
            else:
                # Try to convert as float (Unix timestamp)
                start_ts = float(start_ts)
        except (ValueError, TypeError):
            logger.warning(f"Invalid start_ts format: {start_ts}, ignoring")
            start_ts = None

    if end_ts:
        try:
            # Try to parse as ISO timestamp string first
            if 'T' in end_ts and 'Z' in end_ts:
                end_ts = int(datetime.fromisoformat(end_ts.replace('Z', '+00:00')).timestamp())
            else:
                # Try to convert as float (Unix timestamp)
                end_ts = float(end_ts)
        except (ValueError, TypeError):
            logger.warning(f"Invalid end_ts format: {end_ts}, ignoring")
            end_ts = None

    if y_min:
        try:
            y_min = float(y_min)
        except (ValueError, TypeError):
            logger.warning(f"Invalid y_min format: {y_min}, ignoring")
            y_min = None

    if y_max:
        try:
            y_max = float(y_max)
        except (ValueError, TypeError):
            logger.warning(f"Invalid y_max format: {y_max}, ignoring")
            y_max = None

    # Try to fetch user settings from Redis if any parameters are None
    if any(param is None for param in [start_ts, end_ts, y_min, y_max]):
        email = request.session.get('email')
        if email:
            try:
                redis_conn = await get_redis_connection()
                settings_key = f"settings:{email}:{symbol.upper()}"
                settings_json = await redis_conn.get(settings_key)
                if settings_json:
                    settings_data = json.loads(settings_json)
                    # Use settings values if parameters are None
                    if start_ts is None and 'xAxisMin' in settings_data:
                        start_ts = settings_data['xAxisMin']
                        if isinstance(start_ts, str):
                            start_ts = float(start_ts)
                    if end_ts is None and 'xAxisMax' in settings_data:
                        end_ts = settings_data['xAxisMax']
                        if isinstance(end_ts, str):
                            end_ts = float(end_ts)
                    if y_min is None and 'yAxisMin' in settings_data:
                        y_min = settings_data['yAxisMin']
                        if isinstance(y_min, str):
                            y_min = float(y_min)
                    if y_max is None and 'yAxisMax' in settings_data:
                        y_max = settings_data['yAxisMax']
                        if isinstance(y_max, str):
                            y_max = float(y_max)
                    logger.info(f"Fetched chart parameters from Redis settings for {email}:{symbol}: start_ts={start_ts}, end_ts={end_ts}, y_min={y_min}, y_max={y_max}")
            except Exception as e:
                logger.error(f"Error fetching user settings from Redis: {e}")

    # Log the processed parameters
    if any([start_ts, end_ts, y_min, y_max]):
        logger.info(f"Chart view parameters from URL: start_ts={start_ts}, end_ts={end_ts}, y_min={y_min}, y_max={y_max}")

        # If user is authenticated, save these parameters to their settings in Redis
        email = request.session.get('email')
        if email and any([start_ts, end_ts, y_min, y_max]):
            try:
                redis_conn = await get_redis_connection()
                settings_key = f"settings:{email}:{symbol.upper()}"

                # Get existing settings
                settings_json = await redis_conn.get(settings_key)
                if settings_json:
                    settings_data = json.loads(settings_json)
                else:
                    settings_data = {}

                # Update with URL parameters if provided
                if start_ts is not None:
                    settings_data['xAxisMin'] = start_ts
                if end_ts is not None:
                    settings_data['xAxisMax'] = end_ts
                if y_min is not None:
                    settings_data['yAxisMin'] = y_min
                if y_max is not None:
                    settings_data['yAxisMax'] = y_max

                # Save updated settings
                updated_settings_json = json.dumps(settings_data)
                await redis_conn.set(settings_key, updated_settings_json)
                logger.info(f"Updated chart view settings for {email}:{symbol.upper()} from URL parameters")

            except Exception as e:
                logger.error(f"Error saving chart view settings from URL parameters: {e}")

    # Render template
    response = templates.TemplateResponse("index.html", {
        "request": request,
        "authenticated": authenticated,
        "symbol": symbol.upper(),
        "supported_resolutions": SUPPORTED_RESOLUTIONS,
        "supported_ranges": SUPPORTED_RANGES,
        "supported_symbols": SUPPORTED_SYMBOLS,
        "url_start_ts": start_ts,
        "url_end_ts": end_ts,
        "url_y_min": y_min,
        "url_y_max": y_max
    })
    return response


if __name__ == "__main__":
    # Configure Gemini API key globally
    try:
        import google.generativeai as genai
        genai.configure(api_key=creds.GEMINI_API_KEY)
    except ImportError:
        logger.warning("Google Generative AI not available. AI features will be limited.")

    logger.info(f'Local address: {IP_ADDRESS}')

    # Set debug mode
    is_debug_mode = True
    app.extra["debug_mode"] = is_debug_mode

    uvicorn.run(
        "AppTradingView2:app",
        host="0.0.0.0",
        port=5000,
        reload=True,
        reload_excludes="*.log",
        http="h11",
        timeout_keep_alive=10,
        log_level="info",
        timeout_graceful_shutdown=1
    )