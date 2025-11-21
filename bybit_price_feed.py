# Bybit Price Feed Background Task
# Connects to Bybit WebSocket and records last prices for all known symbols to Redis

import asyncio
import json
import os
from typing import Dict, Any
from pybit.unified_trading import WebSocket as BybitWS
from config import SUPPORTED_SYMBOLS
from redis_utils import get_redis_connection
import logging
logger = logging.getLogger('bybit_price_feed')

async def bybit_price_feed_task():
    """
    Background task that connects to Bybit WebSocket and stores last prices
    for all supported symbols in Redis with keys "live:{symbol}".
    """
    # Check if Bybit price feed is disabled via environment variable
    if os.getenv("DISABLE_BYBIT_PRICE_FEED", "false").lower() == "true":
        logger.info("Bybit price feed disabled via DISABLE_BYBIT_PRICE_FEED environment variable")
        return

    logger.info("Starting Bybit price feed background task")

    # Initialize Redis connection
    redis_conn = None
    try:
        redis_conn = await get_redis_connection()
    except Exception as e:
        logger.error(f"Failed to connect to Redis in price feed task: {e}")
        return

    # Get the current event loop for thread-safe callback scheduling
    loop = asyncio.get_running_loop()

    # Create Bybit WebSocket client
    bybit_ws_client = BybitWS(
        testnet=False,
        channel_type="linear"
    )

    def price_update_handler(message: Dict[str, Any]):
        """
        Callback function to handle price updates from Bybit WebSocket.
        Stores the last price in Redis with key "live:{symbol}".
        This callback runs in a thread managed by pybit's WebSocket client.
        """
        try:
            if "topic" in message and "data" in message:
                topic_str = message["topic"]
                ticker_data = message["data"]

                # Extract symbol from topic (format: "tickers.{symbol}")
                if topic_str.startswith("tickers."):
                    symbol = topic_str.split(".", 1)[1]

                    if "lastPrice" in ticker_data:
                        last_price = float(ticker_data["lastPrice"])
                        redis_key = f"live:{symbol}"

                        # Check if the event loop is still running before scheduling
                        if loop.is_running():
                            try:
                                # Schedule the Redis storage task on the event loop using run_coroutine_threadsafe
                                asyncio.run_coroutine_threadsafe(
                                    store_price_in_redis(redis_conn, redis_key, last_price, symbol),
                                    loop
                                )
                            except RuntimeError as e:
                                if "Event loop is closed" in str(e):
                                    logger.debug(f"Event loop closed, skipping Redis update for {symbol}")
                                else:
                                    logger.error(f"RuntimeError scheduling Redis task for {symbol}: {e}")
                        else:
                            logger.debug(f"Event loop not running, skipping Redis update for {symbol}")

        except Exception as e:
            logger.error(f"Error processing price update message: {e}")

    async def store_price_in_redis(redis_conn, key: str, price: float, symbol: str):
        """Store the price in Redis."""
        try:
            await redis_conn.set(key, str(price))
            logger.debug(f"Updated Redis key {key} with price {price} for {symbol}")
        except Exception as e:
            logger.error(f"Failed to store price for {symbol} in Redis: {e}")
            # Try to reconnect and retry once
            try:
                logger.info(f"Attempting to reconnect to Redis for {symbol}")
                redis_conn = await get_redis_connection()
                await redis_conn.set(key, str(price))
                logger.info(f"Successfully reconnected and stored price for {symbol}")
            except Exception as retry_e:
                logger.error(f"Failed to reconnect and store price for {symbol}: {retry_e}")

    # Subscribe to ticker updates for all supported symbols
    # Use the same pattern as existing WebSocket handlers
    topic_template = 'tickers.{symbol}'
    symbols_list = [symbol for symbol in SUPPORTED_SYMBOLS if symbol != "BTCDOM"]

    logger.info(f"Subscribing to price feeds for {len(symbols_list)} symbols: {symbols_list}")

    # Add retry logic for WebSocket subscription
    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            bybit_ws_client.subscribe(
                topic=topic_template,
                callback=price_update_handler,
                symbol=symbols_list
            )
            logger.info(f"Successfully subscribed to Bybit WebSocket on attempt {attempt + 1}")
            break
        except Exception as e:
            logger.warning(f"Failed to subscribe to Bybit WebSocket on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying WebSocket subscription in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"Failed to subscribe to Bybit WebSocket after {max_retries} attempts")
                return

    # Keep the task running
    try:
        while True:
            await asyncio.sleep(60)  # Check every minute
            # logger.debug("Bybit price feed task is alive")
    except Exception as e:
        logger.error(f"Error in Bybit price feed task: {e}")
    finally:
        # Clean up WebSocket connection
        try:
            if hasattr(bybit_ws_client, 'exit') and callable(bybit_ws_client.exit):
                # logger.info("Attempting to close Bybit WebSocket connection...")
                bybit_ws_client.exit()
                # logger.info("Bybit WebSocket connection closed successfully")
            else:
                logger.warning("Bybit WebSocket client does not have exit method")
        except Exception as e:
            logger.error(f"Error closing Bybit WebSocket: {e}")

        # Clean up Redis connection
        try:
            if redis_conn:
                await redis_conn.close()
                logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

# Function to start the price feed task
async def start_bybit_price_feed():
    """Start the Bybit price feed background task."""
    task = asyncio.create_task(bybit_price_feed_task())
    return task

if __name__ == "__main__":
    # For testing the price feed independently
    asyncio.run(bybit_price_feed_task())
