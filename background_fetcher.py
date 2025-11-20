import logging
from typing import Callable, Optional, List, Tuple
import requests
import redis
import websocket
import json
import threading
import time


def validate_kline_continuity(klines: List[list], symbol: str, interval: str, logger: logging.Logger) -> bool:
    """
    Validate candle continuity in fetched klines before storing.

    Returns True if all continuity checks pass, False if violations found.
    Logs warnings for any continuity violations detected.
    """
    if len(klines) < 2:
        return True  # Can't check continuity with fewer than 2 klines

    violations = []

    for i in range(len(klines) - 1):
        current = klines[i]
        next_k = klines[i + 1]

        close_price = float(current[4])  # close
        open_price = float(next_k[1])   # open

        # Allow small tolerance to account for normal market microstructure
        CONTINUITY_TOLERANCE = 0.1  # 10 cents for crypto assets

        if abs(close_price - open_price) > CONTINUITY_TOLERANCE:
            violations.append({
                'index': i,
                'timestamp1': int(current[0]),
                'close': close_price,
                'timestamp2': int(next_k[0]),
                'open': open_price,
                'difference': abs(close_price - open_price)
            })

    if violations:
        logger.warning(
            "Continuity violations in fetched klines (potential data corruption): symbol=%s interval=%s violations=%d",
            symbol, interval, len(violations)
        )
        for v in violations[:5]:  # Log first 5 violations
            logger.warning(
                "Continuity violation: T1=%s close=%.2f, T2=%s open=%.2f, diff=%.2f",
                v['timestamp1'], v['close'], v['timestamp2'], v['open'], v['difference']
            )
        if len(violations) > 5:
            logger.warning("... and %d more continuity violations", len(violations) - 5)
        return False

    return True


def start_live_price_stream(
    symbols: List[str],
    redis_client: redis.Redis,
    app_logger: logging.Logger,
    price_callback: Optional[Callable[[str, float, int], None]] = None,
) -> None:
    """
    Connect to Binance WebSocket and stream live prices for all symbols.
    
    Persists prices to Redis and calls price_callback if provided.
    
    Parameters:
        symbols: List of trading symbols (e.g., ['BTCUSDT', 'ETHUSDT'])
        redis_client: Redis connection instance
        app_logger: Logger instance
        price_callback: Optional callback function(symbol, price, timestamp) called on each price update
    """
    if not symbols:
        app_logger.warning("No symbols provided for live price stream")
        return
    
    # Convert symbols to lowercase for WebSocket subscription
    streams = [f"{symbol.lower()}@ticker" for symbol in symbols]
    
    def on_message(ws, message):
        try:
            data = json.loads(message)
            symbol = data.get('s')  # Symbol from Binance (uppercase)
            price = float(data.get('c', 0))  # Last price
            timestamp = int(data.get('E', int(time.time() * 1000)))  # Event time in ms
            
            if symbol and price > 0:
                # Persist to Redis with key: live_price:{symbol}
                redis_key = f"live_price:{symbol}"
                redis_client.set(
                    redis_key,
                    json.dumps({
                        'symbol': symbol,
                        'price': price,
                        'timestamp': timestamp
                    })
                )
                
                # Call callback if provided
                if price_callback:
                    price_callback(symbol, price, timestamp)
        except Exception as e:
            app_logger.exception("Error processing live price message: %s", e)
    
    def on_error(ws, error):
        app_logger.error("WebSocket error: %s", error)
    
    def on_close(ws, close_status_code, close_msg):
        app_logger.warning("WebSocket closed: status=%s msg=%s", close_status_code, close_msg)
        # Attempt to reconnect after a delay
        time.sleep(5)
        start_live_price_stream(symbols, redis_client, app_logger, price_callback)
    
    def on_open(ws):
        app_logger.info("WebSocket connected, subscribing to symbols: %s", symbols)
    
    # Build WebSocket URL with all streams
    stream_param = "/".join(streams)
    url = f"wss://stream.binance.com:9443/ws/{stream_param}"
    
    try:
        ws = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        
        # Run WebSocket in a daemon thread
        ws_thread = threading.Thread(target=ws.run_forever, daemon=True)
        ws_thread.start()
        app_logger.info("Live price stream started for symbols: %s", symbols)
    except Exception as e:
        app_logger.exception("Failed to start live price stream: %s", e)


def get_live_price(symbol: str, redis_client: redis.Redis) -> Optional[dict]:
    """
    Retrieve the latest live price for a symbol from Redis.
    
    Returns:
        Dict with 'symbol', 'price', 'timestamp' or None if not available
    """
    redis_key = f"live_price:{symbol}"
    try:
        data = redis_client.get(redis_key)
        if data:
            return json.loads(data)
    except Exception:
        pass
    return None
    """
    Validate candle continuity in fetched klines before storing.

    Returns True if all continuity checks pass, False if violations found.
    Logs warnings for any continuity violations detected.
    """
    if len(klines) < 2:
        return True  # Can't check continuity with fewer than 2 klines

    violations = []

    for i in range(len(klines) - 1):
        current = klines[i]
        next_k = klines[i + 1]

        close_price = float(current[4])  # close
        open_price = float(next_k[1])   # open

        # Allow small tolerance to account for normal market microstructure
        CONTINUITY_TOLERANCE = 0.1  # 10 cents for crypto assets

        if abs(close_price - open_price) > CONTINUITY_TOLERANCE:
            violations.append({
                'index': i,
                'timestamp1': int(current[0]),
                'close': close_price,
                'timestamp2': int(next_k[0]),
                'open': open_price,
                'difference': abs(close_price - open_price)
            })

    if violations:
        logger.warning(
            "Continuity violations in fetched klines (potential data corruption): symbol=%s interval=%s violations=%d",
            symbol, interval, len(violations)
        )
        for v in violations[:5]:  # Log first 5 violations
            logger.warning(
                "Continuity violation: T1=%s close=%.2f, T2=%s open=%.2f, diff=%.2f",
                v['timestamp1'], v['close'], v['timestamp2'], v['open'], v['difference']
            )
        if len(violations) > 5:
            logger.warning("... and %d more continuity violations", len(violations) - 5)
        return False

    return True


def fetch_gap_from_binance(
    symbol: str,
    interval: str,
    gap_start: int,
    gap_end: int,
    interval_ms: int,
    upsert_klines_to_zset: Callable[[str, str, List[list]], Tuple[int, Optional[int], Optional[int]]],
    update_klines_meta_after_insert: Callable[[str, str, Optional[int], Optional[int], int], None],
    gap_logger: logging.Logger,
    app_logger: logging.Logger,
    now_ms: Optional[int] = None,
    redis_client: Optional[redis.Redis] = None,
    redis_key_template: str = "gap_fill_running:{symbol}:{interval}",
) -> None:
    """
    Fetch historical klines for a single [gap_start, gap_end] interval from Binance
    and persist them into Redis via the provided callbacks.

    This function is stateless w.r.t. Redis and logging so it can be executed
    safely in parallel threads for different gaps or (symbol, interval) pairs.
    """
    if now_ms is not None and gap_end > now_ms:
        gap_end = now_ms

    original_gap_start = gap_start
    original_gap_end = gap_end

    # Round gap boundaries to interval alignments to prevent overlapping candles
    gap_start = (gap_start // interval_ms) * interval_ms
    gap_end = ((gap_end + interval_ms - 1) // interval_ms) * interval_ms

    # Skip trivial or inverted gaps
    if gap_start >= gap_end:
        app_logger.debug(
            "Skipping trivial/future gap for symbol=%s interval=%s "
            "original_start=%s original_end=%s clamped_start=%s clamped_end=%s",
            symbol,
            interval,
            original_gap_start,
            original_gap_end,
            gap_start,
            gap_end,
        )
        return

    gap_logger.info(
        "Filling gap for symbol=%s interval=%s start=%s end=%s",
        symbol,
        interval,
        gap_start,
        gap_end,
    )

    start_time = gap_start

    while start_time < gap_end:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "limit": 1000,
        }
        try:
            resp = requests.get(
                "https://api.binance.com/api/v3/klines",
                params=params,
                timeout=10,
            )
            resp.raise_for_status()
            new_klines = resp.json()
            for kline in new_klines:
                kline[0] = (kline[0] // interval_ms) * interval_ms
        except Exception:
            app_logger.exception(
                "Error fetching klines from Binance for gap fill "
                "symbol=%s interval=%s start_time=%s",
                symbol,
                interval,
                start_time,
            )
            break

        if not new_klines:
            # No data for this portion of the gap; stop trying this gap.
            app_logger.debug(
                "No klines returned for gap segment: symbol=%s interval=%s "
                "gap_start=%s gap_end=%s segment_start=%s",
                symbol,
                interval,
                gap_start,
                gap_end,
                start_time,
            )
            break

        # Validate candle continuity before storing
        if not validate_kline_continuity(new_klines, symbol, interval, gap_logger):
            gap_logger.warning(
                "Skipping storage of corrupted klines batch: symbol=%s interval=%s batch_size=%d",
                symbol, interval, len(new_klines)
            )
            # Continue to next batch rather than break the entire gap fill
            start_time = ((new_klines[-1][0] // interval_ms) + 1) * interval_ms
            continue

        try:
            added_count, batch_earliest, batch_latest = upsert_klines_to_zset(
                symbol, interval, new_klines
            )
            update_klines_meta_after_insert(
                symbol,
                interval,
                batch_earliest,
                batch_latest,
                added_count,
            )
            if added_count:
                gap_logger.info(
                    "Gap fill batch: symbol=%s interval=%s added_count=%d "
                    "batch_earliest=%s batch_latest=%s",
                    symbol,
                    interval,
                    added_count,
                    batch_earliest,
                    batch_latest,
                )
            else:
                app_logger.debug(
                    "Gap fill batch produced no new candles: symbol=%s interval=%s "
                    "batch_earliest=%s batch_latest=%s",
                    symbol,
                    interval,
                    batch_earliest,
                    batch_latest,
                )
        except Exception:
            app_logger.exception(
                "Failed to insert klines into zset during gap fill symbol=%s interval=%s",
                symbol,
                interval,
            )
            break

        try:
            start_time = ((new_klines[-1][0] // interval_ms) + 1) * interval_ms
        except Exception:
            # If response format is unexpected, stop processing this gap.
            break

        # Extra safety against tight loops on pathological responses
        if start_time <= gap_start:
            break

    # Clear running flag on completion
    if redis_client:
        try:
            running_key = redis_key_template.format(symbol=symbol, interval=interval)
            redis_client.delete(running_key)
            gap_logger.info(
                "Cleared running flag for symbol=%s interval=%s",
                symbol,
                interval,
            )
        except Exception:
            app_logger.exception(
                "Failed to clear running flag for symbol=%s interval=%s",
                symbol,
                interval,
            )
