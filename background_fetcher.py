import logging
from typing import Callable, Optional, List, Tuple, Dict
import requests
import redis
import websocket
import json
import threading
import time
import queue
from datetime import datetime
from contextlib import contextmanager


class BackgroundFetcherSingleton:
    """
    Singleton class to manage background kline fetching with queue-based processing.
    Only one instance can process at a time to prevent overlapping data.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        self.redis_client = None
        self.is_processing = False
        self.fetch_queue = queue.Queue()
        self.worker_thread = None
        self.logger = logging.getLogger("background_fetcher")
        
    def initialize(self, redis_client: redis.Redis, upsert_func=None, update_meta_func=None):
        """Initialize the singleton with Redis client and upsert functions."""
        with self._lock:
            if not self.is_processing:
                self.redis_client = redis_client
                self.upsert_klines_to_zset = upsert_func
                self.update_klines_meta_after_insert = update_meta_func
                self.start_worker()
                self.logger.info("BackgroundFetcherSingleton initialized")
    
    def start_worker(self):
        """Start the background worker thread."""
        if self.worker_thread is None or not self.worker_thread.is_alive():
            self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
            self.worker_thread.start()
            self.logger.info("Background fetch worker started")
    
    def _process_queue(self):
        """Process fetch requests from the queue."""
        while True:
            try:
                # Get task from queue with timeout to allow graceful shutdown
                task = self.fetch_queue.get(timeout=1)
                
                if task is None:
                    break
                    
                symbol, interval, gap_start, gap_end, interval_ms = task
                
                self.logger.info(
                    "Processing fetch task: symbol=%s interval=%s start=%s end=%s",
                    symbol, interval, gap_start, gap_end
                )
                
                # Set processing flag to prevent concurrent operations
                with self._lock:
                    self.is_processing = True
                
                try:
                    # Process the fetch operation
                    self._fetch_gap_safe(symbol, interval, gap_start, gap_end, interval_ms)
                except Exception as e:
                    self.logger.exception("Error in fetch task: %s", e)
                finally:
                    # Reset processing flag
                    with self._lock:
                        self.is_processing = False
                
                # Mark task as done
                self.fetch_queue.task_done()
                
            except queue.Empty:
                # Continue checking for new tasks
                continue
            except Exception as e:
                self.logger.exception("Error in queue processing: %s", e)
    
    def _fetch_gap_safe(self, symbol: str, interval: str, gap_start: int, gap_end: int, interval_ms: int):
        """Safe version of fetch_gap_from_binance with proper error handling."""
        # Prevent concurrent operations on the same symbol/interval
        processing_key = f"processing:{symbol}:{interval}"
        
        if self.redis_client.get(processing_key):
            self.logger.warning("Already processing %s:%s, skipping", symbol, interval)
            return
        
        try:
            # Set processing lock
            self.redis_client.setex(processing_key, 300, "1")  # 5 minute timeout
            
            # Call the original fetch logic
            self._fetch_gap_core(symbol, interval, gap_start, gap_end, interval_ms)
            
        finally:
            # Always clear the processing lock
            self.redis_client.delete(processing_key)
    
    def _fetch_gap_core(self, symbol: str, interval: str, gap_start: int, gap_end: int, interval_ms: int):
        """Core fetch logic extracted for reuse."""
        # Create gap logger for this task
        gap_logger = logging.getLogger(f"gap_{symbol}_{interval}")
        
        # Use the same API logic but with better error handling
        if gap_end > int(time.time() * 1000):
            gap_end = int(time.time() * 1000)
        
        original_gap_start = gap_start
        original_gap_end = gap_end
        
        # Round gap boundaries to interval alignments to prevent overlapping candles
        gap_start = (gap_start // interval_ms) * interval_ms
        gap_end = ((gap_end + interval_ms - 1) // interval_ms) * interval_ms
        
        # Skip trivial or inverted gaps
        if gap_start >= gap_end:
            gap_logger.debug(
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
        
        start_time = gap_start
        
        while start_time < gap_end:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": start_time,
                "limit": 1000,
            }
            
            # Use futures endpoint for APEXUSDT, spot for others
            if symbol == "APEXUSDT":
                base_url = "https://fapi.binance.com/fapi/v1/klines"
            else:
                base_url = "https://api.binance.com/api/v3/klines"
            
            try:
                resp = requests.get(base_url, params=params, timeout=10)
                resp.raise_for_status()
                new_klines = resp.json()
                
                # Process timestamps to ensure integers
                for kline in new_klines:
                    kline[0] = int(kline[0])
                    kline[0] = (kline[0] // interval_ms) * interval_ms
                    
            except requests.exceptions.RequestException as e:
                gap_logger.warning("API request failed for %s:%s: %s", symbol, interval, e)
                break
            
            if not new_klines:
                gap_logger.debug(
                    "No klines returned for gap segment: symbol=%s interval=%s "
                    "gap_start=%s gap_end=%s segment_start=%s",
                    symbol, interval, gap_start, gap_end, start_time,
                )
                break
            
            # Validate continuity
            if not validate_kline_continuity(new_klines, symbol, interval, gap_logger):
                gap_logger.warning(
                    "Skipping storage of corrupted klines batch: symbol=%s interval=%s batch_size=%d",
                    symbol, interval, len(new_klines)
                )
                start_time = ((new_klines[-1][0] // interval_ms) + 1) * interval_ms
                continue
            
            # Check for overlaps
            existing_timestamps = check_kline_overlap(self.redis_client, symbol, interval, new_klines)
            if existing_timestamps:
                gap_logger.info(
                    "Filtering out %d overlapping klines for symbol=%s interval=%s",
                    len(existing_timestamps), symbol, interval
                )
                new_klines = [kline for kline in new_klines if int(kline[0]) not in existing_timestamps]
            
            if not new_klines:
                gap_logger.debug(
                    "All klines filtered out due to overlaps: symbol=%s interval=%s",
                    symbol, interval
                )
                start_time = ((start_time // interval_ms) + 1) * interval_ms
                continue
            
            gap_logger.info(
                "Successfully processed %d klines for %s:%s",
                len(new_klines), symbol, interval
            )

            # Insert the new klines into Redis
            if self.upsert_klines_to_zset and self.update_klines_meta_after_insert:
                try:
                    added_count, batch_earliest, batch_latest = self.upsert_klines_to_zset(symbol, interval, new_klines)
                    self.update_klines_meta_after_insert(symbol, interval, batch_earliest, batch_latest, added_count)
                    gap_logger.info(
                        "Inserted %d new klines for %s:%s (earliest=%s, latest=%s)",
                        added_count, symbol, interval, batch_earliest, batch_latest
                    )
                except Exception as e:
                    gap_logger.exception("Failed to insert klines for %s:%s: %s", symbol, interval, e)
            else:
                gap_logger.warning("Upsert functions not available for %s:%s", symbol, interval)

            # Move to next segment
            try:
                start_time = ((new_klines[-1][0] // interval_ms) + 1) * interval_ms
            except Exception:
                break

            # Safety check against infinite loops
            if start_time <= gap_start:
                break
    
    def queue_fetch(self, symbol: str, interval: str, gap_start: int, gap_end: int, interval_ms: int):
        """Queue a fetch operation for background processing."""
        if not self.redis_client:
            self.logger.error("BackgroundFetcherSingleton not initialized with Redis client")
            return
        
        # Check if already processing this exact task
        task_key = f"queued:{symbol}:{interval}:{gap_start}:{gap_end}"
        if self.redis_client.get(task_key):
            self.logger.debug("Task already queued: %s", task_key)
            return
        
        # Add to queue
        task = (symbol, interval, gap_start, gap_end, interval_ms)
        self.fetch_queue.put(task)
        
        # Mark as queued to prevent duplicates
        self.redis_client.setex(task_key, 3600, "1")  # 1 hour timeout
        
        self.logger.info("Queued fetch task: symbol=%s interval=%s start=%s end=%s",
                        symbol, interval, gap_start, gap_end)
    
    def shutdown(self):
        """Shutdown the background worker gracefully."""
        if self.worker_thread and self.worker_thread.is_alive():
            self.fetch_queue.put(None)
            self.worker_thread.join(timeout=5)
            self.logger.info("BackgroundFetcherSingleton shutdown complete")


def check_kline_overlap(redis_client: redis.Redis, symbol: str, interval: str, klines: List[list]) -> List[int]:
    """
    Check for overlapping klines in Redis and return list of existing timestamps.
    
    Returns list of timestamps that already exist in Redis for this symbol/interval.
    """
    if not redis_client or not klines:
        return []
    
    zset_key = f"klines:{symbol}:{interval}"
    existing_timestamps = []
    
    try:
        # Get the timestamp range we're checking
        timestamps = [int(kline[0]) for kline in klines]
        min_ts = min(timestamps)
        max_ts = max(timestamps)
        
        # Use ZRANGEBYSCORE to find existing klines in this range
        existing = redis_client.zrangebyscore(zset_key, min_ts, max_ts)
        
        # Extract timestamps from existing klines
        for kline_data in existing:
            try:
                kline = json.loads(kline_data)
                existing_timestamps.append(int(kline[0]))
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
                
    except Exception as e:
        # Log but don't fail the entire operation
        print(f"Warning: Could not check for overlaps: {e}")
    
    return existing_timestamps


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


# Legacy function for backward compatibility - now uses singleton
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
    Legacy function - now queues operations through the singleton pattern.
    
    This function is kept for backward compatibility but now delegates to the singleton.
    """
    if redis_client is None:
        app_logger.error("Redis client required for queue-based processing")
        return
    
    # Initialize singleton if needed
    fetcher = BackgroundFetcherSingleton()
    fetcher.initialize(redis_client)
    
    # Queue the operation for background processing
    fetcher.queue_fetch(symbol, interval, gap_start, gap_end, interval_ms)
    
    app_logger.info("Queued background fetch operation: symbol=%s interval=%s", symbol, interval)


def initialize_background_fetcher(redis_client: redis.Redis, upsert_func=None, update_meta_func=None) -> BackgroundFetcherSingleton:
    """Initialize and return the singleton background fetcher instance."""
    fetcher = BackgroundFetcherSingleton()
    fetcher.initialize(redis_client, upsert_func, update_meta_func)
    return fetcher


def shutdown_background_fetcher():
    """Shutdown all background fetchers."""
    fetcher = BackgroundFetcherSingleton()
    fetcher.shutdown()
