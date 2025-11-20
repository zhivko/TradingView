from flask import Flask, render_template, request, make_response, jsonify, session, redirect, url_for
from flask_mail import Mail, Message
from functools import wraps
import redis
import json
import requests
import logging
import math
import os
import secrets
import uuid
import io
import tempfile
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from config import SUPPORTED_RESOLUTIONS, SUPPORTED_SYMBOLS, TRADING_SYMBOL, MAIL_SERVER, MAIL_PORT, MAIL_USE_TLS, MAIL_USERNAME, MAIL_PASSWORD, MAIL_DEFAULT_SENDER, get_timeframe_seconds, SECRET_KEY
from flask_socketio import SocketIO, emit, join_room
from typing import Optional, Dict, Any, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from background_fetcher import fetch_gap_from_binance, start_live_price_stream, get_live_price
import pandas as pd
import pandas_ta as ta

app = Flask(__name__, static_folder='static', template_folder='templates')
socketio = SocketIO(app, cors_allowed_origins="*")

# Session / cookie security; set SESSION_COOKIE_SECURE=True in production behind HTTPS.
app.config.update(
    SECRET_KEY=SECRET_KEY,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=False,
)

load_cancel_flags = {}

# Mail configuration
app.config['MAIL_SERVER'] = MAIL_SERVER
app.config['MAIL_PORT'] = MAIL_PORT
app.config['MAIL_USE_TLS'] = MAIL_USE_TLS
app.config['MAIL_USERNAME'] = MAIL_USERNAME
app.config['MAIL_PASSWORD'] = MAIL_PASSWORD
app.config['MAIL_DEFAULT_SENDER'] = MAIL_DEFAULT_SENDER

mail = Mail(app)

r = redis.Redis(host='localhost', port=6379, db=0)

# Redis key templates for kline storage
KLINES_ZSET_KEY_TEMPLATE = "klines_z:{symbol}:{interval}"
KLINES_META_KEY_TEMPLATE = "klines_meta:{symbol}:{interval}"


def get_klines_zset_key(symbol: str, interval: str) -> str:
    """
    Build the Redis sorted-set key for a given (symbol, interval) pair.
    """
    return KLINES_ZSET_KEY_TEMPLATE.format(symbol=symbol, interval=interval)


def get_klines_meta_key(symbol: str, interval: str) -> str:
    """
    Build the Redis metadata key for a given (symbol, interval) pair.
    """
    return KLINES_META_KEY_TEMPLATE.format(symbol=symbol, interval=interval)


def load_klines_meta(symbol: str, interval: str) -> Dict[str, Any]:
    """
    Load kline metadata for a (symbol, interval) pair.

    Returns a dict parsed from JSON, or {} if the key is missing or invalid.
    """
    key = get_klines_meta_key(symbol, interval)
    try:
        raw = r.get(key)
        if not raw:
            return {}
        return json.loads(raw)
    except Exception as exc:
        # Metadata is non-critical; fall back to empty on any failure.
        logging.getLogger(__name__).warning(
            "Failed to load klines meta: symbol=%s interval=%s error=%s",
            symbol,
            interval,
            exc,
        )
        return {}


def save_klines_meta(symbol: str, interval: str, meta: Dict[str, Any]) -> None:
    """
    Persist kline metadata for a (symbol, interval) pair as JSON.
    """
    key = get_klines_meta_key(symbol, interval)
    # Use compact separators to reduce storage size.
    payload = json.dumps(meta, separators=(",", ":"))
    r.set(key, payload)


def upsert_klines_to_zset(
    symbol: str,
    interval: str,
    klines_batch: List[list],
) -> Tuple[int, Optional[int], Optional[int]]:
    """
    Insert a batch of klines into the sorted set for (symbol, interval).

    Each kline `k` is expected to be a list/tuple compatible with Binance's
    /klines format, where:
        k[0] = open time (ms since epoch)

    Timestamps are rounded to interval boundaries to prevent duplicates.
    The Redis sorted set member is the JSON-encoded kline, scored by k[0].

    Returns:
        (new_count, earliest_ts, latest_ts)
        - new_count: number of *new* members actually added (NX semantics)
        - earliest_ts: minimum k[0] in the batch (or None if batch empty)
        - latest_ts: maximum k[0] in the batch (or None if batch empty)
    """
    if not klines_batch:
        return 0, None, None

    zset_key = get_klines_zset_key(symbol, interval)
    interval_ms = get_timeframe_seconds(interval) * 1000

    earliest_ts: Optional[int] = None
    latest_ts: Optional[int] = None

    pipe = r.pipeline()
    seen = set()
    for k in klines_batch:
        if not k:
            continue
        try:
            # Round timestamp to interval boundary to prevent duplicates
            original_ts = int(k[0])
            rounded_ts = (original_ts // interval_ms) * interval_ms

            # Update the kline with rounded timestamp
            k[0] = rounded_ts

            if rounded_ts in seen:
                continue
            seen.add(rounded_ts)
        except (TypeError, ValueError):
            # Skip malformed entries
            continue

        if earliest_ts is None or rounded_ts < earliest_ts:
            earliest_ts = rounded_ts
        if latest_ts is None or rounded_ts > latest_ts:
            latest_ts = rounded_ts

        member = json.dumps(k, separators=(",", ":"))
        pipe.zadd(zset_key, {member: rounded_ts}, nx=True)

    results = pipe.execute()
    # Each zadd returns 1 if a new member was added, 0 otherwise.
    new_count = sum(int(res) for res in results if isinstance(res, (int, float)))

    return new_count, earliest_ts, latest_ts


def update_klines_meta_after_insert(
    symbol: str,
    interval: str,
    batch_earliest: Optional[int],
    batch_latest: Optional[int],
    added_count: int,
) -> None:
    """
    Update klines metadata after inserting a batch into the sorted set.

    This maintains:
        - earliest: minimum timestamp ever seen
        - latest: maximum timestamp ever seen
        - count: total number of elements inserted (approximate, based on adds)
        - version: schema version (currently 1)

    This function is safe to call even if `added_count` is zero or the batch
    timestamps are None; in that case it becomes a no-op for times and count,
    but still ensures that version is at least 1.
    """
    meta = load_klines_meta(symbol, interval)

    current_earliest = meta.get("earliest")
    current_latest = meta.get("latest")
    current_count = meta.get("count", 0) or 0

    # Update earliest/latest only if we have valid batch timestamps.
    if batch_earliest is not None:
        if current_earliest is None:
            meta["earliest"] = int(batch_earliest)
        else:
            meta["earliest"] = int(min(int(current_earliest), int(batch_earliest)))

    if batch_latest is not None:
        if current_latest is None:
            meta["latest"] = int(batch_latest)
        else:
            meta["latest"] = int(max(int(current_latest), int(batch_latest)))

    # Increment count by number of newly added elements.
    if added_count:
        try:
            meta["count"] = int(current_count) + int(added_count)
        except Exception:
            meta["count"] = int(added_count)

    # Ensure version is at least 1.
    existing_version = meta.get("version", 0) or 0
    meta["version"] = max(int(existing_version), 1)

    save_klines_meta(symbol, interval, meta)


def get_earliest_kline_time(symbol: str, interval: str) -> Optional[int]:
    """
    Return the earliest kline open time (ms) for (symbol, interval).

    Prefers the metadata's `earliest` field when present; falls back to
    inspecting the sorted set via ZRANGE if metadata is missing.
    """
    meta = load_klines_meta(symbol, interval)
    earliest = meta.get("earliest")
    if earliest is not None:
        try:
            return int(earliest)
        except (TypeError, ValueError):
            pass

    zset_key = get_klines_zset_key(symbol, interval)
    try:
        rows = r.zrange(zset_key, 0, 0, withscores=True)
        if not rows:
            return None
        _, score = rows[0]
        return int(score)
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Failed to read earliest kline time from zset: symbol=%s interval=%s error=%s",
            symbol,
            interval,
            exc,
        )
        return None


def get_latest_kline_time(symbol: str, interval: str) -> Optional[int]:
    """
    Return the latest kline open time (ms) for (symbol, interval).

    Prefers the metadata's `latest` field when present; falls back to
    inspecting the sorted set via ZREVRANGE if metadata is missing.
    """
    meta = load_klines_meta(symbol, interval)
    latest = meta.get("latest")
    if latest is not None:
        try:
            return int(latest)
        except (TypeError, ValueError):
            pass

    zset_key = get_klines_zset_key(symbol, interval)
    try:
        rows = r.zrevrange(zset_key, 0, 0, withscores=True)
        if not rows:
            return None
        _, score = rows[0]
        return int(score)
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Failed to read latest kline time from zset: symbol=%s interval=%s error=%s",
            symbol,
            interval,
            exc,
        )
        return None


def ms_to_iso(ms: int) -> str:
    """
    Convert a millisecond UNIX timestamp to an ISO 8601 UTC string.

    Falls back to str(ms) if conversion fails.
    """
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        return str(ms)


def aggregate_1m_candles_to_interval(
    candles_1m: List[dict],
    target_interval: str,
    interval_ms: int
) -> Tuple[List[int], List[float], List[float], List[float], List[float], List[float]]:
    """
    Aggregate 1-minute candles into a higher timeframe.
    
    Args:
        candles_1m: List of 1-min candles (dictionaries with time, open, high, low, close, volume)
        target_interval: Target interval name (e.g., '5m', '1h', '4h', '1d')
        interval_ms: Milliseconds for target interval
    
    Returns:
        Tuple of (times, opens, highs, lows, closes, volumes) as parallel lists
    """
    if not candles_1m:
        return [], [], [], [], [], []
    
    # Group candles by target interval boundary
    candles_by_interval = {}
    
    for candle in candles_1m:
        try:
            open_time = int(candle.get('time', 0))
            open_price = float(candle.get('open', 0))
            high_price = float(candle.get('high', 0))
            low_price = float(candle.get('low', 0))
            close_price = float(candle.get('close', 0))
            volume = float(candle.get('volume', 0))
            
            # Round to interval boundary
            interval_key = (open_time // interval_ms) * interval_ms
            
            if interval_key not in candles_by_interval:
                # First candle in this interval
                candles_by_interval[interval_key] = {
                    'time': interval_key,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': volume
                }
            else:
                # Aggregate: take highest high, lowest low, latest close, sum volume
                existing = candles_by_interval[interval_key]
                existing['high'] = max(existing['high'], high_price)
                existing['low'] = min(existing['low'], low_price)
                existing['close'] = close_price
                existing['volume'] += volume
        except (ValueError, KeyError, TypeError):
            # Skip malformed candles
            continue
    
    # Convert to parallel arrays, sorted by time
    times = []
    opens = []
    highs = []
    lows = []
    closes = []
    volumes = []
    
    for interval_key in sorted(candles_by_interval.keys()):
        agg = candles_by_interval[interval_key]
        times.append(agg['time'])
        opens.append(agg['open'])
        highs.append(agg['high'])
        lows.append(agg['low'])
        closes.append(agg['close'])
        volumes.append(agg['volume'])
    
    return times, opens, highs, lows, closes, volumes


# Logging configuration
os.makedirs("logs", exist_ok=True)

file_handler = RotatingFileHandler(
    "logs/app.log", maxBytes=10 * 1024 * 1024, backupCount=5
)
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s [%(name)s] %(filename)s:%(lineno)d - %(message)s"
)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

# Add console handler for development
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

# Remove any existing console handlers to avoid duplicate logging
for h in list(app.logger.handlers):
    if isinstance(h, logging.StreamHandler):
        app.logger.removeHandler(h)

if not any(isinstance(h, RotatingFileHandler) for h in app.logger.handlers):
    app.logger.addHandler(file_handler)
app.logger.addHandler(console_handler)

app.logger.setLevel(logging.INFO)

# Suppress werkzeug access logs to avoid different log formats
logging.getLogger('werkzeug').setLevel(logging.WARNING)

app.logger.info("Application startup")

gap_logger = logging.getLogger("gap_fill_batch")

# Ensure the gap-fill logger is file-only and does not emit to console.
gap_logger.setLevel(logging.INFO)
gap_logger.propagate = False

# Find an existing gap-fill file handler if one is already attached.
existing_gap_file_handler = None
for h in gap_logger.handlers:
    if isinstance(h, RotatingFileHandler) and getattr(
        h, "baseFilename", ""
    ).endswith("gap_fill_batch.log"):
        existing_gap_file_handler = h
        break

# Create the dedicated file handler if it does not exist yet.
if existing_gap_file_handler is None:
    gap_file_handler = RotatingFileHandler(
        "logs/gap_fill_batch.log", maxBytes=10 * 1024 * 1024, backupCount=5
    )
    gap_file_handler.setFormatter(formatter)
    gap_file_handler.setLevel(logging.INFO)
    gap_logger.addHandler(gap_file_handler)
else:
    # Reuse the existing handler but ensure it has the correct formatter/level.
    gap_file_handler = existing_gap_file_handler
    gap_file_handler.setFormatter(formatter)
    gap_file_handler.setLevel(logging.INFO)

# Remove any non-file handlers (e.g. console/stream) so gap_fill_batch
# messages never inherit or use console handlers.
for h in list(gap_logger.handlers):
    if h is not gap_file_handler:
        gap_logger.removeHandler(h)

latest_logger = logging.getLogger("latest_fetch_batch")

# Ensure the latest fetch logger is file-only and does not emit to console.
latest_logger.setLevel(logging.INFO)
latest_logger.propagate = False

# Find an existing latest fetch file handler if one is already attached.
existing_latest_file_handler = None
for h in latest_logger.handlers:
    if isinstance(h, RotatingFileHandler) and getattr(
        h, "baseFilename", ""
    ).endswith("latest_fetch_batch.log"):
        existing_latest_file_handler = h
        break

# Create the dedicated file handler if it does not exist yet.
if existing_latest_file_handler is None:
    latest_file_handler = RotatingFileHandler(
        "logs/latest_fetch_batch.log", maxBytes=10 * 1024 * 1024, backupCount=5
    )
    latest_file_handler.setFormatter(formatter)
    latest_file_handler.setLevel(logging.INFO)
    latest_logger.addHandler(latest_file_handler)
else:
    # Reuse the existing handler but ensure it has the correct formatter/level.
    latest_file_handler = existing_latest_file_handler
    latest_file_handler.setFormatter(formatter)
    latest_file_handler.setLevel(logging.INFO)

# Remove any non-file handlers (e.g. console/stream) so latest_fetch_batch
# messages never inherit or use console handlers.
for h in list(latest_logger.handlers):
    if h is not latest_file_handler:
        latest_logger.removeHandler(h)

import threading
import time
import whisper
import httpx



def start_background_fetch():
    """
    Background task that continuously:
      1) Scans Redis for gaps per (symbol, interval).
      2) Submits gap fetches to a shared ThreadPoolExecutor for concurrent
          historical backfill from Binance.
      3) Periodically fetches the latest 500 candles per (symbol, interval).

    The actual HTTP fetch + insert for a single gap lives in
    background_fetcher.fetch_gap_from_binance so that this function stays
    concise and orchestration-focused.
    """
    def fetch_loop():
        while True:
            executor = ThreadPoolExecutor(max_workers=8)
            # First, discover all gaps across all (symbol, interval) pairs.
            gap_tasks = []  # (symbol, interval, gap_start, gap_end, interval_ms, now_ms)
            started_gap_symbols_intervals = set()
            for symbol in SUPPORTED_SYMBOLS:
                for interval in SUPPORTED_RESOLUTIONS:
                    try:
                        zset_key = get_klines_zset_key(symbol, interval)

                        gap_fill_running_key = f'gap_fill_running:{symbol}:{interval}'
                        last_gap_fill_key = f'last_gap_fill:{symbol}:{interval}'
                        last_fill = r.get(last_gap_fill_key)
                        current_time_sec = int(time.time())
                        now_ms = current_time_sec * 1000

                        # Gap fill per (symbol, interval) - only if not already running
                        is_running = r.get(gap_fill_running_key)
                        if not is_running:
                            # Set running flag with 2 hour expiration (safety valve)
                            r.set(gap_fill_running_key, str(current_time_sec), ex=7200)

                            interval_ms = get_timeframe_seconds(interval) * 1000
                            bootstrap_start_ms = 1577836800000

                            gap_logger.info(
                                "Starting gap analysis for symbol=%s interval=%s",
                                symbol,
                                interval,
                            )

                            # Detect gaps using ZSET scanning instead of loading full JSON lists.
                            gaps = []
                            earliest = get_earliest_kline_time(symbol, interval)
                            latest = get_latest_kline_time(symbol, interval)

                            if earliest is None or latest is None:
                                # No existing data; bootstrap the entire historical range.
                                gaps.append((bootstrap_start_ms, now_ms))
                            else:
                                # Leading gap from bootstrap_start_ms up to the earliest stored candle.
                                if earliest > bootstrap_start_ms:
                                    gaps.append(
                                        (
                                            bootstrap_start_ms,
                                            int(earliest) - interval_ms,
                                        )
                                    )

                                chunk_size = 5000
                                prev_time: Optional[int] = None
                                current_min = int(earliest)

                                while True:
                                    try:
                                        # members: list of (member_bytes, score_float)
                                        members = r.zrangebyscore(
                                            zset_key,
                                            current_min,
                                            "+inf",
                                            start=0,
                                            num=chunk_size,
                                            withscores=True,
                                        )
                                    except Exception:
                                        app.logger.exception(
                                            "Failed to scan zset for gaps: symbol=%s interval=%s",
                                            symbol,
                                            interval,
                                        )
                                        break

                                    if not members:
                                        break

                                    for member, score in members:
                                        try:
                                            current_time = int(score)
                                        except (TypeError, ValueError):
                                            # Skip malformed scores
                                            continue

                                        if prev_time is not None:
                                            expected_next = prev_time + interval_ms
                                            if expected_next < current_time:
                                                gaps.append(
                                                    (expected_next, current_time - interval_ms)
                                                )
                                        prev_time = current_time

                                    # Prepare next chunk range (open interval)
                                    last_score = int(members[-1][1])
                                    current_min = last_score + 1

                                # Tail gap from last kline to "now"
                                if (
                                    prev_time is not None
                                    and prev_time + interval_ms < now_ms
                                ):
                                    gaps.append((prev_time + interval_ms, now_ms))

                            gap_logger.info(
                                "Detected %d gaps for symbol=%s interval=%s",
                                len(gaps),
                                symbol,
                                interval,
                            )

                            # Record gaps for parallel processing.
                            for gap_start, gap_end in gaps:
                                gap_tasks.append(
                                    (symbol, interval, gap_start, gap_end, interval_ms, now_ms)
                                )

                            # Mark gap analysis completion time (even though gap fill may still be running).
                            try:
                                r.set(last_gap_fill_key, str(current_time_sec))
                            except Exception:
                                app.logger.exception(
                                    "Failed to update last_gap_fill for symbol=%s interval=%s",
                                    symbol,
                                    interval,
                                )

                        # Ongoing fetch for latest klines; fetch from last stored timestamp to now
                        try:
                            latest_ts = get_latest_kline_time(symbol, interval)
                            if latest_ts is not None:
                                start_time = latest_ts
                            else:
                                # No data yet, start from bootstrap
                                start_time = 1577836800000  # 2020-01-01

                            end_time = current_time_sec * 1000

                            if start_time >= end_time:
                                # No new data to fetch
                                continue

                            # Fetch klines from start_time to end_time
                            new_klines = fetch_klines_range(symbol, interval, start_time, end_time)
                            if new_klines:
                                # Validate candle continuity before storing
                                if not validate_kline_continuity(new_klines, symbol, interval, latest_logger):
                                    latest_logger.warning(
                                        "Skipping storage of corrupted latest klines batch: symbol=%s interval=%s batch_size=%d",
                                        symbol, interval, len(new_klines)
                                    )
                                    # Skip this batch but continue with next cycle - exit the try-except block cleanly
                                    continue
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
                                batch_earliest_iso = (
                                    ms_to_iso(batch_earliest)
                                    if batch_earliest is not None
                                    else str(batch_earliest)
                                )
                                batch_latest_iso = (
                                    ms_to_iso(batch_latest)
                                    if batch_latest is not None
                                    else str(batch_latest)
                                )
                                latest_logger.info(
                                    "Latest fetch batch: symbol=%s interval=%s added_count=%d "
                                    "batch_earliest=%s (%s) batch_latest=%s (%s)",
                                    symbol,
                                    interval,
                                    added_count,
                                    batch_earliest,
                                    batch_earliest_iso,
                                    batch_latest,
                                    batch_latest_iso,
                                )
                        except Exception:
                            app.logger.exception(
                                "Error during latest klines fetch for symbol=%s interval=%s",
                                symbol,
                                interval,
                            )
                    except Exception:
                        # Ensure one bad (symbol, interval) never crashes the background thread.
                        app.logger.exception(
                            "Unexpected error in background fetch loop for symbol=%s interval=%s",
                            symbol,
                            interval,
                        )

            # After scanning all (symbol, interval) pairs, execute gap fills in parallel.
            if gap_tasks:
                app.logger.info(
                    "Submitting %d gap tasks to ThreadPoolExecutor", len(gap_tasks)
                )
                futures = []
                for symbol, interval, gap_start, gap_end, interval_ms, now_ms in gap_tasks:
                    futures.append(
                        executor.submit(
                            fetch_gap_from_binance,
                            symbol,
                            interval,
                            gap_start,
                            gap_end,
                            interval_ms,
                            upsert_klines_to_zset,
                            update_klines_meta_after_insert,
                            gap_logger,
                            app.logger,
                            now_ms,
                            r,  # Redis client
                            "gap_fill_running:{symbol}:{interval}",  # key template
                        )
                    )

                # Optionally wait for completion to have bounded concurrency per cycle.
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception:
                        app.logger.exception("Gap task failed in executor")

            time.sleep(60)

    t = threading.Thread(target=fetch_loop, daemon=True)
    t.start()

# Background fetch starts when app runs


def fetch_klines_range(symbol, interval, start_time, end_time, klines=None, progress_callback=None):
    """
    Fetch klines from Binance in [start_time, end_time].
    If klines is provided, extend it with fetched data and calculate progress based on time range coverage.
    Otherwise, return fetched klines with progress based on fetch fraction.
    Progress callback expects (percentage, info_dict) when klines is provided, (fraction, info_dict) otherwise.
    """
    if klines is not None:
        # Time-range based progress
        fetched_klines = []
        current_start = start_time
        interval_ms = get_timeframe_seconds(interval) * 1000

        # Initial progress
        if end_time > start_time:
            latest_time = max(k[0] for k in klines) if klines else start_time
            initial_percentage = ((latest_time - start_time) / (end_time - start_time)) * 100
            initial_percentage = max(0.0, min(100.0, initial_percentage))
        else:
            initial_percentage = 100.0 if klines else 0.0

        if progress_callback:
            try:
                progress_callback(initial_percentage, {"symbol": symbol, "interval": interval})
            except Exception:
                pass

        while current_start < end_time:
            params = {
                'symbol': symbol,
                'interval': interval,
                'startTime': current_start,
                'limit': 1000
            }
            # Use futures endpoint for APEXUSDT, spot for others
            base_url = "https://fapi.binance.com/fapi/v1/klines" if symbol == "APEXUSDT" else "https://api.binance.com/api/v3/klines"
            try:
                resp = requests.get(base_url, params=params)
                new_klines = resp.json()
                if not new_klines:
                    break
                fetched_klines.extend(new_klines)
                klines.extend(new_klines)
                current_start = new_klines[-1][0] + interval_ms
                # Progress after extending
                if end_time > start_time:
                    latest_time = max(k[0] for k in klines) if klines else start_time
                    percentage = ((latest_time - start_time) / (end_time - start_time)) * 100
                    percentage = max(0.0, min(100.0, percentage))
                else:
                    percentage = 100.0 if klines else 0.0
                if progress_callback:
                    try:
                        progress_callback(percentage, {"symbol": symbol, "interval": interval})
                    except Exception:
                        pass
                if current_start >= end_time:
                    break
            except Exception as e:
                print(f"Error fetching klines: {e}")
                break

        # Filter
        klines[:] = [k for k in klines if k[0] <= end_time]

        # Final progress
        if end_time > start_time:
            latest_time = max(k[0] for k in klines) if klines else start_time
            final_percentage = ((latest_time - start_time) / (end_time - start_time)) * 100
            final_percentage = max(0.0, min(100.0, final_percentage))
        else:
            final_percentage = 100.0 if klines else 0.0

        if progress_callback:
            try:
                progress_callback(final_percentage, {"symbol": symbol, "interval": interval})
            except Exception:
                pass

        return fetched_klines
    else:
        # Original logic for background fetch
        klines = []
        current_start = start_time
        interval_ms = get_timeframe_seconds(interval) * 1000

        total_range = max(int(end_time - start_time), 1) if end_time is not None and start_time is not None else None

        if progress_callback and total_range is not None:
            try:
                progress_callback(0.0, {"symbol": symbol, "interval": interval})
            except Exception:
                pass

        while current_start < end_time:
            params = {
                'symbol': symbol,
                'interval': interval,
                'startTime': current_start,
                'limit': 1000
            }
            # Use futures endpoint for APEXUSDT, spot for others
            base_url = "https://fapi.binance.com/fapi/v1/klines" if symbol == "APEXUSDT" else "https://api.binance.com/api/v3/klines"
            try:
                resp = requests.get(base_url, params=params)
                new_klines = resp.json()
                if not new_klines:
                    break
                klines.extend(new_klines)
                current_start = new_klines[-1][0] + interval_ms
                if total_range is not None and progress_callback:
                    try:
                        fraction = (current_start - start_time) / total_range
                        fraction = max(0.0, min(1.0, float(fraction)))
                        progress_callback(fraction, {"symbol": symbol, "interval": interval})
                    except Exception:
                        pass
                if current_start >= end_time:
                    break
            except Exception as e:
                print(f"Error fetching klines: {e}")
                break

        klines = [k for k in klines if k[0] <= end_time]

        if progress_callback and total_range is not None:
            try:
                progress_callback(1.0, {"symbol": symbol, "interval": interval})
            except Exception:
                pass

        return klines


def get_current_user_id() -> Optional[str]:
    """
    Return the current authenticated user identifier.

    If a logged-in email session exists, returns session['user_email'].
    Otherwise, if a guest session exists, returns session['guest_id'].
    Returns None when no authenticated or guest user is present.
    """
    email = session.get("user_email")
    if email:
        return email

    guest_id = session.get("guest_id")
    if guest_id:
        return guest_id

    return None


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not get_current_user_id():
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated_function


@socketio.on('join_user_room')
def handle_join_user_room(data=None):
    """
    WebSocket event: client requests to join its personal room.

    The client may emit this event with or without a payload; we ignore the
    payload and derive the room name from the authenticated user id stored
    in the server-side session (email for logged-in users, guest_id for guests).
    """
    user_id = get_current_user_id()
    if not user_id:
        app.logger.warning("join_user_room denied: unauthenticated client (data=%s)", data)
        return

    join_room(user_id)
    app.logger.info("User %s joined personal Socket.IO room", user_id)


@socketio.on('cancel_load')
def handle_cancel_load(data):
    load_id = (data or {}).get('loadId')
    user_id = get_current_user_id()

    if user_id and load_id:
        if user_id not in load_cancel_flags:
            load_cancel_flags[user_id] = {}
        load_cancel_flags[user_id][load_id] = True
        app.logger.info(f"Canceled load {load_id} for user {user_id}")


@socketio.on('get_live_price')
def handle_get_live_price(data):
    """Handle request for current live price of a symbol"""
    symbol = (data or {}).get('symbol', '')
    if not symbol:
        emit('live_price', {'error': 'Symbol required'})
        return
    
    price_data = get_live_price(symbol, r)
    if price_data:
        emit('live_price', price_data)
    else:
        emit('live_price', {'error': f'No live price available for {symbol}'})


def log_route(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            duration_ms = (time.time() - start) * 1000
            app.logger.info(
                "Exiting route %s path=%s method=%s duration_ms=%.2f",
                func.__name__, request.path, request.method, duration_ms
            )
    return wrapper


def validate_kline_continuity(new_klines, symbol, interval, logger):
    """
    Validate that consecutive klines have proper continuity (close[i] ≈ open[i+1]).

    Returns True if validation passes, False if continuity violations found.
    """
    if not new_klines or len(new_klines) < 2:
        return True  # Single candle or empty batch is trivially valid

    # Sort by timestamp to ensure chronological order
    sorted_klines = sorted(new_klines, key=lambda k: int(k[0]))

    CONTINUITY_TOLERANCE = 10  # Allow 10 cents difference to avoid flagging normal market behavior

    violations_found = False

    for i in range(len(sorted_klines) - 1):
        try:
            close_price = float(sorted_klines[i][4])  # Close is index 4
            next_open_price = float(sorted_klines[i + 1][1])  # Open is index 1

            if abs(close_price - next_open_price) > CONTINUITY_TOLERANCE:
                logger.warning(
                    "Candle continuity violation: symbol=%s interval=%s close_price=%s next_open_price=%s "
                    "timestamp=%s next_timestamp=%s diff=%s tolerance=%s",
                    symbol,
                    interval,
                    close_price,
                    next_open_price,
                    sorted_klines[i][0],
                    sorted_klines[i + 1][0],
                    abs(close_price - next_open_price),
                    CONTINUITY_TOLERANCE
                )
                violations_found = True

        except (IndexError, TypeError, ValueError) as e:
            logger.warning(
                "Failed to parse prices for continuity check: symbol=%s interval=%s index=%s error=%s",
                symbol, interval, i, e
            )
            violations_found = True  # Treat parse errors as violations

    return not violations_found


def analyze_timestamps(timestamps, symbol, interval, user_id):
    """
    Analyze timestamps for duplicates and patterns.

    Args:
        timestamps: List of timestamp integers from Redis

    Returns:
        dict: Analysis containing duplicate statistics and patterns
    """
    if not timestamps:
        return {
            'total': 0,
            'unique': 0,
            'duplicates': [],
            'patterns': [],
            'hasDuplicates': False,
            'duplicateCount': 0
        }

    analysis = {
        'total': len(timestamps),
        'unique': len(set(timestamps)),
        'duplicates': [],
        'patterns': [],
        'hasDuplicates': False,
        'duplicateCount': 0
    }

    # Find duplicates
    seen = {}
    for ts in timestamps:
        if ts in seen:
            seen[ts] += 1
            if seen[ts] == 2:  # First duplicate
                analysis['hasDuplicates'] = True
                analysis['duplicates'].append(ts)
        else:
            seen[ts] = 1

    # Count total duplicates (not just unique values)
    analysis['duplicateCount'] = sum(count - 1 for count in seen.values() if count > 1)

    # Find consecutive duplicate patterns
    if len(timestamps) > 1:
        current_streak = None
        current_count = 0

        for i, ts in enumerate(timestamps):
            if ts == current_streak:
                current_count += 1
            else:
                # Finish previous streak
                if current_streak is not None and current_count > 1:
                    analysis['patterns'].append({
                        'timestamp': current_streak,
                        'consecutiveCount': current_count,
                        'date': datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
                    })
                # Start new streak
                current_streak = ts
                current_count = 1

        # Check last streak
        if current_streak is not None and current_count > 1:
            analysis['patterns'].append({
                'timestamp': current_streak,
                'consecutiveCount': current_count,
                'date': datetime.fromtimestamp(current_streak / 1000, tz=timezone.utc).isoformat()
            })

    # Sort duplicates by frequency
    analysis['duplicates'].sort(key=lambda ts: seen[ts], reverse=True)

    # Add human readable dates to patterns for easier debugging
    for pattern in analysis['patterns']:
        pattern['date'] = datetime.fromtimestamp(pattern['timestamp'] / 1000, tz=timezone.utc).isoformat()

    return analysis


@app.route('/settings', methods=['GET', 'POST'])
@log_route
def settings():
    if request.method == 'POST':
        payload = request.get_json()
        r.set('settings', json.dumps(payload))
        return jsonify({'status': 'success'})
    else:
        data = r.get('settings')
        if data:
            settings_data = json.loads(data)
        else:
            settings_data = {}
        return jsonify(settings_data)

@app.route('/view-range', methods=['GET', 'POST'])
@log_route
def view_range():
    user_id = get_current_user_id()
    if not user_id:
        return jsonify({'error': 'Unauthorized'}), 401

    key = f'view_range:{user_id}'
    if request.method == 'POST':
        payload = request.get_json()
        r.set(key, json.dumps(payload))
        return jsonify({'status': 'success'})
    else:
        data = r.get(key)
        if data:
            return jsonify(json.loads(data))
        else:
            # Return a default range (last 10 days) for new users
            from datetime import timedelta
            now = datetime.now(timezone.utc)
            ten_days_ago = now - timedelta(days=10)
            default_range = {
                'xaxis': {
                    'range': [ten_days_ago.isoformat(), now.isoformat()]
                },
                'yaxis': {
                    'range': None
                }
            }
            return jsonify(default_range)


@app.route('/drawings', methods=['GET', 'POST'])
@log_route
@login_required
def drawings():
    """
    Store and retrieve user drawings (lines, rectangles, etc.) per symbol.

    Data is stored in Redis under key:
        drawings:<user_email>:<symbol>

    GET:  /drawings?symbol=BTCUSDT&interval=1h&limit=500
          → { "shapes": [...], "rect_volume_profiles": [...] } (if rectangles present)

    POST: /drawings
          body: { "symbol": "BTCUSDT", "shapes": [...] }
    """
    user_id = get_current_user_id()
    if not user_id:
        return jsonify({'error': 'Unauthorized'}), 401

    if request.method == 'GET':
        symbol = request.args.get('symbol')
        if not symbol:
            return jsonify({'error': 'symbol is required'}), 400

        key = f'drawings:{user_id}:{symbol}'
        data = r.get(key)
        if not data:
            return jsonify({'shapes': []})

        try:
            shapes = json.loads(data)
        except Exception:
            shapes = []
        
        # If we have shapes, compute volume profiles for any rectangles
        rect_volume_profiles = []
        if shapes and isinstance(shapes, list):
            # Get current timeframe and limit for volume profile calculation
            interval = request.args.get('interval', SUPPORTED_RESOLUTIONS[0])
            limit = request.args.get('limit', 500)
            start_time_str = request.args.get('startTime')
            end_time_str = request.args.get('endTime')
             
            start_time = int(start_time_str) if start_time_str else None
            end_time = int(end_time_str) if end_time_str else None
            limit = int(limit)
             
            try:
                # Get volume profiles for rectangles using the existing function
                _, _, _, _, _, _, rect_vps = get_volume_profile(
                    symbol=symbol,
                    interval=interval,
                    user_email=user_id,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit,
                    klines=None,  # Will fetch from Redis
                )
                rect_volume_profiles = rect_vps
            except Exception as e:
                app.logger.warning(
                    "Failed to compute volume profiles for drawings: symbol=%s error=%s",
                    symbol, e
                )
                rect_volume_profiles = []
        
        response_data = {'shapes': shapes}
        if rect_volume_profiles:
            response_data['rect_volume_profiles'] = rect_volume_profiles
            
        return jsonify(response_data)

    # POST - save shapes
    payload = request.get_json(silent=True) or {}
    symbol = payload.get('symbol')
    shapes = payload.get('shapes', [])

    app.logger.info(
        "Drawings POST request - user=%s symbol=%s shapes_count=%s shapes=%s",
        user_id, symbol, len(shapes), json.dumps(shapes)[:500] + ('...' if len(json.dumps(shapes)) > 500 else '')
    )

    if not symbol:
        return jsonify({'error': 'symbol is required'}), 400

    key = f'drawings:{user_id}:{symbol}'
    
    # Log existing data before update
    try:
        existing_data = r.get(key)
    except Exception as e:
        app.logger.error("Failed to read existing Redis data: user=%s symbol=%s key=%s error=%s", user_id, symbol, key, e, exc_info=True)
    
    try:
        shapes_json = json.dumps(shapes)
        app.logger.info(
            "Writing to Redis - user=%s symbol=%s key=%s data_length=%s",
            user_id, symbol, key, len(shapes_json)
        )
        
        # Perform the Redis set operation
        redis_result = r.set(key, shapes_json)
        
        app.logger.info(
            "Redis set result - user=%s symbol=%s key=%s result=%s",
            user_id, symbol, key, redis_result
        )
        
        # Verify the data was actually saved
        verification = r.get(key)
        app.logger.info(
            "Redis verification - user=%s symbol=%s key=%s verification_success=%s data_match=%s",
            user_id, symbol, key, verification is not None,
            verification == shapes_json if verification else False
        )
        
        if verification:
            verification_count = len(json.loads(verification))
            app.logger.info(
                "Redis verification count - user=%s symbol=%s verified_shapes_count=%s",
                user_id, symbol, verification_count
            )
        
        app.logger.info(
            "Drawings saved successfully - user=%s symbol=%s shapes_count=%s",
            user_id, symbol, len(shapes)
        )
        return jsonify({'status': 'success'})
    except Exception as e:
        app.logger.error("Failed to save drawings: user=%s symbol=%s key=%s error=%s", user_id, symbol, key, e, exc_info=True)
        app.logger.exception("Full exception details:")
        return jsonify({'error': str(e)}), 500


def emit_progress(
    user_email: Optional[str],
    load_id: Any,
    symbol: str,
    interval: str,
    stage: str,
    progress: float,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Shared helper to emit a Socket.IO progress message to a specific user.

    Parameters
    ----------
    user_email:
        Email identifying the user's personal Socket.IO room (from cookie).
    load_id:
        Identifier for the logical load operation (e.g. for cancellation).
    symbol, interval:
        Context of the data request (used for debugging on the client).
    stage:
        Logical stage label (e.g. 'start', 'redis_read', 'complete', 'error').
    progress:
        Numeric progress in [0, 100].
    extra:
        Optional dict with additional fields to attach to the payload.
    """
    if not user_email:
        return

    # If this load has been cancelled for this user, do not emit further updates.
    if load_cancel_flags.get(user_email, {}).get(load_id, False):
        return

    try:
        rounded = round(float(progress), 0)
    except Exception:
        rounded = 0.0

    payload: Dict[str, Any] = {
        "type": "data_progress",
        "symbol": symbol,
        "interval": interval,
        "stage": stage,
        "progress": rounded,
        "loadId": load_id,
    }
    if extra:
        payload.update(extra)

    try:
        socketio.emit("progress", payload, to=user_email)
    except Exception as e:
        app.logger.exception("Failed to emit progress update: %s", e)


def get_volume_profile(
    symbol: str,
    interval: str,
    user_email: Optional[str],
    start_time: Optional[int],
    end_time: Optional[int],
    limit: int,
    klines: Optional[Dict[str, Any]] = None,
    progress_callback: Optional[callable] = None,
):
    """
    Shared helper to compute rectangle volume profiles.

    If `klines` is provided, it must be a dict with keys:
        "time", "open", "high", "low", "close", "volume"
    and those arrays will be used directly.

    If `klines` is None, OHLCV candles are fetched from the Redis ZSET
    storage using the same window / latest-N conventions as /data.
    Returns:
        (times, opens, highs, lows, closes, volumes, rect_volume_profiles)
    """
    # --- Step 1: resolve OHLCV series (either from provided klines or Redis) ---
    if klines is not None:
        times = list(klines.get("time") or [])
        opens = list(klines.get("open") or [])
        highs = list(klines.get("high") or [])
        lows = list(klines.get("low") or [])
        closes = list(klines.get("close") or [])
        volumes = list(klines.get("volume") or [])
    else:
        zset_key = get_klines_zset_key(symbol, interval)

        if progress_callback:
            progress_callback("start", 0.0, {"message": "Starting volume profile computation"})

        now_ms = int(time.time() * 1000)
        interval_ms = get_timeframe_seconds(interval) * 1000

        # Determine window or latest-N mode
        window_start: Optional[int] = None
        window_end: Optional[int] = None

        if start_time is not None or end_time is not None:
            if start_time is not None and end_time is not None:
                window_start = start_time
                window_end = end_time
            elif start_time is not None:
                window_start = start_time
                window_end = now_ms
            else:
                window_end = end_time
                window_start = end_time - limit * interval_ms
                if window_start < 0:
                    window_start = 0
        else:
            # latest-N mode
            window_start = None
            window_end = None

        redis_total = 0
        if window_start is not None and window_end is not None:
            redis_total = int(r.zcount(zset_key, window_start, window_end))
        elif window_start is None and window_end is None:
            zcard = int(r.zcard(zset_key))
            redis_total = min(zcard, int(limit))

        times: List[int] = []
        opens: List[float] = []
        highs: List[float] = []
        lows: List[float] = []
        closes: List[float] = []
        volumes: List[float] = []
        members = []
    
        try:
            if window_start is None or window_end is None:
                # Latest-N mode: use ZREVRANGE and reverse to chronological order
                if redis_total > 0:
                    members = r.zrevrange(zset_key, 0, redis_total - 1)
                    members = list(reversed(members))
                else:
                    members = []
            else:
                # Windowed mode: stream the full range [window_start, window_end]
                members = r.zrangebyscore(zset_key, window_start, window_end)
        except Exception as exc:
            logging.getLogger(__name__).exception(
                "Failed to fetch klines from Redis for volume profile: "
                "symbol=%s interval=%s start=%s end=%s limit=%s error=%s",
                symbol,
                interval,
                start_time,
                end_time,
                limit,
                exc,
            )
            members = []

        for member in members:
            try:
                k = json.loads(member)
                if not k:
                    continue
                t = int(k[0])
                times.append(t)
                opens.append(float(k[1]))
                highs.append(float(k[2]))
                lows.append(float(k[3]))
                closes.append(float(k[4]))
                try:
                    vol = float(k[5])
                except (IndexError, TypeError, ValueError):
                    vol = 0.0
                volumes.append(vol)
            except Exception:
                # Skip malformed entries
                continue

            redis_read = len(times)
            if redis_total > 0:
                progress = (redis_read / float(redis_total)) * 100.0
            else:
                progress = 100.0
            if progress_callback:
                progress_callback("redis_read", progress, {"redis_total": redis_total, "redis_read": redis_read})

    # Normalise candle volumes to match time series length
    if len(volumes) < len(times):
        volumes.extend([0.0] * (len(times) - len(volumes)))
    elif len(volumes) > len(times):
        volumes = volumes[: len(times)]

    rect_volume_profiles: List[Dict[str, Any]] = []

    try:
        if user_email and times:
            drawings_key = f"drawings:{user_email}:{symbol}"
            drawings_raw = r.get(drawings_key)
            if drawings_raw:
                try:
                    user_shapes = json.loads(drawings_raw)
                except Exception:
                    user_shapes = []
            else:
                user_shapes = []

            def _parse_shape_time(value):
                if value is None:
                    return None
                if isinstance(value, (int, float)):
                    # Heuristic: seconds vs milliseconds
                    if value < 10**11:
                        return int(value * 1000)
                    return int(value)
                if isinstance(value, str):
                    v = value.strip()
                    # Try ISO-8601
                    try:
                        if v.endswith("Z"):
                            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                        else:
                            dt = datetime.fromisoformat(v)
                        return int(dt.timestamp() * 1000)
                    except Exception:
                        pass
                    # Fallback: numeric string
                    try:
                        num = float(v)
                        if num < 10**11:
                            return int(num * 1000)
                        return int(num)
                    except Exception:
                        return None
                return None

            # Effective time window for the results
            effective_start = start_time if start_time is not None else times[0]
            effective_end = end_time if end_time is not None else times[-1]

            for idx, shape in enumerate(user_shapes):
                try:
                    s_type = (shape.get("type") or "").lower()
                    if s_type not in ("rect", "rectangle", "box"):
                        continue

                    x0_raw = shape.get("x0")
                    x1_raw = shape.get("x1")
                    x0_ms = _parse_shape_time(x0_raw)
                    x1_ms = _parse_shape_time(x1_raw)
                    if x0_ms is None or x1_ms is None:
                        continue
                    rect_start = min(x0_ms, x1_ms)
                    rect_end = max(x0_ms, x1_ms)

                    # Only consider rectangles that intersect the requested window
                    if rect_end < effective_start or rect_start > effective_end:
                        continue

                    y0_raw = shape.get("y0")
                    y1_raw = shape.get("y1")
                    try:
                        y0f = float(y0_raw)
                        y1f = float(y1_raw)
                    except (TypeError, ValueError):
                        continue
                    price_min = min(y0f, y1f)
                    price_max = max(y0f, y1f)
                    if (
                        not math.isfinite(price_min)
                        or not math.isfinite(price_max)
                        or price_min >= price_max
                    ):
                        continue

                    # Indices of candles inside the rectangle's time span
                    indices = [
                        i for i, t in enumerate(times) if rect_start <= t <= rect_end
                    ]
                    if not indices:
                        continue

                    # Simple volume-by-price histogram using candle typical price
                    num_bins = 20
                    bin_width = (price_max - price_min) / num_bins
                    if bin_width <= 0:
                        continue
                    bin_volumes = [0.0] * num_bins

                    for i in indices:
                        o = opens[i]
                        h = highs[i]
                        l = lows[i]
                        c = closes[i]
                        vol = volumes[i] if i < len(volumes) else 0.0
                        # Typical price approximation
                        price = (o + h + l + c) / 4.0
                        if price < price_min or price > price_max:
                            continue
                        bin_idx = int((price - price_min) / bin_width)
                        if bin_idx >= num_bins:
                            bin_idx = num_bins - 1
                        bin_volumes[bin_idx] += float(vol)

                    levels = []
                    for b_idx, vol in enumerate(bin_volumes):
                        if vol <= 0:
                            continue
                        level_price = price_min + (b_idx + 0.5) * bin_width
                        levels.append(
                            {
                                "price": level_price,
                                "totalVolume": vol,
                            }
                        )

                    if not levels:
                        continue

                    rect_volume_profiles.append(
                        {
                            "shape_index": idx,
                            "x0": x0_ms,
                            "x1": x1_ms,
                            "y0": y0f,
                            "y1": y1f,
                            "volume_profile": levels,
                        }
                    )
                except Exception:
                    logging.getLogger(__name__).exception(
                        "Failed to compute volume profile for rectangle index=%s", idx
                    )
                    continue
    except Exception:
        logging.getLogger(__name__).exception(
            "Error while computing rectangle volume profiles for symbol=%s interval=%s user=%s",
            symbol,
            interval,
            user_email,
        )
        rect_volume_profiles = []

    if progress_callback:
        progress_callback("complete", 100.0, {"message": "Volume profile computation complete"})

    return times, opens, highs, lows, closes, volumes, rect_volume_profiles


@app.route('/volume_profile', methods=['GET', 'POST'])
@log_route
@login_required
def volume_profile():
    """
    Endpoint that returns rectangle-based volume profiles for the current user.

    Query parameters:
        symbol:   trading symbol (default TRADING_SYMBOL)
        interval: timeframe (default first SUPPORTED_RESOLUTIONS)
        startTime, endTime (optional): ms timestamps defining window
        limit (optional): max number of candles when no explicit window
        loadId (optional): identifier for progress tracking

    Request body (optional, JSON):
        {
          "klines": {
             "time": [...],
             "open": [...],
             "high": [...],
             "low": [...],
             "close": [...],
             "volume": [...]
          }
        }
    If klines are provided, they are used directly and no Redis fetch is done.
    """
    symbol = request.args.get('symbol', TRADING_SYMBOL)
    interval = request.args.get('interval', SUPPORTED_RESOLUTIONS[0])
    start_time_str = request.args.get('startTime')
    end_time_str = request.args.get('endTime')
    limit = request.args.get('limit', 500)
    load_id = request.args.get('loadId', 'volume_profile')

    start_time = int(start_time_str) if start_time_str else None
    end_time = int(end_time_str) if end_time_str else None
    limit = int(limit)

    # Validate provided time range
    if start_time is not None and end_time is not None:
        if not (start_time > 0 and end_time > start_time and (end_time - start_time) >= 3600000):
            app.logger.warning(
                f"Invalid time range provided in request: start_time={start_time}, end_time={end_time}, diff={end_time - start_time}. Falling back to defaults."
            )
            start_time = None
            end_time = None

    user_id = get_current_user_id()
    if not user_id:
        return jsonify({"error": "Unauthorized"}), 401

    klines_payload = None
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        if isinstance(payload, dict):
            candidate = payload.get("klines")
            if isinstance(candidate, dict):
                klines_payload = candidate

    def progress_cb(stage, progress, extra=None):
        emit_progress(
            user_email=user_id,
            load_id=load_id,
            symbol=symbol,
            interval=interval,
            stage=stage,
            progress=progress,
            extra=extra or {},
        )

    try:
        (
            times,
            opens,
            highs,
            lows,
            closes,
            volumes,
            rect_volume_profiles,
        ) = get_volume_profile(
            symbol=symbol,
            interval=interval,
            user_email=user_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            klines=klines_payload,
            progress_callback=progress_cb,
        )

        return jsonify(
            {
                "time": times,
                "open": opens,
                "high": highs,
                "low": lows,
                "close": closes,
                "volume": volumes,
                "rect_volume_profiles": rect_volume_profiles,
            }
        )
    except RuntimeError as e:
        # Typically "series not migrated"
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        app.logger.exception(
            "Unexpected error in /volume_profile: symbol=%s interval=%s error=%s",
            symbol,
            interval,
            e,
        )
        return jsonify({"error": str(e)}), 500

@app.route('/data', methods=['GET'])
@log_route
@login_required
def data():
    symbol = request.args.get('symbol', TRADING_SYMBOL)
    interval = request.args.get('interval', SUPPORTED_RESOLUTIONS[0])
    start_time_str = request.args.get('startTime')
    end_time_str = request.args.get('endTime')
    limit = request.args.get('limit', 500)

    # Ensure every /data request has some load_id
    raw_load_id = request.args.get("loadId")
    load_id = raw_load_id if raw_load_id is not None else "background"

    start_time = int(start_time_str) if start_time_str else None
    end_time = int(end_time_str) if end_time_str else None
    limit = int(limit)

    # Identify the current user so we can send per-user progress updates.
    user_id = get_current_user_id()

    # If the client did not send an explicit time range, fall back to the last
    # persisted view range for this user (if available). This keeps the visible
    # window consistent when the user changes resolution but /data is called
    # without startTime/endTime.
    if start_time is None and end_time is None and user_id:
        try:
            vr_key = f"view_range:{user_id}"
            raw = r.get(vr_key)
            if raw:
                vr = json.loads(raw)
                xaxis = vr.get("xaxis") or {}
                rng = xaxis.get("range")
                if isinstance(rng, list) and len(rng) == 2:
                    def _parse_ts(value):
                        if value is None:
                            return None
                        if isinstance(value, (int, float)):
                            # Heuristic: seconds vs milliseconds
                            return int(value * 1000) if value < 10**11 else int(value)
                        if isinstance(value, str):
                            v = value.strip()
                            # Try ISO-8601
                            try:
                                if v.endswith("Z"):
                                    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                                else:
                                    dt = datetime.fromisoformat(v)
                                return int(dt.timestamp() * 1000)
                            except Exception:
                                pass
                            # Fallback: numeric string
                            try:
                                num = float(v)
                                return int(num * 1000) if num < 10**11 else int(num)
                            except Exception:
                                return None
                        return None

                    st = _parse_ts(rng[0])
                    et = _parse_ts(rng[1])
                    if st is not None and et is not None:
                        # Validate the range: must be positive timestamps, st < et, and span at least 1 hour
                        if st > 0 and et > st and (et - st) >= 3600000:
                            start_time = st
                            end_time = et
                        else:
                            app.logger.warning(
                                f"Invalid view_range for user {user_id}: st={st}, et={et}, diff={et-st if et and st else 'N/A'}. Falling back to defaults."
                            )
                            start_time = None
                            end_time = None
            else:
                # No persisted view range, set defaults for new user
                symbol = 'btcusdt'
                interval = '1h'
                now_ms = int(time.time() * 1000)
                ten_days_ms = 10 * 24 * 60 * 60 * 1000
                start_time = now_ms - ten_days_ms
                end_time = now_ms
        except Exception as exc:
            app.logger.warning(
                "Failed to apply view_range fallback in /data for user=%s: %s",
                user_id,
                exc,
            )

    app.logger.info(
        "Starting /data request: symbol=%s, interval=%s, start_time=%s, end_time=%s, "
        "limit=%s, loadId=%s, user=%s",
        symbol,
        interval,
        start_time,
        end_time,
        limit,
        load_id,
        user_id,
    )

    # Add detailed logging for data requests to troubleshoot issues
    # Logging moved after is_latest_mode is defined


    try:
        # --- Step 4: Use ZSET as the source of truth ---
        zset_key = get_klines_zset_key(symbol, interval)

        now_ms = int(time.time() * 1000)
        interval_ms = get_timeframe_seconds(interval) * 1000

        # Define requested window (or latest-N mode)
        window_start = None
        window_end = None
        is_latest_mode = False

        app.logger.info(
            "[DATA_DEBUG] /data request: symbol=%s, interval=%s, start_time=%s, end_time=%s, "
            "limit=%s, user=%s, is_latest_mode=%s",
            symbol,
            interval,
            start_time,
            end_time,
            limit,
            user_id,
            is_latest_mode,
        )

        if start_time is not None or end_time is not None:
            # Time-bounded modes
            if start_time is not None and end_time is not None:
                window_start = start_time
                window_end = end_time
            elif start_time is not None:
                window_start = start_time
                window_end = now_ms
            else:
                # Only end_time provided
                window_end = end_time
                window_start = end_time - limit * interval_ms
                if window_start < 0:
                    app.logger.warning(
                        "Computed window_start < 0 for symbol=%s interval=%s; "
                        "end_time=%s limit=%s interval_ms=%s. Clamping to 0.",
                        symbol,
                        interval,
                        end_time,
                        limit,
                        interval_ms,
                    )
                    window_start = 0
        else:
            # Latest N mode (no explicit time window)
            is_latest_mode = True

        emit_progress(
            user_email=user_id,
            load_id=load_id,
            symbol=symbol,
            interval=interval,
            stage="start",
            progress=0.0,
            extra={
                "message": "Starting data load",
                "mode": "latest" if is_latest_mode else "window",
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit,
            },
        )


        # --- Step 4: Compute Redis totals for the window / latest-N ---
        if not is_latest_mode:
            if window_start is None or window_end is None:
                redis_total = 0
            else:
                # In windowed mode, redis_total reflects the full number of candles
                # available in [window_start, window_end], independent of any
                # client-supplied `limit`. This matches the original pre-ZSET
                # behaviour where limit was ignored when a time range was specified.
                redis_total = int(r.zcount(zset_key, window_start, window_end))
        else:
            zcard = int(r.zcard(zset_key))
            redis_total = min(zcard, int(limit))

        # --- Try aggregation from 1m candles if insufficient direct candles ---
        aggregation_source = None
        
        # Only attempt aggregation if:
        # 1. Requested interval is NOT 1m (no point aggregating 1m to 1m)
        # 2. Redis total is insufficient (< 10 candles) AND interval is not 1m
        # 3. We have an explicit time window
        if (interval != '1m' and redis_total < 10 and window_start is not None and window_end is not None):
            app.logger.info(
                "[AGGREGATION] Insufficient direct %s candles (%d), attempting to aggregate from 1m: symbol=%s window=[%s, %s]",
                interval, redis_total, symbol, window_start, window_end
            )
            
            zset_key_1m = get_klines_zset_key(symbol, '1m')
            redis_total_1m = int(r.zcount(zset_key_1m, window_start, window_end))
            
            if redis_total_1m >= 10:  # Ensure we have enough 1m candles
                app.logger.info(
                    "[AGGREGATION] Found %d 1m candles, aggregating to %s: symbol=%s",
                    redis_total_1m, interval, symbol
                )
                
                try:
                    # Fetch all 1m candles for the window
                    candles_1m_raw = r.zrangebyscore(zset_key_1m, window_start, window_end)
                    candles_1m = []
                    
                    for member in candles_1m_raw:
                        try:
                            k = json.loads(member)
                            candles_1m.append({
                                'time': int(k[0]),
                                'open': float(k[1]),
                                'high': float(k[2]),
                                'low': float(k[3]),
                                'close': float(k[4]),
                                'volume': float(k[5]) if len(k) > 5 else 0.0
                            })
                        except (json.JSONDecodeError, ValueError, IndexError, TypeError):
                            continue
                    
                    # Aggregate to target interval
                    interval_ms = get_timeframe_seconds(interval) * 1000
                    times, opens, highs, lows, closes, volumes = aggregate_1m_candles_to_interval(
                        candles_1m, interval, interval_ms
                    )
                    
                    if times:
                        redis_total = len(times)
                        aggregation_source = 'aggregated_from_1m'
                        app.logger.info(
                            "[AGGREGATION] Successfully aggregated %d 1m candles to %d %s candles for symbol=%s",
                            len(candles_1m), len(times), interval, symbol
                        )
                except Exception as e:
                    app.logger.warning(
                        "[AGGREGATION] Failed to aggregate from 1m: symbol=%s interval=%s error=%s",
                        symbol, interval, e
                    )

        # --- Step 4: Chunked retrieval from Redis (Phase B, 50–100% or 0–100%) ---
        redis_read = 0
        if not aggregation_source:  # Only fetch if we didn't already aggregate
            times = []
            opens = []
            highs = []
            lows = []
            closes = []
            volumes = []

        if not is_latest_mode and not aggregation_source:
            # Time-bounded window: always stream the full range [window_start, window_end]
            # in chronological order, ignoring `limit` for the number of returned candles.
            # (Skip this if we already aggregated from 1m)
            if window_start is not None and window_end is not None and redis_total > 0:
                # Windowed full-range: use forward ZRANGEBYSCORE in chunks
                chunk_size = 2000
                current_min = window_start

                while True:
                    if load_cancel_flags.get(user_id, {}).get(load_id, False):
                        app.logger.info(
                            "Load %s for user %s cancelled during Redis range "
                            "scan; aborting.",
                            load_id,
                            user_id,
                        )
                        return jsonify(
                            {
                                "time": [],
                                "open": [],
                                "high": [],
                                "low": [],
                                "close": [],
                            }
                        )

                    members = r.zrangebyscore(
                        zset_key,
                        current_min,
                        window_end,
                        start=0,
                        num=chunk_size,
                    )

                    if not members:
                        break

                    last_time_in_chunk = None
                    for member in members:
                        try:
                            k = json.loads(member)
                        except Exception:
                            continue
                        if not k:
                            continue
                        try:
                            t = int(k[0])
                        except Exception:
                            continue
                        if t < window_start or t > window_end:
                            continue

                        last_time_in_chunk = t
                        times.append(t)
                        opens.append(float(k[1]))
                        highs.append(float(k[2]))
                        lows.append(float(k[3]))
                        closes.append(float(k[4]))
                        # Volume (Binance kline index 5)
                        try:
                            vol = float(k[5])
                        except (IndexError, TypeError, ValueError):
                            vol = 0.0
                        volumes.append(vol)

                    if last_time_in_chunk is None:
                        break

                    current_min = last_time_in_chunk + 1

                    # redis_read reflects how many candles we are actually returning
                    redis_read = len(times)

                    if redis_total > 0:
                        progress_redis = min(1.0, redis_read / float(redis_total))
                    else:
                        progress_redis = 1.0

                    progress = progress_redis * 100.0

                    if ((round(progress, 1)*10) % 10) == 0 :
                        emit_progress(
                            user_email=user_id,
                            load_id=load_id,
                            symbol=symbol,
                            interval=interval,
                            stage="redis_read",
                            progress=progress,
                            extra={
                                "redis_total": redis_total,
                                "redis_read": redis_read,
                            },
                        )
                    if (round(progress, 1) *10 % 10) == 0 :        
                        app.logger.info("Progress: %s",progress)

                    if redis_total and redis_read >= redis_total:
                        # Safety: stop once we've streamed the expected total
                        break
        else:
            # Latest N mode: use ZREVRANGE, then reverse to chronological order
            if redis_total > 0:
                if load_cancel_flags.get(user_id, {}).get(load_id, False):
                    app.logger.info(
                        "Load %s for user %s cancelled before Redis latest-N "
                        "read; aborting.",
                        load_id,
                        user_id,
                    )
                    return jsonify(
                        {
                            "time": [],
                            "open": [],
                            "high": [],
                            "low": [],
                            "close": [],
                        }
                    )

                members = r.zrevrange(zset_key, 0, redis_total - 1)
                members = list(reversed(members))

                for member in members:
                    try:
                        k = json.loads(member)
                    except Exception:
                        continue
                    if not k:
                        continue
                    try:
                        t = int(k[0])
                    except Exception:
                        continue

                    times.append(t)
                    opens.append(float(k[1]))
                    highs.append(float(k[2]))
                    lows.append(float(k[3]))
                    closes.append(float(k[4]))
                    # Volume (Binance kline index 5)
                    try:
                        vol = float(k[5])
                    except (IndexError, TypeError, ValueError):
                        vol = 0.0
                    volumes.append(vol)

                # redis_read reflects the number of candles we actually return
                redis_read = len(times)

                if redis_total > 0:
                    progress_redis = min(1.0, redis_read / float(redis_total))
                else:
                    progress_redis = 1.0

                progress = progress_redis * 100.0

                if progress == int(progress):
                    emit_progress(
                        user_email=user_id,
                        load_id=load_id,
                        symbol=symbol,
                        interval=interval,
                        stage="redis_read",
                        progress=progress,
                        extra={
                            "redis_total": redis_total,
                            "redis_read": redis_read,
                        },
                    )

        # Final discrepancy logging based on ZSET-derived arrays
        if times:
            returned_start = times[0]
            returned_end = times[-1]
            returned_count = len(times)
            print(
                f"Requested (ZSET): symbol={symbol}, interval={interval}, "
                f"start_time={start_time}, end_time={end_time}, limit={limit}, "
                f"is_latest_mode={is_latest_mode}"
            )
            print(
                f"Returned (ZSET range): start={returned_start}, end={returned_end}, "
                f"count={returned_count}"
            )
            if start_time and returned_start > start_time:
                print(
                    f"Discrepancy: Requested start {start_time}, "
                    f"returned start {returned_start}"
                )
            if end_time and returned_end < end_time:
                print(
                    f"Discrepancy: Requested end {end_time}, "
                    f"returned end {returned_end}"
                )
            if not is_latest_mode and limit and returned_count < limit:
                print(
                    f"Discrepancy: Requested limit {limit}, "
                    f"returned {returned_count}"
                )

            # Analyze timestamps for duplicates before sending to client
            duplicate_analysis = analyze_timestamps(times, symbol, interval, user_id)
            if duplicate_analysis['hasDuplicates'] and duplicate_analysis['duplicateCount'] > 10:
                app.logger.warning(
                    "Significant duplicates detected in /data response: total=%d, unique=%d, duplicates=%d",
                    duplicate_analysis['total'], duplicate_analysis['unique'], duplicate_analysis['duplicateCount']
                )
                app.logger.warning("Duplicate timestamps: %s", duplicate_analysis['duplicates'][:10])  # Log first 10
                if duplicate_analysis['patterns']:
                    app.logger.warning("Consecutive duplicate patterns: %s", duplicate_analysis['patterns'])
            elif duplicate_analysis['hasDuplicates']:
                app.logger.debug(
                    "Minor duplicates detected in /data response: total=%d, unique=%d, duplicates=%d",
                    duplicate_analysis['total'], duplicate_analysis['unique'], duplicate_analysis['duplicateCount']
                )

            # Check candle continuity: close[i] should be approximately equal to open[i+1]
            # Allow small tolerance to account for normal market microstructure (avoid flagging normal price movement)
            continuity_violations = []
            CONTINUITY_TOLERANCE = 0.1  # Allow 10 cents difference for BTC to avoid flagging normal market behavior

            for i in range(len(closes) - 1):
                try:
                    close_price = closes[i]
                    next_open_price = opens[i + 1]
                    # Only flag significant differences that might indicate data corruption
                    if abs(close_price - next_open_price) > CONTINUITY_TOLERANCE:
                        continuity_violations.append((times[i], close_price, times[i+1], next_open_price))
                except (IndexError, TypeError, ValueError):
                    continue
            if continuity_violations:
                app.logger.warning(
                    "Significant candle continuity violations in /data response (potential data corruption): symbol=%s interval=%s count=%d, tolerance=%s",
                    symbol, interval, len(continuity_violations), CONTINUITY_TOLERANCE
                )
                app.logger.warning("First few continuity violations: %s", continuity_violations[:10])
            else:
                # Log at debug level when there are no significant violations (verification successful)
                if continuity_violations == [] and len(closes) > 1:
                    app.logger.debug(
                        "Candle continuity verified: symbol=%s interval=%s, candles=%d, max_diff_within_tolerance=%s",
                        symbol, interval, len(closes), CONTINUITY_TOLERANCE
                    )
        else:
            print("No data returned from ZSET")

        # --- Step 5: Compute per-rectangle volume profiles for this user ---
        try:
            klines_dict: Dict[str, Any] = {
                "time": times,
                "open": opens,
                "high": highs,
                "low": lows,
                "close": closes,
                "volume": volumes,
            }
            
            (
                times,
                opens,
                highs,
                lows,
                closes,
                volumes,
                rect_volume_profiles,
            ) = get_volume_profile(
                symbol=symbol,
                interval=interval,
                user_email=user_id,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
                klines=klines_dict,
            )
        except Exception:
            app.logger.exception(
                "Error while computing rectangle volume profiles for symbol=%s interval=%s user=%s",
                symbol,
                interval,
                user_id,
            )
            rect_volume_profiles = []

        emit_progress(
            user_email=user_id,
            load_id=load_id,
            symbol=symbol,
            interval=interval,
            stage="complete",
            progress=100.0,
            extra={
                "message": "Data load complete",
                "redis_total": redis_total,
                "redis_read": redis_read,
            },
        )

        app.logger.info(
            "Completed /data request: symbol=%s, interval=%s, returned_count=%d, "
            "loadId=%s, user=%s, redis_total=%s, redis_read=%s",
            symbol,
            interval,
            len(times),
            load_id,
            user_id,
            redis_total,
            redis_read,
        )

        # Add detailed logging for data responses to troubleshoot issues
        app.logger.info(
            "[DATA_DEBUG] /data response: symbol=%s, returned_count=%d, "
            "time_range=[%s to %s], first_close=%s, last_close=%s, "
            "rect_volume_profiles_count=%d",
            symbol,
            len(times),
            times[0] if times else 'none',
            times[-1] if times else 'none',
            closes[0] if closes else 'none',
            closes[-1] if closes else 'none',
            len(rect_volume_profiles),
        )

        return jsonify(
            {
                "time": times,
                "open": opens,
                "high": highs,
                "low": lows,
                "close": closes,
                "volume": volumes,
                "rect_volume_profiles": rect_volume_profiles,
                "metadata": {
                    "source": aggregation_source if aggregation_source else "direct",
                    "interval": interval,
                    "candle_count": len(times)
                }
            }
        )
    except Exception as e:
        emit_progress(
            user_email=user_id,
            load_id=load_id,
            symbol=symbol,
            interval=interval,
            stage="error",
            progress=100.0,
            extra={"message": f"Data load failed: {e}"},
        )
        return jsonify({"error": str(e)}), 500
    finally:
        if user_id and load_id in load_cancel_flags.get(user_id, {}):
            del load_cancel_flags[user_id][load_id]

@app.route('/start-login', methods=['POST'])
@log_route
def start_login():
    """
    Begin an email-based login by issuing a one-time magic link.

    Expects JSON: { "email": "user@example.com" }
    """
    payload = request.get_json(silent=True) or {}
    email = (payload.get("email") or "").strip().lower()

    if not email or "@" not in email or len(email) > 255:
        return jsonify({"error": "Invalid email address"}), 400

    # Very simple per-email rate limiting (max ~20 requests per hour).
    try:
        now = int(time.time())
        window = datetime.utcfromtimestamp(now).strftime("%Y%m%d%H")
        rate_key = f"login_email_count:{email}:{window}"
        count = r.incr(rate_key)
        if count == 1:
            r.expire(rate_key, 3600)
        if count > 20:
            return jsonify({"error": "Too many login attempts, please try again later."}), 429
    except Exception:
        app.logger.exception("Failed to apply login rate limit for email=%s", email)

    token = secrets.token_urlsafe(32)
    token_key = f"magic_token:{token}"
    now_ts = int(time.time())
    token_payload = json.dumps(
        {"email": email, "created": now_ts},
        separators=(",", ":"),
    )

    try:
        # Token valid for 15 minutes
        r.set(token_key, token_payload, ex=900)
    except Exception:
        app.logger.exception("Failed to store magic login token for email=%s", email)
        return jsonify({"error": "Internal error"}), 500

    # Build confirmation URL based on current request root
    confirm_url = request.url_root.rstrip("/") + "/confirm-email?token=" + token

    try:
        msg = Message("Your login link", recipients=[email])
        msg.body = (
            f"Click the link below to sign in:\n\n{confirm_url}\n\n"
            "This link will expire in 15 minutes. If you did not request this, you can ignore this email."
        )
        mail.send(msg)
    except Exception:
        app.logger.exception("Failed to send magic login email to %s", email)

    return jsonify({"status": "ok"})


@app.route('/guest-login', methods=['POST'])
@log_route
def guest_login():
    """
    Create an anonymous guest session for users who do not want to enter an email.

    Guest users get a per-browser random identifier and are isolated from each
    other at the data level.
    """
    session.clear()
    guest_id = f"guest:{uuid.uuid4().hex}"
    session["guest_id"] = guest_id
    session["is_guest"] = True
    return jsonify({"status": "ok", "guest_id": guest_id})


@app.route('/logout', methods=['POST'])
@log_route
def logout():
    """
    Clear the current session (email or guest) and log the user out.
    """
    session.clear()
    return jsonify({"status": "ok"})


@app.route('/me', methods=['GET'])
@log_route
def me():
    """
    Return information about the current authenticated/guest user so that the
    frontend does not need to read authentication cookies directly.
    """
    user_email = session.get("user_email")
    guest_id = session.get("guest_id")

    if user_email:
        return jsonify(
            {
                "authenticated": True,
                "email": user_email,
                "is_guest": False,
            }
        )

    if guest_id:
        return jsonify(
            {
                "authenticated": True,
                "email": None,
                "is_guest": True,
            }
        )

    return jsonify({"authenticated": False}), 401


@app.route('/confirm-email', methods=['GET'])
@log_route
def confirm_email():
    """
    Complete magic-link login.

    Expects a single-use, time-limited token in the query string:
        /confirm-email?token=...

    If the token is valid, creates a logged-in session for the associated email
    and redirects the user to the main index page.
    """
    token = (request.args.get("token") or "").strip()
    if not token:
        return make_response("Invalid or expired link", 400)

    token_key = f"magic_token:{token}"
    try:
        raw = r.get(token_key)
        if not raw:
            return make_response("Invalid or expired link", 400)

        r.delete(token_key)
        data = json.loads(raw)
        email = (data.get("email") or "").strip().lower()
        if not email:
            return make_response("Invalid or expired link", 400)
    except Exception:
        app.logger.exception("Failed to validate magic login token")
        return make_response("Invalid or expired link", 400)

    # Establish a fresh logged-in session for this email.
    session.clear()
    session["user_email"] = email
    session["is_guest"] = False

    return redirect(url_for('index'))

def get_simple_trend_analysis(prices, symbol, resolution, all_closes):
    """Helper function for simple trend analysis"""
    if len(prices) < 2:
        return f"Insufficient data for {symbol}. Need more data points."

    avg_recent = sum(prices) / len(prices)
    first_half = prices[:len(prices)//2]
    second_half = prices[len(prices)//2:]
    avg_first = sum(first_half) / len(first_half)
    avg_second = sum(second_half) / len(second_half)

    if avg_second > avg_first * 1.005:  # 0.5% increase
        suggestion = "BUY"
        reason = f"Recent prices show upward trend (avg {avg_first:.4f} to {avg_second:.4f})."
    elif avg_second < avg_first * 0.995:  # 0.5% decrease
        suggestion = "SELL"
        reason = f"Recent prices show downward trend (avg {avg_first:.4f} to {avg_second:.4f})."
    else:
        suggestion = "HOLD"
        reason = f"Prices are relatively stable around {avg_recent:.4f}."

    return f"For {symbol} ({resolution}): {suggestion}. {reason} Data points analyzed: {len(all_closes)}."


@app.route('/AI_Local_OLLAMA_Models', methods=['GET'])
@log_route
@login_required
def get_local_ollama_models():
    """
    Return list of available local Ollama models.
    """
    try:
        # Query LM Studio API for available models
        lm_studio_url = "http://localhost:1234/v1/models"
        response = requests.get(lm_studio_url, timeout=10)
        if response.status_code == 200:
            result = response.json()
            models = [model.get("id", "") for model in result.get("data", []) if model.get("id")]
            if models:
                return jsonify({"models": models})
        # Fallback to example models if API call fails
        app.logger.warning(f"Failed to fetch models from LM Studio: {response.status_code}")
        models = ["llama2", "codellama", "mistral"]  # Example models
        return jsonify({"models": models})
    except Exception as e:
        app.logger.exception("Error fetching local Ollama models")
        # Return example models as fallback
        models = ["llama2", "codellama", "mistral"]  # Example models
        return jsonify({"models": models})


@app.route('/AI', methods=['POST'])
@log_route
@login_required
def ai_suggestion():
    """
    Generate AI trading suggestion based on chart data and indicators.
    """
    try:
        app.logger.info("Received /AI request")

        payload = request.get_json(silent=True) or {}
        symbol = payload.get('symbol')
        resolution = payload.get('resolution')
        x_axis_min = payload.get('xAxisMin')
        x_axis_max = payload.get('xAxisMax')
        active_indicator_ids = payload.get('activeIndicatorIds', [])
        question = payload.get('question', "Based on the provided market data, what is your trading suggestion (BUY, SELL, or HOLD) and why?")
        use_local_ollama = payload.get('use_local_ollama', False)
        local_ollama_model_name = payload.get('local_ollama_model_name') if payload.get('local_ollama_model_name') else "openai/gpt-oss-20b"

        user_id = get_current_user_id()

        if not symbol or not resolution:
            return jsonify({"error": "Symbol and resolution are required"}), 400

        # Fetch chart data for the time range from Redis
        zset_key = get_klines_zset_key(symbol, resolution)
        times = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []

        # Convert x_axis_min and x_axis_max from seconds to milliseconds
        start_time_ms = x_axis_min * 1000 if x_axis_min else None
        end_time_ms = x_axis_max * 1000 if x_axis_max else None

        # Log human readable datetimes
        if start_time_ms:
            start_readable = datetime.fromtimestamp(start_time_ms / 1000, tz=timezone.utc).isoformat()
            app.logger.info(f"Start time: {start_readable}")
        if end_time_ms:
            end_readable = datetime.fromtimestamp(end_time_ms / 1000, tz=timezone.utc).isoformat()
            app.logger.info(f"End time: {end_readable}")

        data = {}

        if start_time_ms and end_time_ms:
            members = r.zrangebyscore(zset_key, start_time_ms, end_time_ms)
            for member in members:
                try:
                    k = json.loads(member)
                    times.append(int(k[0]))
                    opens.append(float(k[1]))
                    highs.append(float(k[2]))
                    lows.append(float(k[3]))
                    closes.append(float(k[4]))
                    volumes.append(float(k[5]) if len(k) > 5 else 0.0)
                except Exception:
                    continue
            # Limit to last 200 data points
            if len(closes) > 200:
                opens = opens[-200:]
                highs = highs[-200:]
                lows = lows[-200:]
                closes = closes[-200:]
                volumes = volumes[-200:]
                times = times[-200:]
            # Compute indicators
            if closes:
                df = pd.DataFrame({'open': opens, 'high': highs, 'low': lows, 'close': closes, 'volume': volumes})
                ema20 = ta.ema(df['close'], length=20)
                macd_df = ta.macd(df['close'])
                rsi7 = ta.rsi(df['close'], length=7)
                atr3 = ta.atr(df['high'], df['low'], df['close'], length=3)
                atr14 = ta.atr(df['high'], df['low'], df['close'], length=14)
                ema50 = ta.ema(df['close'], length=50)
                # Build the data structure
                symbol_data = {
                    "current_price": closes[-1],
                    "current_ema20": ema20.iloc[-1] if len(ema20) > 0 else None,
                    "current_macd": macd_df['MACD_12_26_9'].iloc[-1] if 'MACD_12_26_9' in macd_df.columns else None,
                    "current_rsi": rsi7.iloc[-1] if len(rsi7) > 0 else None,
                    "open_interest_latest": None,
                    "open_interest_avg": None,
                    "funding_rate": None,
                    "intraday_prices": closes,
                    "ema_20_series": ema20.tolist() if len(ema20) > 0 else [],
                    "macd_value_series": macd_df['MACD_12_26_9'].tolist() if 'MACD_12_26_9' in macd_df.columns else [],
                    "macd_signal_series": macd_df['MACDs_12_26_9'].tolist() if 'MACDs_12_26_9' in macd_df.columns else [],
                    "rsi_7_series": rsi7.tolist() if len(rsi7) > 0 else [],
                    "long_term_ema_20": str(ema20.iloc[-1]) if len(ema20) > 0 else "0",
                    "long_term_ema_50": str(ema50.iloc[-1]) if len(ema50) > 0 else "0",
                    "atr_3_period": str(atr3.iloc[-1]) if len(atr3) > 0 else "0",
                    "atr_14_period": str(atr14.iloc[-1]) if len(atr14) > 0 else "0",
                    "top_10_buy_liquidations": [],
                    "top_10_sell_liquidations": []
                }
                data = {symbol: symbol_data}
            else:
                data = {}

        # Check if using local Ollama/LM Studio
        if use_local_ollama and local_ollama_model_name:
            try:
                # Prepare market data for AI prompt
                recent_prices = data.get(symbol, {}).get('intraday_prices', closes)[-50:] if data.get(symbol, {}).get('intraday_prices') else closes[-50:] if len(closes) >= 50 else closes  # Last 50 data points
                if len(recent_prices) < 5:
                    response_text = f"Insufficient data for {symbol}. Need more data points."
                    return jsonify({"response": response_text})
                else:
                    # Create prompt with market data in JSON format
                    data_json = json.dumps(data.get(symbol, {}), indent=2)
                    prompt = f"{data_json}\n\n{question}\n\nPlease provide a clear BUY, SELL, or HOLD recommendation with your reasoning."
                    with open('llm_user_prompt.txt', 'w') as f:
                        f.write(prompt)

                    # Call LM Studio API (OpenAI compatible)
                    lm_studio_url = "http://localhost:1234/v1/chat/completions"
                    payload = {
                        "model": local_ollama_model_name,
                        "messages": [
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.7,
                        "max_tokens": 4096,
                        "stream": True
                    }

                    response = requests.post(lm_studio_url, json=payload, timeout=360, stream=True)
                    response.raise_for_status()

                    full_content = ""
                    token_count = 0
                    chunk_id = 0
                    max_tokens = 4096

                    # Emit progress start
                    socketio.emit("progress", {
                        "type": "ai_progress",
                        "stage": "generating",
                        "progress": 10,
                        "message": "AI generating response..."
                    }, to=user_id)

                    for line in response.iter_lines():
                        if line:
                            line = line.decode('utf-8')
                            if line.startswith('data: '):
                                data = line[6:]
                                if data == '[DONE]':
                                    break
                                try:
                                    chunk = json.loads(data)
                                    delta = chunk.get("choices", [{}])[0].get("delta", {})
                                    content = delta.get("content", "")
                                    if content:
                                        full_content += content
                                        token_count += len(content.split())
                                        socketio.emit("ai_response", {
                                            "partial": content,
                                            "chunk_id": chunk_id,
                                            "complete": False
                                        }, to=user_id)
                                        chunk_id += 1
                                        # Update progress
                                        progress_pct = min(90, 10 + (token_count / max_tokens * 80))
                                        socketio.emit("progress", {
                                            "type": "ai_progress",
                                            "stage": "generating",
                                            "progress": progress_pct,
                                            "message": f"AI generating... {token_count} tokens"
                                        }, to=user_id)
                                except json.JSONDecodeError:
                                    continue

                    # Emit final complete response in chunks to avoid truncation
                    final_response = f"AI Analysis for {symbol} ({resolution}):\n{full_content}\n\nData points analyzed: {len(recent_prices)}."
                    chunk_size = 200
                    for i in range(0, len(final_response), chunk_size):
                        chunk = final_response[i:i + chunk_size]
                        socketio.emit("ai_response", {
                            "partial": chunk,
                            "chunk_id": chunk_id,
                            "is_final": True,
                            "complete": False
                        }, to=user_id)
                        chunk_id += 1

                    socketio.emit("ai_response", {
                        "complete": True
                    }, to=user_id)
                    # Emit progress complete
                    socketio.emit("progress", {
                        "type": "ai_progress",
                        "stage": "complete",
                        "progress": 100,
                        "message": "AI response complete"
                    }, to=user_id)
                    return jsonify({"status": "streaming_started"})

            except requests.exceptions.RequestException as e:
                app.logger.warning(f"Failed to connect to LM Studio: {e}")
                response_text = f"AI service unavailable. " + get_simple_trend_analysis(closes[-20:] if len(closes) >= 20 else closes, symbol, resolution, closes)
                return jsonify({"response": response_text})
            except Exception as e:
                app.logger.error(f"Error calling local AI: {e}", exc_info=True)
                response_text = f"AI analysis failed. " + get_simple_trend_analysis(closes[-20:] if len(closes) >= 20 else closes, symbol, resolution, closes)
                return jsonify({"response": response_text})
        else:
            # Use simple trend analysis
            recent_closes = data.get(symbol, {}).get('intraday_prices', closes)
            response_text = get_simple_trend_analysis(recent_closes[-20:] if len(recent_closes) >= 20 else recent_closes, symbol, resolution, recent_closes)
            return jsonify({"response": response_text})

    except Exception as e:
        app.logger.exception("Error in AI suggestion")
        return jsonify({"error": str(e)}), 500


@app.route('/', methods=['GET'])
@log_route
def index():
    return render_template(
        'index.html',
        supported_resolutions=SUPPORTED_RESOLUTIONS,
        supported_symbols=SUPPORTED_SYMBOLS
    )

# Audio transcription endpoint
@app.route('/transcribe_audio', methods=['POST'])
@log_route
@login_required
def transcribe_audio():
    """
    Transcribe audio file to text using Whisper.
    Accepts audio files and returns transcribed text.
    Requires user authentication.
    """
    try:
        # Validate file type
        allowed_extensions = ['.wav', '.mp3', '.m4a', '.flac', '.ogg', '.webm']
        audio_file = request.files.get('audio_file')

        if not audio_file:
            return jsonify({"error": "No audio file provided"}), 400

        file_extension = os.path.splitext(audio_file.filename)[1].lower()

        if file_extension not in allowed_extensions:
            return jsonify({"error": f"Unsupported file type. Allowed types: {', '.join(allowed_extensions)}"}), 400

        # Read audio file content
        audio_content = audio_file.read()

        # Log file details for debugging
        app.logger.info(f"Audio file received: {audio_file.filename}, size: {len(audio_content)} bytes, type: {file_extension}")

        # Special handling for WebM files - convert to WAV for Whisper compatibility
        if file_extension == '.webm':
            try:
                from pydub import AudioSegment
                app.logger.info("Converting WebM to WAV for Whisper compatibility")

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
                app.logger.info("WebM to WAV conversion completed")

            except ImportError:
                app.logger.error("pydub not available for WebM conversion - install with: pip install pydub", exc_info=True)
                return jsonify({"error": "WebM conversion not available. Please use WAV, MP3, or other supported formats."}), 500
            except Exception as e:
                app.logger.error(f"WebM conversion failed: {e}", exc_info=True)
                return jsonify({"error": f"Audio conversion failed: {str(e)}"}), 500

        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
            temp_file.write(audio_content)
            temp_file_path = temp_file.name

        try:
            # Check if Whisper model is preloaded
            model = getattr(app, 'whisper_model', None)
            if model is None:
                app.logger.error("Whisper model not available - failed to load at startup", exc_info=True)
                return jsonify({"error": "Audio transcription service is temporarily unavailable. Please try again later."}), 503

            # Get language parameter from request (optional)
            requested_language = request.form.get('language', '').strip().lower()
            if requested_language and requested_language not in ['en', 'sl']:
                return jsonify({"error": "Invalid language. Supported languages: en (English), sl (Slovenian)"}), 400

            app.logger.info("Transcribing audio file using preloaded model")
            # If language is specified, force Whisper to use it
            if requested_language:
                app.logger.info(f"Forcing transcription language to: {requested_language}")
                result = model.transcribe(temp_file_path, language=requested_language)
            else:
                result = model.transcribe(temp_file_path)

            # Check detected/forced language - only allow English and Slovenian
            detected_language = result.get("language", "unknown")
            allowed_languages = ["en", "sl"]
            if detected_language not in allowed_languages:
                app.logger.warning(f"Unsupported language detected: {detected_language}. Only English (en) and Slovenian (sl) are supported.")
                return jsonify({
                    "error": f"Unsupported language detected: {detected_language}. Only English and Slovenian audio is supported for transcription."
                }), 400

            # Extract transcribed text
            transcribed_text = result["text"].strip()

            app.logger.info(f"Audio transcription completed. Text length: {len(transcribed_text)}, Language: {detected_language}")

            # Process transcribed text with AI analysis
            symbol = request.form.get('symbol', 'BTCUSDT')
            resolution = request.form.get('resolution', '1h')
            xAxisMin = request.form.get('xAxisMin')
            xAxisMax = request.form.get('xAxisMax')
            activeIndicatorIds = request.form.get('activeIndicatorIds', '[]')
            use_local_ollama = request.form.get('use_local_ollama', 'false').lower() == 'true'
            use_gemini = request.form.get('use_gemini', 'false').lower() == 'true'

            # Parse activeIndicatorIds from JSON string to list
            try:
                parsed_indicators = json.loads(activeIndicatorIds) if activeIndicatorIds else []
            except json.JSONDecodeError:
                parsed_indicators = []

            # Get AI analysis using existing AI endpoint logic
            ai_response = get_answer_from_LLM_from_audio(transcribed_text, symbol, resolution, local_ollama_model_name="openai/gpt-oss-20b", user_id=get_current_user_id())

            return jsonify({
                "status": "success",
                "transcribed_text": transcribed_text,
                "llm_analysis": ai_response,
                "language": result.get("language", "unknown"),
                "confidence": result.get("confidence", 0.0)
            })

        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_file_path)
            except Exception as e:
                app.logger.warning(f"Failed to clean up temporary file: {e}")

    except Exception as e:
        app.logger.exception("Error during audio transcription")
        return jsonify({"error": f"Audio transcription failed: {str(e)}"}), 500


def get_answer_from_LLM_from_audio(question, symbol, resolution, local_ollama_model_name, user_id):
    """Simple AI analysis for audio transcription based on existing AI logic"""
    try:
        app.logger.info("get_simple_response_from_llm for_audio called with question length: %d", len(question))

        # Call LM Studio API
        lm_studio_url = "http://localhost:1234/v1/chat/completions"
        payload = {
            "model": local_ollama_model_name,
            "messages": [
                {"role": "user", "content": question}
            ],
            "temperature": 0.7,
            "max_tokens": 4096,
            "stream": True
        }

        response = requests.post(lm_studio_url, json=payload, timeout=360, stream=True)
        response.raise_for_status()

        full_content = ""
        token_count = 0
        chunk_id = 0
        max_tokens = 4096

        # Emit progress start
        socketio.emit("progress", {
            "type": "ai_progress",
            "stage": "generating",
            "progress": 10,
            "message": "AI generating response..."
        }, to=user_id)

        for line in response.iter_lines():
            if line:
                line = line.decode('utf-8')
                if line.startswith('data: '):
                    data_chunk = line[6:]
                    if data_chunk == '[DONE]':
                        break
                    try:
                        chunk = json.loads(data_chunk)
                        delta = chunk.get("choices", [{}])[0].get("delta", {})
                        content = delta.get("content", "")
                        if content:
                            full_content += content
                            token_count += len(content.split())
                            socketio.emit("ai_response", {
                                "partial": content,
                                "chunk_id": chunk_id,
                                "complete": False
                            }, to=user_id)
                            chunk_id += 1
                            # Update progress
                            progress_pct = min(90, 10 + (token_count / max_tokens * 80))
                            socketio.emit("progress", {
                                "type": "ai_progress",
                                "stage": "generating",
                                "progress": progress_pct,
                                "message": f"AI generating... {token_count} tokens"
                            }, to=user_id)
                    except json.JSONDecodeError:
                        continue

        # Emit final complete response in chunks to avoid truncation
        final_response = full_content
        chunk_size = 200
        for i in range(0, len(final_response), chunk_size):
            chunk = final_response[i:i + chunk_size]
            socketio.emit("ai_response", {
                "partial": chunk,
                "chunk_id": chunk_id,
                "is_final": True,
                "complete": False
            }, to=user_id)
            chunk_id += 1

        socketio.emit("ai_response", {
            "complete": True
        }, to=user_id)
        # Emit progress complete
        socketio.emit("progress", {
            "type": "ai_progress",
            "stage": "complete",
            "progress": 100,
            "message": "AI response complete"
        }, to=user_id)

    except Exception as e:
        app.logger.error(f"Error in AI analysis: {e}", exc_info=True)
        socketio.emit("ai_response", {
            "partial": f"AI analysis failed. Error: {str(e)}",
            "chunk_id": 0,
            "complete": True
        }, to=user_id)


if __name__ == "__main__":
    # Preload Whisper model
    try:
        app.logger.info("PRELOADING WHISPER MODEL: Loading Whisper base model for audio transcription...")
        import torch
        # Force CPU usage and disable CUDA
        original_cuda_check = torch.cuda.is_available
        torch.cuda.is_available = lambda: False
        # Clear any cached models
        if hasattr(whisper, '_models'):
            whisper._models.clear()
        # Load model
        app.logger.info("Loading Whisper base model...")
        app.whisper_model = whisper.load_model("base", device="cpu")
        app.logger.info("WHISPER MODEL LOADED: Whisper base model successfully loaded and cached")
        # Restore original CUDA check
        torch.cuda.is_available = original_cuda_check
        app.logger.info(f"CUDA availability: {torch.cuda.is_available()}")
    except Exception as e:
        app.logger.error(f"FAILED TO LOAD WHISPER MODEL: {e}", exc_info=True)
        app.whisper_model = None

    start_background_fetch()
    
    # Start live price stream from Binance
    def on_live_price(symbol: str, price: float, timestamp: int):
        """Callback to broadcast live prices to all connected clients"""
        try:
            socketio.server.emit('live_price', {
                'symbol': symbol,
                'price': price,
                'timestamp': timestamp
            }, namespace='/')
        except Exception as e:
            app.logger.error(f"Error emitting live price: {e}")
    
    start_live_price_stream(SUPPORTED_SYMBOLS, r, app.logger, on_live_price)
    
    # Use SocketIO's server so WebSocket events (progress, live, etc.) work.
    socketio.run(app, host='0.0.0.0', debug=True)
