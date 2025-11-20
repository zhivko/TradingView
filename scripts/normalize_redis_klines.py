import redis
import json
import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SUPPORTED_SYMBOLS, SUPPORTED_RESOLUTIONS, get_timeframe_seconds

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

r = redis.Redis(host='localhost', port=6379, db=0)

def normalize_zset(symbol: str, interval: str):
    key = f"klines_z:{symbol}:{interval}"
    interval_ms = get_timeframe_seconds(interval) * 1000

    logger.info(f"Starting normalization of {key} ...")

    try:
        members = r.zrange(key, 0, -1, withscores=True)
    except Exception as e:
        logger.error(f"Failed to read {key}: {e}")
        return

    original_count = len(members)
    if original_count == 0:
        logger.info(f"No entries in {key} - skipping")
        return

    logger.info(f"Processing {original_count} entries in {key} ...")

    normalized = {}
    duplicates_found = 0

    for i, (member, score) in enumerate(members):
        if i % 10000 == 0 or i == original_count - 1:
            percent = (i + 1) * 100.0 / original_count
            logger.info(f"Processing {i+1}/{original_count} entries ({percent:.2f}%)...")

        try:
            kline = json.loads(member)
            ts = int(kline[0])
            rounded_ts = (ts // interval_ms) * interval_ms

            # Update timestamp in kline data
            kline[0] = rounded_ts

            # Check for duplicates
            if rounded_ts in normalized:
                duplicates_found += 1
                # Keep the newer/fresher data by overwriting
                normalized[rounded_ts] = json.dumps(kline, separators=(",", ":"))
            else:
                normalized[rounded_ts] = json.dumps(kline, separators=(",", ":"))

        except Exception as e:
            logger.warning(f"Failed to process member {member[:50]}...: {e}")
            continue

    normalized_count = len(normalized)

    if not normalized:
        logger.warning(f"No valid entries in {key} after processing")
        return

    logger.info(f"Normalization results for {key}:")
    logger.info(f"  Original entries: {original_count}")
    logger.info(f"  Normalized entries: {normalized_count}")
    logger.info(f"  Duplicates removed: {duplicates_found}")

    if normalized_count >= original_count:
        logger.info(f"No changes needed for {key}")
        return

    try:
        logger.info(f"Writing {normalized_count} normalized entries to {key} in chunks...")

        # Delete the old ZSET
        r.delete(key)

        # Write normalized data in chunks
        chunk_size = 10000
        items = list(normalized.items())
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i+chunk_size]
            pipe = r.pipeline()
            for ts, member in chunk:
                pipe.zadd(key, {member: ts})
            pipe.execute()

            chunk_num = (i // chunk_size) + 1
            total_chunks = (len(items) + chunk_size - 1) // chunk_size
            logger.info(f"Written chunk {chunk_num}/{total_chunks} ({len(chunk)} entries)...")

        logger.info(f"Successfully normalized {key}: {original_count} → {normalized_count} entries")

    except Exception as e:
        logger.error(f"Failed to write normalized data to {key}: {e}")

if __name__ == "__main__":
    logger.info("Starting Redis klines normalization script")

    total_symbol_interval_combinations = len(SUPPORTED_SYMBOLS) * len(SUPPORTED_RESOLUTIONS)
    processed = 0

    for symbol in SUPPORTED_SYMBOLS:
        for interval in SUPPORTED_RESOLUTIONS:
            processed += 1
            logger.info(f"Processing {symbol} {interval} ({processed}/{total_symbol_interval_combinations})")
            try:
                normalize_zset(symbol, interval)
            except Exception as e:
                logger.error(f"Unexpected error processing {symbol} {interval}: {e}")

    logger.info("Redis klines normalization completed")
