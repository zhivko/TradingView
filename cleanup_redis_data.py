#!/usr/bin/env python3
"""
Script to clean up corrupted Redis kline data from 2025-11-16 onward.
"""

import redis
import json
from datetime import datetime, timezone
from config import SUPPORTED_SYMBOLS, SUPPORTED_RESOLUTIONS

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

def get_klines_zset_key(symbol, interval):
    return f"klines_z:{symbol}:{interval}"

def get_klines_meta_key(symbol, interval):
    return f"klines_meta:{symbol}:{interval}"

def cleanup_corrupted_data():
    """Delete all kline data from 2025-11-16 onward for all symbols and intervals."""

    # 2025-11-16 00:00:00 UTC in milliseconds
    cutoff_ts = int(datetime(2025, 11, 16, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    cutoff_date = datetime.fromtimestamp(cutoff_ts / 1000, tz=timezone.utc)

    print(f"Cleaning up data from {cutoff_date} onward (timestamp >= {cutoff_ts})")

    total_keys_deleted = 0
    total_records_removed = 0

    for symbol in SUPPORTED_SYMBOLS:
        for interval in SUPPORTED_RESOLUTIONS:
            zset_key = get_klines_zset_key(symbol, interval)
            meta_key = get_klines_meta_key(symbol, interval)

            # Remove records >= cutoff_ts from zset
            removed_count = r.zremrangebyscore(zset_key, cutoff_ts, '+inf')
            total_records_removed += removed_count

            # Update metadata if needed
            meta_data = r.get(meta_key)
            if meta_data:
                try:
                    meta = json.loads(meta_data.decode('utf-8'))
                    latest_ts = meta.get('latest')
                    if latest_ts and latest_ts >= cutoff_ts:
                        # Find the new latest timestamp in the remaining data
                        remaining = r.zrevrange(zset_key, 0, 0, withscores=True)
                        if remaining:
                            _, new_latest_ts = remaining[0]
                            meta['latest'] = int(new_latest_ts)
                            r.set(meta_key, json.dumps(meta, separators=(',', ':')))
                            print(f"Updated metadata for {symbol} {interval}: latest={new_latest_ts}")
                        else:
                            # No data left, remove metadata too
                            r.delete(meta_key)
                            print(f"Removed empty metadata for {symbol} {interval}")
                except Exception as e:
                    print(f"Error updating metadata for {symbol} {interval}: {e}")

            if removed_count > 0:
                print(f"Removed {removed_count} records from {symbol} {interval}")
                total_keys_deleted += 1

    print("\nSummary:")
    print(f"Pairs processed: {len(SUPPORTED_SYMBOLS)} symbols × {len(SUPPORTED_RESOLUTIONS)} intervals = {len(SUPPORTED_SYMBOLS) * len(SUPPORTED_RESOLUTIONS)}")
    print(f"Keys affected: {total_keys_deleted}")
    print(f"Total records removed: {total_records_removed}")
    print(f"Background fetcher can now refill the gaps from {cutoff_date} onward")

if __name__ == "__main__":
    cleanup_corrupted_data()
