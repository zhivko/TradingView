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

def remove_duplicates():
    """Remove duplicate timestamps from Redis zsets, keeping the entry with the highest close price."""
    print("Removing duplicates from Redis zsets...")

    total_duplicates_removed = 0
    pairs_processed = 0

    for symbol in SUPPORTED_SYMBOLS:
        for interval in SUPPORTED_RESOLUTIONS:
            zset_key = get_klines_zset_key(symbol, interval)
            pairs_processed += 1
            print(f"Processing {symbol} {interval} for duplicates")

            # Get all members with scores
            members = r.zrangebyscore(zset_key, '-inf', '+inf', withscores=True)

            if not members:
                continue

            # Group by score (timestamp)
            from collections import defaultdict
            by_timestamp = defaultdict(list)
            for member_bytes, score in members:
                try:
                    member_str = member_bytes.decode('utf-8')
                    k = json.loads(member_str)
                    if isinstance(k, list) and len(k) >= 5:
                        by_timestamp[score].append((member_bytes, k))
                except Exception:
                    continue

            duplicates_removed = 0
            for ts, entries in by_timestamp.items():
                if len(entries) > 1:
                    # Keep the one with highest close price
                    entries.sort(key=lambda x: float(x[1][4]), reverse=True)
                    # Remove all except the first (highest close)
                    for member_bytes, _ in entries[1:]:
                        r.zrem(zset_key, member_bytes)
                        duplicates_removed += 1

            if duplicates_removed > 0:
                print(f"Removed {duplicates_removed} duplicates for {symbol} {interval}")

            total_duplicates_removed += duplicates_removed

    print(f"\nDuplicates removal complete:")
    print(f"Pairs processed: {pairs_processed}")
    print(f"Total duplicates removed: {total_duplicates_removed}")

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
            print(f"Processing {symbol} {interval} for cleanup")

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
    remove_duplicates()
    cleanup_corrupted_data()
