#!/usr/bin/env python3
"""
Script to check the actual Redis data for problematic timestamps.
"""

import redis
import json
from datetime import datetime, timezone

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

def get_klines_zset_key(symbol, interval):
    """Build the Redis sorted-set key for a given (symbol, interval) pair."""
    return f"klines_z:{symbol}:{interval}"

def get_timestamps_from_redis(symbol, interval, start_ts, end_ts):
    """Get timestamp-sorted kline data from Redis."""
    zset_key = get_klines_zset_key(symbol, interval)

    # Get all members in range
    members = r.zrangebyscore(zset_key, start_ts, end_ts, withscores=True)

    klines_data = []
    raw_members = []
    for member_bytes, score in members:
        try:
            member_str = member_bytes.decode('utf-8')
            k = json.loads(member_str)
            if k and isinstance(k, list) and len(k) >= 6:
                raw_members.append((score, k))
                klines_data.append({
                    'timestamp': int(k[0]),
                    'open': float(k[1]),
                    'high': float(k[2]),
                    'low': float(k[3]),
                    'close': float(k[4]),
                    'volume': float(k[5])
                })
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            print(f"Error parsing kline: {e}, data: {member_bytes}")
            continue

    # Sort by timestamp
    klines_data.sort(key=lambda x: x['timestamp'])

    # Check for multiple entries with same timestamp
    from collections import defaultdict
    timestamp_counts = defaultdict(list)
    for data in klines_data:
        timestamp_counts[data['timestamp']].append(data)

    duplicates = {ts: entries for ts, entries in timestamp_counts.items() if len(entries) > 1}

    if duplicates:
        print(f"\nFound duplicate timestamps: {len(duplicates)} timestamps have multiple entries")
        for ts, entries in list(duplicates.items())[:3]:  # Show first 3 duplicates
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            print(f"Timestamp {ts} ({dt}): {len(entries)} entries")
            for i, entry in enumerate(entries):
                print(f"  Entry {i}: close={entry['close']:.2f}")
    else:
        print("\nNo duplicate timestamps found")

    return klines_data

def check_continuity_in_redis(klines_data):
    """Check continuity violations in the Redis data."""
    violations = []

    for i in range(len(klines_data) - 1):
        current = klines_data[i]
        next_k = klines_data[i + 1]

        if abs(current['close'] - next_k['open']) > 0.1:  # Use same tolerance
            dt1 = datetime.fromtimestamp(current['timestamp'] / 1000, tz=timezone.utc)
            dt2 = datetime.fromtimestamp(next_k['timestamp'] / 1000, tz=timezone.utc)
            diff = abs(current['close'] - next_k['open'])
            violations.append({
                'index': i,
                'timestamp1': current['timestamp'],
                'timestamp2': next_k['timestamp'],
                'close': current['close'],
                'open': next_k['open'],
                'difference': diff
            })

    return violations

def main():
    SYMBOL = 'BTCUSDT'
    INTERVAL = '1h'

    # Check around the duplicate timestamp
    # Duplicate: 1763658000000
    start_ts = 1763600000000  # ~1 hour before
    end_ts = 1763700000000    # ~1 hour after

    print(f"Checking Redis data for {SYMBOL} {INTERVAL}")
    print(f"Time range: {datetime.fromtimestamp(start_ts/1000, tz=timezone.utc)} to {datetime.fromtimestamp(end_ts/1000, tz=timezone.utc)}")

    klines_data = get_timestamps_from_redis(SYMBOL, INTERVAL, start_ts, end_ts)

    print(f"Found {len(klines_data)} klines in Redis")

    if len(klines_data) < 2:
        print("Not enough data to check continuity")
        return

    # Check timestamps for gaps or irregularities
    expected_interval_ms = 15 * 60 * 1000  # 15 minutes in ms

    gaps = []
    for i in range(len(klines_data) - 1):
        ts1 = klines_data[i]['timestamp']
        ts2 = klines_data[i + 1]['timestamp']

        expected_next = ts1 + expected_interval_ms
        if ts2 != expected_next:
            dt1 = datetime.fromtimestamp(ts1/1000, tz=timezone.utc)
            diff_seconds = (ts2 - expected_next) // 1000
            gaps.append(f"Gap at {dt1}: expected {expected_next}, got {ts2} (diff: {diff_seconds}s)")

    if gaps:
        print(f"\nFound {len(gaps)} timestamp gaps:")
        for gap in gaps[:10]:  # Show first 10
            print(f"  {gap}")
        if len(gaps) > 10:
            print(f"  ... and {len(gaps) - 10} more")
    else:
        print("\nNo timestamp gaps found")

    # Check continuity violations
    continuity_violations = check_continuity_in_redis(klines_data)

    print(f"\nContinuity violations (tolerance 0.1): {len(continuity_violations)}")

    if continuity_violations:
        print("\nFirst 10 violations:")
        for v in continuity_violations[:10]:
            dt1 = datetime.fromtimestamp(v['timestamp1'] / 1000, tz=timezone.utc)
            dt2 = datetime.fromtimestamp(v['timestamp2'] / 1000, tz=timezone.utc)
            print(f"T1={dt1.strftime('%Y-%m-%d %H:%M:%S')} close={v['close']:.2f}, T2={dt2.strftime('%Y-%m-%d %H:%M:%S')} open={v['open']:.2f}, diff={v['difference']:.2f}")
    else:
        print("No continuity violations found")

    # Show a sample of the data around the problematic timestamps
    print("\nData around the first violation:")
    violation_ts = 1763555400000
    for k in klines_data:
        if abs(k['timestamp'] - violation_ts) <= expected_interval_ms * 2:
            dt = datetime.fromtimestamp(k['timestamp'] / 1000, tz=timezone.utc)
            print(f"{dt.strftime('%Y-%m-%d %H:%M:%S')} {k['close']:.2f}")

if __name__ == "__main__":
    main()
