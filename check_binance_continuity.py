#!/usr/bin/env python3
"""
Script to check Binance klines data continuity for specific timestamps.
"""

import requests
import json
import sys
from datetime import datetime, timezone

# Configurations
SYMBOL = 'BTCUSDT'
INTERVAL = '15m'

# Timestamps from the log (ms)
# First few violations:
# (1763516700000, 92439.99, 1763517600000, 92440.0)
# (1763517600000, 92736.35, 1763518500000, 92736.36)
# etc.

def fetch_binance_klines(symbol, interval, start_time=None, end_time=None, limit=100):
    """Fetch klines from Binance API."""
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }

    if start_time:
        params['startTime'] = start_time
    if end_time:
        params['endTime'] = end_time

    try:
        response = requests.get(
            'https://api.binance.com/api/v3/klines',
            params=params,
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching Binance data: {e}")
        return None

def check_continuity(klines, symbol, interval):
    """Check continuity between consecutive candles."""
    violations = []

    for i in range(len(klines) - 1):
        k1 = klines[i]
        k2 = klines[i + 1]

        close_price = float(k1[4])  # close
        next_open_price = float(k2[1])  # open

        # Check for exact equality (no tolerance for now)
        if close_price != next_open_price:
            diff = abs(close_price - next_open_price)
            violations.append({
                'timestamp1': int(k1[0]),
                'close': close_price,
                'timestamp2': int(k2[0]),
                'open': next_open_price,
                'difference': diff
            })

    return violations

def main():
    # Check a range that includes the problematic timestamps
    start_time = 1763516700000  # First problematic timestamp
    end_time = start_time + (24 * 60 * 60 * 1000)  # Add 24 hours

    print(f"Fetching Binance klines for {SYMBOL} {INTERVAL}")
    print(f"Start time: {datetime.fromtimestamp(start_time / 1000, tz=timezone.utc)}")
    print(f"End time: {datetime.fromtimestamp(end_time / 1000, tz=timezone.utc)}")

    # Fetch data
    klines = fetch_binance_klines(SYMBOL, INTERVAL, start_time=start_time, end_time=end_time, limit=1000)

    if not klines:
        print("Failed to fetch Binance data")
        return

    print(f"Fetched {len(klines)} klines from Binance")

    # Check continuity
    violations = check_continuity(klines, SYMBOL, INTERVAL)

    print(f"\nContinuity check:")
    print(f"Total violations: {len(violations)}")

    if violations:
        print("First 10 violations:")
        for v in violations[:10]:
            dt1 = datetime.fromtimestamp(v['timestamp1'] / 1000, tz=timezone.utc)
            dt2 = datetime.fromtimestamp(v['timestamp2'] / 1000, tz=timezone.utc)
            print(f"T1={dt1.strftime('%Y-%m-%d %H:%M:%S')} close={v['close']:.2f}, T2={dt2.strftime('%Y-%m-%d %H:%M:%S')} open={v['open']:.2f}, diff={v['difference']:.2f}")
    else:
        print("No violations found in Binance data!")

    # Also show first few klines for reference
    print("\nFirst few Binance klines:")
    for k in klines[:5]:
        dt = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc)
    print(f"{dt.strftime('%Y-%m-%d %H:%M:%S UTC')}: {float(k[1]):.2f} | {float(k[2]):.2f} | {float(k[3]):.2f} | {float(k[4]):.2f} | {float(k[5]):.2f}")
if __name__ == "__main__":
    main()
