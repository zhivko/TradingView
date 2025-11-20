#!/usr/bin/env python3
"""
Test script for 1-minute candle aggregation.
Tests the aggregate_1m_candles_to_interval function.
"""

import sys
import json
from typing import List, Tuple

# Mock the aggregation function (copied from app.py)
def aggregate_1m_candles_to_interval(
    candles_1m: List[dict],
    target_interval: str,
    interval_ms: int
) -> Tuple[List[int], List[float], List[float], List[float], List[float], List[float]]:
    """
    Aggregate 1-minute candles into a higher timeframe.
    """
    if not candles_1m:
        return [], [], [], [], [], []
    
    candles_by_interval = {}
    
    for candle in candles_1m:
        try:
            open_time = int(candle.get('time', 0))
            open_price = float(candle.get('open', 0))
            high_price = float(candle.get('high', 0))
            low_price = float(candle.get('low', 0))
            close_price = float(candle.get('close', 0))
            volume = float(candle.get('volume', 0))
            
            interval_key = (open_time // interval_ms) * interval_ms
            
            if interval_key not in candles_by_interval:
                candles_by_interval[interval_key] = {
                    'time': interval_key,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': volume
                }
            else:
                existing = candles_by_interval[interval_key]
                existing['high'] = max(existing['high'], high_price)
                existing['low'] = min(existing['low'], low_price)
                existing['close'] = close_price
                existing['volume'] += volume
        except (ValueError, KeyError, TypeError):
            continue
    
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


def test_aggregation_5m():
    """Test aggregating 1m candles to 5m"""
    print("\n=== Test 1: Aggregating 5 x 1m candles to 5m ===")
    
    # Create 5 consecutive 1-min candles (18:00 to 18:04)
    base_time = 1700470800000  # 2023-11-20 18:00:00 UTC in ms
    
    candles_1m = [
        {'time': base_time,              'open': 45100, 'high': 45150, 'low': 45080, 'close': 45120, 'volume': 350},
        {'time': base_time + 60000,      'open': 45120, 'high': 45180, 'low': 45100, 'close': 45150, 'volume': 420},
        {'time': base_time + 120000,     'open': 45150, 'high': 45200, 'low': 45120, 'close': 45180, 'volume': 500},
        {'time': base_time + 180000,     'open': 45180, 'high': 45190, 'low': 45160, 'close': 45170, 'volume': 380},
        {'time': base_time + 240000,     'open': 45170, 'high': 45210, 'low': 45160, 'close': 45190, 'volume': 450},
    ]
    
    interval_ms = 5 * 60 * 1000  # 5 minutes
    times, opens, highs, lows, closes, volumes = aggregate_1m_candles_to_interval(candles_1m, '5m', interval_ms)
    
    assert len(times) == 1, f"Expected 1 candle, got {len(times)}"
    assert opens[0] == 45100, f"Expected open=45100, got {opens[0]}"
    assert highs[0] == 45210, f"Expected high=45210, got {highs[0]}"
    assert lows[0] == 45080, f"Expected low=45080, got {lows[0]}"
    assert closes[0] == 45190, f"Expected close=45190, got {closes[0]}"
    assert volumes[0] == 2100, f"Expected volume=2100, got {volumes[0]}"
    
    print(f"✅ Aggregation successful!")
    print(f"   Time: {times[0]}")
    print(f"   OHLCV: {opens[0]} / {highs[0]} / {lows[0]} / {closes[0]} / {volumes[0]}")


def test_aggregation_1h():
    """Test aggregating 60 x 1m candles to 1h"""
    print("\n=== Test 2: Aggregating 60 x 1m candles to 1h ===")
    
    base_time = 1700470800000  # 2023-11-20 18:00:00 UTC in ms
    candles_1m = []
    
    # Create 60 consecutive 1-min candles
    for i in range(60):
        price = 45100 + (i % 10) * 10  # Vary price between 45100 and 45190
        candles_1m.append({
            'time': base_time + (i * 60000),
            'open': price,
            'high': price + 50,
            'low': price - 50,
            'close': price + 25,
            'volume': 100 + i
        })
    
    interval_ms = 60 * 60 * 1000  # 1 hour
    times, opens, highs, lows, closes, volumes = aggregate_1m_candles_to_interval(candles_1m, '1h', interval_ms)
    
    assert len(times) == 1, f"Expected 1 candle, got {len(times)}"
    assert opens[0] == candles_1m[0]['open'], f"Expected open to match first candle"
    assert highs[0] == 45240, f"Expected high=45240, got {highs[0]}"  # max high + 50
    assert lows[0] == 45050, f"Expected low=45050, got {lows[0]}"     # min low - 50
    assert closes[0] == candles_1m[-1]['close'], f"Expected close to match last candle"
    expected_volume = sum(100 + i for i in range(60))
    assert volumes[0] == expected_volume, f"Expected volume={expected_volume}, got {volumes[0]}"
    
    print(f"✅ 1h aggregation successful!")
    print(f"   Time: {times[0]}")
    print(f"   OHLCV: {opens[0]} / {highs[0]} / {lows[0]} / {closes[0]} / {volumes[0]}")
    print(f"   Volume sum: {volumes[0]} (from 60 candles)")


def test_aggregation_multiple_hours():
    """Test aggregating 120 x 1m candles into 2x 1h candles"""
    print("\n=== Test 3: Aggregating 120 x 1m candles to multiple 1h candles ===")
    
    base_time = 1700470800000  # 2023-11-20 18:00:00 UTC in ms
    candles_1m = []
    
    # Create 120 consecutive 1-min candles (2 hours)
    for i in range(120):
        price = 45100 + (i % 20) * 5
        candles_1m.append({
            'time': base_time + (i * 60000),
            'open': price,
            'high': price + 20,
            'low': price - 20,
            'close': price + 10,
            'volume': 50 + i
        })
    
    interval_ms = 60 * 60 * 1000  # 1 hour
    times, opens, highs, lows, closes, volumes = aggregate_1m_candles_to_interval(candles_1m, '1h', interval_ms)
    
    assert len(times) == 2, f"Expected 2 candles, got {len(times)}"
    
    # First hour: 0-59 candles
    print(f"✅ Multi-hour aggregation successful!")
    print(f"   Candle 1 (18:00-19:00): OHLCV = {opens[0]}/{highs[0]}/{lows[0]}/{closes[0]}/{volumes[0]}")
    print(f"   Candle 2 (19:00-20:00): OHLCV = {opens[1]}/{highs[1]}/{lows[1]}/{closes[1]}/{volumes[1]}")


def test_empty_input():
    """Test with empty input"""
    print("\n=== Test 4: Empty input ===")
    
    times, opens, highs, lows, closes, volumes = aggregate_1m_candles_to_interval([], '5m', 5*60*1000)
    
    assert times == [], "Expected empty times"
    assert opens == [], "Expected empty opens"
    
    print(f"✅ Empty input handled correctly")


def test_malformed_candles():
    """Test with malformed candles (should skip them)"""
    print("\n=== Test 5: Malformed candles (skipped gracefully) ===")
    
    base_time = 1700470800000
    
    # All valid candles within same 5m interval (0, 60s, 120s all < 5min = 300s)
    candles_1m = [
        {'time': base_time,              'open': 45100, 'high': 45150, 'low': 45080, 'close': 45120, 'volume': 350},
        {'time': base_time + 60000,      'open': 45120, 'high': 45180, 'low': 45100, 'close': 45150, 'volume': 420},
        {'time': 'invalid'},  # Missing required fields (skipped)
        {'time': base_time + 90000,      'open': 45150, 'high': 45200, 'low': 45120, 'close': 45180, 'volume': 500},
        {},  # Completely empty (skipped)
    ]
    
    interval_ms = 5 * 60 * 1000
    times, opens, highs, lows, closes, volumes = aggregate_1m_candles_to_interval(candles_1m, '5m', interval_ms)
    
    # Should have 1 candle (all valid candles fall into same 5m interval 18:00-18:05)
    assert len(times) == 1, f"Expected 1 candle after filtering, got {len(times)}"
    
    print(f"✅ Malformed candles handled gracefully")
    print(f"   Processed 5 input candles, aggregated to {len(times)} valid candle(s)")
    print(f"   Result: {opens[0]}/{highs[0]}/{lows[0]}/{closes[0]}/{volumes[0]}")


if __name__ == '__main__':
    print("=" * 60)
    print("Testing 1-Minute Candle Aggregation Function")
    print("=" * 60)
    
    try:
        test_aggregation_5m()
        test_aggregation_1h()
        test_aggregation_multiple_hours()
        test_empty_input()
        test_malformed_candles()
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED!")
        print("=" * 60)
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
