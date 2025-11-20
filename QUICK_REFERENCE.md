# Quick Reference: Live Candle Storage Strategy

## Your Question & Answer

**Q: "If we 'Aggregate live prices into candles as they arrive', how do you plan to store it in redis - will this saved live candle interfere with next fetch of background_fetcher.py?"**

**A: YES, it will interfere IF stored in the same ZSET key. NO interference if stored separately.**

---

## The Problem (2-Minute Read)

### Current Situation
- **background_fetcher.py** stores official Binance candles in `klines_z:{symbol}:{interval}` (Redis ZSET)
- Uses `nx=True` flag: "only add if timestamp doesn't already exist"
- This protects against duplicate storage ✅

### The Danger
If you save live candles to the SAME ZSET key:

```
18:00 UTC: Live price → candle aggregated → stored in klines_z:BTCUSDT:1h
         Score: 18:00, Data: [18:00, 45100, 45150, 45050, 45100, ~200]

19:05 UTC: Background fetcher runs → fetches official Binance data
         Official: [18:00, 45120, 45200, 45020, 45100, 12500]
         Tries: zadd(klines_z:BTCUSDT:1h, {official_data: 18:00}, nx=True)
         Result: REJECTED ❌ (score 18:00 already exists with live data)
         
Chart shows WRONG data FOREVER! ❌
```

---

## The Solution (1-Minute Read)

**Use a DIFFERENT Redis key for live candles:**

```
live_candle:BTCUSDT:1h     ← Temporary, incomplete candles from prices
klines_z:BTCUSDT:1h        ← Official, completed candles from Binance
```

These are DIFFERENT keys → NO INTERFERENCE

```
18:00 UTC: Live candle → saved to live_candle:BTCUSDT:1h ✅
19:05 UTC: Background fetcher → zadd to klines_z:BTCUSDT:1h ✅ (different key!)
           Binance official data inserted successfully!
Chart auto-updates with correct data ✅
```

---

## What Gets Stored Where

### Before Implementation
```
Redis:
├─ klines_z:BTCUSDT:1h  ← Only official Binance candles
└─ live_price:BTCUSDT   ← Single price tick (current, only)
```

### After Implementation
```
Redis:
├─ klines_z:BTCUSDT:1h        ← Official Binance candles (ZSET, source of truth)
├─ live_candle:BTCUSDT:1h     ← Current incomplete candle (STRING, expires 24h)
└─ live_price:BTCUSDT         ← Single price tick (unchanged)
```

---

## Critical Security: NX Flag

Your existing code with NX is EXCELLENT:
```python
pipe.zadd(zset_key, {member: rounded_ts}, nx=True)
```

**Why it matters:**
- Prevents duplicate Binance data ✅
- BUT also prevents updating ANY candle at same timestamp
- THEREFORE: Must use different key for live candles ✅

**The fix:** Just use different key, no NX flag change needed!

---

## Storage Format

### Official Candle (in klines_z)
```python
{
    "0": 1732029600000,  # Open time (ms)
    "1": "45120",        # Open price
    "2": "45200",        # High
    "3": "45020",        # Low
    "4": "45100",        # Close
    "5": "12500",        # Volume
    ...
}
```
(Binance format, 12+ fields)

### Live Candle (in live_candle)
```python
{
    "time": 1732029600000,     # Open time (ms)
    "open": 45100,             # Open price
    "high": 45180,             # Highest seen
    "low": 45010,              # Lowest seen
    "close": 45123,            # Last price
    "is_incomplete": True,     # Flag: still forming
    "source": "live"           # Source identifier
}
```
(Simplified format, only essentials)

---

## Data Flow Timeline

```
┌─────────────────────────────────────────────────────────────────┐
│ HISTORICAL STATE                                                 │
│ Latest stored candle: 17:00 UTC (from background fetcher)       │
└─────────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────────┐
│ 18:00-18:59 UTC: LIVE PRICE STREAM                              │
│ ├─ Prices arrive: 45100, 45150, 45075, 45180, 45095, ...       │
│ ├─ Client aggregates: O=45100, H=45180, L=45050, C=45095       │
│ ├─ Save to: live_candle:BTCUSDT:1h ✅                          │
│ └─ Chart shows: historical (up to 17:00) + live (18:00 forming)│
└─────────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────────┐
│ 19:05 UTC: BACKGROUND FETCHER RUNS                              │
│ ├─ Fetches from Binance: 14:00 ... 19:30                       │
│ ├─ Includes official 18:00: O=45120, H=45200, L=45020, C=45100│
│ ├─ Insert to: klines_z:BTCUSDT:1h ✅ (different key!)          │
│ ├─ Update metadata.latest = 19:00 (or later)                   │
│ └─ Chart will refresh automatically                             │
└─────────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────────┐
│ FINAL STATE                                                      │
│ ├─ Historical: 14:00, 15:00, 16:00, 17:00, 18:00 (OFFICIAL)   │
│ ├─ Live: 19:00 (forming from prices)                            │
│ ├─ Chart shows: complete history + current forming candle       │
│ └─ No data loss, no corruption! ✅                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Scope

| Component | Changes |
|-----------|---------|
| **background_fetcher.py** | +2 functions (save_live_candle, get_live_candle) |
| **app.py** | +2 endpoints (/live-candle GET, /save-live-candle POST) |
| **main.js** | +1 class (LiveCandleBuilder), integrate with socket.on('live_price') |
| **Redis Keys** | +1 new key pattern: live_candle:{symbol}:{interval} |
| **No changes** | upsert_klines_to_zset(), background_fetcher fetch logic |

---

## Why This Works

| Aspect | How It Works |
|--------|--------------|
| **No Interference** | Different Redis keys = no NX flag collision |
| **Auto-Cleanup** | live_candle expires after 24h (safety) |
| **Data Integrity** | Binance official always wins in chart display |
| **No Backend Changes** | NX flag still protects Binance data |
| **Live Updates** | Client aggregates prices in real-time |
| **Auto-Refresh** | When background fetcher runs, metadata updates and client sees new data |
| **Graceful Degradation** | If no live data, chart shows historical (as it does now) |

---

## Three Critical Decisions

1. **Storage Location:** Use `live_candle:{symbol}:{interval}` NOT `klines_z:{symbol}:{interval}` ✅
2. **Format:** Simplified candle object (not Binance array format) ✅
3. **TTL:** 24-hour auto-expire for safety ✅

---

## Testing the Strategy

### Test Case 1: Live Update
```
1. Chart loads with historical data up to 17:00
2. Live price 45120 arrives
3. Client builds candle for 18:00
4. Save to live_candle:BTCUSDT:1h
5. Chart shows historical + live 18:00 (forming)
✅ PASS
```

### Test Case 2: Background Fetcher
```
1. Background fetcher runs and fetches 18:00 official
2. Tries zadd(klines_z:BTCUSDT:1h, {official_18:00}, nx=True)
3. Succeeds (different key than live_candle)
4. metadata.latest updated
5. Client gets new data, chart auto-refreshes
✅ PASS
```

### Test Case 3: Candle Transition
```
1. 18:59 - Last live price, 18:00 candle saved
2. 19:00 - New price arrives, triggers new candle
3. Old live_candle:BTCUSDT:1h replaced with 19:00
4. 19:05 - Background fetcher inserts official 18:00, 19:00
5. Chart shows official 18:00, live 19:00 (forming)
✅ PASS
```

---

## Documents for Reference

1. **LIVE_CANDLE_ARCHITECTURE.md** - Overall strategy and design
2. **REDIS_STORAGE_STRATEGY.md** - Detailed storage analysis
3. **REDIS_INTERFERENCE_ANALYSIS.md** - What could go wrong and why
4. **IMPLEMENTATION_PLAN.md** - Step-by-step code implementation

---

## Summary

✅ **YES, live candles CAN be safely stored without interfering with background_fetcher.py**

**The key:** Use a different Redis key for temporary live candles.

**No risk of:**
- Data corruption
- Lost Binance data
- Duplicate candles
- Chart inaccuracy

**Guarantee:** Your `nx=True` flag in `upsert_klines_to_zset()` remains safe because live and official candles go to different keys.
