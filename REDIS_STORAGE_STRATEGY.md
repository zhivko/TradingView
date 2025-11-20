# Redis Storage Strategy: Live Candles vs Background Fetcher Interference

## Current Storage Model

### ZSET Structure
```
Key: klines_z:{symbol}:{interval}
├─ Member: JSON-encoded kline [time, open, high, low, close, volume, ...]
└─ Score: timestamp in milliseconds
```

### Insert Mechanism
```python
pipe.zadd(zset_key, {member: rounded_ts}, nx=True)
```

**CRITICAL**: `nx=True` means "**Only add if key does NOT exist**"

---

## The Interference Problem

### Scenario: Live Candle vs Background Fetcher Race

```
Timeline:
─────────────────────────────────────────────────────────────

T1: 18:00 UTC
  └─ Live price arrives: $45,123
     └─ Client aggregates into candle [18:00, 45,100, 45,150, 45,050, 45,123]
     └─ Saved to ZSET with NX flag
     └─ Redis now has THIS entry

T2: 18:15 UTC
  └─ Background fetcher runs (scheduled or triggered)
     └─ Fetches from Binance for range [14:00 ... 19:00]
     └─ Binance returns SAME timestamp 18:00 with potentially DIFFERENT OHLC
        (official Binance candle: [18:00, 45,120, 45,200, 45,020, 45,100])
     └─ Tries to insert via NX flag
     └─ **NX REJECTS IT** - entry already exists!
     └─ Live candle stays, Binance official candle is NOT updated!

T3: 19:00 UTC
  └─ Chart now shows INCORRECT OHLC for 18:00 candle
     (missing the true high of 45,200, low of 45,020)
```

---

## The Problem In Detail

### Why This Happens

1. **Live prices are bid/ask ticks**, not official Binance aggregated candles
2. **Live candle is INCOMPLETE** while forming (prices keep changing)
3. **Binance official candle** is the definitive OHLC after the hour closes
4. **NX flag prevents overwriting** → live candle data persists even after complete official data arrives

### The Data Quality Issue

```
Live Candle (Built from price ticks during 18:00):
  - Open: 45,100 (first price received)
  - High: 45,150 (highest price seen so far)
  - Low: 45,050 (lowest price seen so far)
  - Close: 45,123 (last price received by 18:00)
  - Volume: ~500 (rough estimate from tick count)

Official Binance Candle (18:00 complete):
  - Open: 45,120 (true opening trade)
  - High: 45,200 (all trades during this hour)
  - Low: 45,020
  - Close: 45,100 (true closing trade)
  - Volume: 12,500 (accurate aggregate)

RESULT: Chart shows WRONG data permanently! ❌
```

---

## Solutions: Choose Your Strategy

### Option 1: Separate Storage (RECOMMENDED - NO INTERFERENCE)

**Store live candles in a SEPARATE Redis namespace:**

```
Live (incomplete) candles: live_candle:{symbol}:{interval}
Historical candles: klines_z:{symbol}:{interval}

Key Differences:
├─ Live candles → Used ONLY for rendering incomplete current candle
├─ Historical → Source of truth for completed candles
└─ NO CONFLICT because different keys
```

**Implementation:**

```python
# In background_fetcher.py - NEW function
def save_live_candle(symbol: str, interval: str, candle_data: dict, redis_client: redis.Redis) -> None:
    """
    Store incomplete candle (being formed from live prices) in temporary storage.
    
    This is SEPARATE from historical klines_z ZSET to avoid interference with
    Binance data fetches via background_fetcher.
    
    Candle will be replaced when official Binance data arrives.
    """
    # Use separate key to avoid NX conflict
    redis_key = f"live_candle:{symbol}:{interval}"
    redis_client.set(
        redis_key,
        json.dumps(candle_data, separators=(",", ":")),
        ex=86400  # Expire after 24h (safety cleanup)
    )

def get_live_candle(symbol: str, interval: str, redis_client: redis.Redis) -> Optional[dict]:
    """
    Retrieve the incomplete live candle (if it exists and is current).
    """
    redis_key = f"live_candle:{symbol}:{interval}"
    data = redis_client.get(redis_key)
    if data:
        return json.loads(data)
    return None
```

**Client-side (main.js):**

```javascript
class LiveCandleBuilder {
    constructor(symbol, interval, redisClient) {
        this.symbol = symbol;
        this.interval = interval;
        this.intervalMs = getIntervalMs(interval);
        this.currentCandle = null;
        this.redis = redisClient;  // Pass Redis client
    }
    
    saveLiveCandle() {
        if (this.currentCandle && this.currentCandle.isIncomplete) {
            // Send to server to save in separate Redis key
            fetch('/save-live-candle', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    symbol: this.symbol,
                    interval: this.interval,
                    candle: this.currentCandle
                })
            });
        }
    }
    
    update(price, timestamp) {
        const candleStart = Math.floor(timestamp / this.intervalMs) * this.intervalMs;
        
        if (!this.currentCandle || candleStart > this.currentCandle.time) {
            // New candle started
            if (this.currentCandle) {
                this.saveLiveCandle();  // Save to SEPARATE storage
            }
            this.currentCandle = {
                time: candleStart,
                open: price,
                high: price,
                low: price,
                close: price,
                isIncomplete: true,
                source: 'live'  // Mark as live-aggregated
            };
        } else {
            this.currentCandle.high = Math.max(this.currentCandle.high, price);
            this.currentCandle.low = Math.min(this.currentCandle.low, price);
            this.currentCandle.close = price;
        }
        
        this.renderCandle(this.currentCandle);
    }
}
```

**Server endpoint:**

```python
@app.route('/save-live-candle', methods=['POST'])
@login_required
def save_live_candle():
    symbol = request.json.get('symbol')
    interval = request.json.get('interval')
    candle = request.json.get('candle')
    
    from background_fetcher import save_live_candle
    
    save_live_candle(symbol, interval, candle, r)
    
    return jsonify({"status": "saved"})
```

**Client-side rendering - Use BOTH sources:**

```javascript
// When rendering chart:
async function loadChartData(symbol, interval) {
    // Get historical completed candles
    const historicalData = await fetch(`/data?symbol=${symbol}&interval=${interval}`);
    const hist = await historicalData.json();
    
    // Get live incomplete candle (if exists)
    const liveResp = await fetch(`/live-candle?symbol=${symbol}&interval=${interval}`);
    const liveData = await liveResp.json();
    
    // Merge: historical + live
    let times = [...hist.time];
    let opens = [...hist.open];
    let highs = [...hist.high];
    let lows = [...hist.low];
    let closes = [...hist.close];
    
    // If live candle exists and is NEWER than last historical
    if (liveData && liveData.candle) {
        const liveTime = new Date(liveData.candle.time).toISOString();
        const lastHistTime = times[times.length - 1];
        
        if (liveTime > lastHistTime) {
            // Append live candle as new
            times.push(liveTime);
            opens.push(liveData.candle.open);
            highs.push(liveData.candle.high);
            lows.push(liveData.candle.low);
            closes.push(liveData.candle.close);
        } else if (liveTime === lastHistTime) {
            // REPLACE last historical candle (live is updated version)
            times[times.length - 1] = liveTime;
            opens[opens.length - 1] = liveData.candle.open;
            highs[highs.length - 1] = liveData.candle.high;
            lows[lows.length - 1] = liveData.candle.low;
            closes[closes.length - 1] = liveData.candle.close;
        }
    }
    
    plotChart(times, opens, highs, lows, closes);
}
```

---

### Option 2: Replace with CX Flag (OVERRIDE)

**Problem:** Allows live data to PERMANENTLY override Binance official data

```python
# DANGEROUS - NOT RECOMMENDED
pipe.zadd(zset_key, {member: rounded_ts}, xx=False)  # Without NX - overwrites
```

**Risk:** Live candle data persists even after official Binance candle arrives. Chart shows INCORRECT OHLC.

---

### Option 3: Soft Updates (TEMPORARY HOLDING AREA)

Store live candles with timestamp suffix to avoid collision:

```python
# Problematic: Creates duplicate entries
redis_key = f"live_candle:{symbol}:{interval}:{timestamp}_live"

# Fetcher later removes these entries when official data arrives
```

**Issue:** Adds complexity, potential for orphaned entries.

---

## Recommended Implementation: Option 1

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    REDIS STORAGE LAYOUT                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  klines_z:{symbol}:{interval}  (ZSET)                       │
│  ├─ Binance official candles ONLY                           │
│  ├─ NX=True prevents overwrites                             │
│  ├─ Source of truth for historical data                     │
│  └─ Updated by: background_fetcher.py                       │
│                                                              │
│  live_candle:{symbol}:{interval}  (STRING)                  │
│  ├─ CURRENT incomplete candle being formed                  │
│  ├─ Replaced every interval                                 │
│  ├─ Can be discarded without data loss                      │
│  ├─ Source: live price aggregation                          │
│  └─ Updated by: client + server on live_price event        │
│                                                              │
│  klines_meta:{symbol}:{interval}  (JSON)                    │
│  ├─ earliest, latest, count, version                        │
│  └─ Tracks only klines_z ZSET metadata                      │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Live Prices (WebSocket)
  ↓
Client aggregates into candle
  ↓
POST /save-live-candle
  ↓
Save to live_candle:{symbol}:{interval} (STRING key)
  ↓
Client /data fetch:
  ├─ Get from klines_z (historical)
  └─ Get from live_candle (incomplete current)
     ↓
  Chart shows merged data
  
Later: Background Fetcher Runs
  ↓
Fetches from Binance
  ↓
Tries zadd(..., nx=True) to klines_z
  ↓
✅ SUCCEEDS - different key than live_candle!
  ↓
Metadata.latest updated
  ↓
Client refreshes /data
  ↓
Now shows official Binance candle
  ↓
live_candle key auto-expires/gets replaced with new incomplete candle
```

---

## Key Advantages of Option 1

| Aspect | Impact |
|--------|--------|
| **No Data Loss** | Live and historical never interfere |
| **No Overwrites** | NX flag still protects Binance data |
| **Auto-Cleanup** | Redis key expires after 24h |
| **Source Separation** | Clear distinction live vs official |
| **Chart Consistency** | When background fetcher updates, chart auto-refreshes |
| **No Double-Fetch** | Client requests both in single call |

---

## Critical Points for Implementation

### 1. Expiration Safety
```python
# Always set TTL on live_candle to prevent stale data
redis_client.set(
    f"live_candle:{symbol}:{interval}",
    json.dumps(candle_data),
    ex=86400  # 24 hours max age
)
```

### 2. Source Tracking
```python
# Mark source in candle data for debugging
candle_data = {
    'time': candle_start,
    'open': price,
    'high': price,
    'low': price,
    'close': price,
    'source': 'live',  # Can be 'live' or 'binance'
    'is_complete': False
}
```

### 3. Background Fetcher Awareness
```python
# When background fetcher completes, notify clients to refresh
def notify_data_updated(symbol: str, interval: str):
    """Emit event telling clients to reload from /data"""
    socketio.emit('data_updated', {
        'symbol': symbol,
        'interval': interval,
        'timestamp': int(time.time() * 1000)
    })
```

### 4. No Modification Needed to `upsert_klines_to_zset()`
The existing function works perfectly as-is because:
- It uses separate `klines_z:` key
- NX flag protects against overwrites
- We never save live candles to this ZSET

---

## Summary: Why Option 1 is Safe

```
Live Candle Storage: live_candle:{symbol}:{interval}
Historical Storage: klines_z:{symbol}:{interval}

These are DIFFERENT keys → NO INTERFERENCE

Background Fetcher tries: zadd(klines_z:..., nx=True)
Live Candle at: live_candle:...
Result: ✅ SUCCESS - Different keys!

Chart merges both:
├─ Historical (from klines_z) = Authoritative
└─ Live (from live_candle) = Incomplete current

When Binance candle arrives (next hour):
└─ Replaces the incomplete live candle in rendering
└─ No data corruption
```
