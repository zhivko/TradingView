# Live OHLCV Candle Strategy - Comprehensive Analysis

## Current Data Flow Architecture

### 1. Historical Data Pipeline (background_fetcher.py)

```
Binance API
    ↓
fetch_gap_from_binance() 
    ↓
Redis ZSET (klines_z:{symbol}:{interval})
    + Metadata (klines_meta:{symbol}:{interval})
```

**Key Components:**
- **ZSET Storage**: Each candle is stored as JSON member, scored by timestamp (milliseconds)
- **Metadata Tracking**: 
  - `earliest`: First timestamp pulled (oldest data)
  - `latest`: Last timestamp pulled (newest data)
  - `count`: Total candles stored
  - `version`: Schema version

**Critical Issue Identified:**
The metadata tracks `latest` timestamp from **background fetcher operations only**, NOT from live prices. When the app starts:
1. `latest` = last time Binance API was queried for historical data
2. Live prices continue arriving, but `latest` stays stale
3. Gap between `latest` metadata and current market time = **MISSING CANDLES**

### 2. Real-time Price Stream (live_price via websocket)

```
Binance WebSocket (ticker stream)
    ↓
start_live_price_stream() in background_fetcher.py
    ↓
Redis key: live_price:{symbol} (single price tick)
    ↓
Socket.IO emit('live_price', {symbol, price, timestamp})
    ↓
Client JS receives individual price updates
```

**Current State**: Only single price points, no OHLC aggregation

### 3. Client Data Fetch on Page Load (/data endpoint)

```
Client requests /data with (symbol, interval, [startTime], [endTime], limit)
    ↓
Server queries Redis ZSET for window
    ↓
Returns: arrays of [time, open, high, low, close, volume]
    ↓
Plotly renders candlestick chart
```

**Problem**: Client only gets historical data up to `latest` metadata timestamp. Any candles completed AFTER the last background_fetcher run are MISSING.

---

## The Missing Candles Problem

### Scenario Example:
```
Background Fetcher State:
  - Latest candle timestamp: 2025-11-20 14:00 UTC
  - Metadata.latest = 1732029600000 ms

Current Time: 2025-11-20 18:45 UTC
  - 4h 45m have passed
  - ~23 missing 1h-candles since last fetch
  - ~45 missing 5m-candles

Client loads chart:
  - Gets data up to 14:00 UTC
  - Live prices show current price (18:45)
  - Gap of 4h 45m with NO candles
  - Only a horizontal price line at 18:45 (current updateLivePriceLine())
```

### Root Causes:
1. **No auto-fill mechanism**: Background fetcher doesn't run continuously
2. **Metadata lag**: `latest` isn't updated when live prices arrive
3. **No client-side awareness**: Client doesn't know about the gap
4. **No gap detection**: `/data` endpoint doesn't check for gaps between stored data and now

---

## Smart Implementation Strategy

### Phase 1: Detect Gaps (Non-Breaking)

**Endpoint**: Enhance `/data` response with metadata

```python
# In /data endpoint after reading from ZSET:
latest_stored_ts = get_latest_kline_time(symbol, interval)
now_ms = int(time.time() * 1000)
interval_ms = get_timeframe_seconds(interval) * 1000

gap_ms = now_ms - latest_stored_ts
gap_candles = gap_ms // interval_ms

return jsonify({
    "time": times,
    "open": opens,
    "high": highs,
    "low": lows,
    "close": closes,
    "volume": volumes,
    # NEW:
    "metadata": {
        "latest_stored_ts": latest_stored_ts,
        "now_ms": now_ms,
        "interval_ms": interval_ms,
        "gap_ms": gap_ms,
        "gap_candles": gap_candles,  # How many candles are missing
        "gap_status": "has_gap" if gap_candles > 0 else "complete"
    }
})
```

**Client-side** (main.js):
```javascript
socket.on('data_received', (data) => {
    if (data.metadata && data.metadata.gap_candles > 0) {
        console.warn(`⚠️ Gap detected: ${data.metadata.gap_candles} missing candles`);
        // Show warning in UI
        statusDisplay.textContent = `Missing ${data.metadata.gap_candles} candles since last update`;
    }
});
```

### Phase 2: Fill Gaps Automatically

**Option A: Pull missing candles from Binance**

```python
# New endpoint: /fill-gap
@app.route('/fill-gap', methods=['POST'])
@login_required
def fill_gap():
    symbol = request.json.get('symbol')
    interval = request.json.get('interval')
    
    latest_stored = get_latest_kline_time(symbol, interval)
    now_ms = int(time.time() * 1000)
    interval_ms = get_timeframe_seconds(interval) * 1000
    
    # Fetch from Binance starting after latest_stored
    gap_start = latest_stored + interval_ms
    gap_end = now_ms
    
    fetch_gap_from_binance(
        symbol=symbol,
        interval=interval,
        gap_start=gap_start,
        gap_end=gap_end,
        interval_ms=interval_ms,
        ...
    )
    
    return jsonify({"status": "filled"})
```

**Option B: Build incomplete candle from live prices (Recommended)**

Track the current forming candle as prices arrive:

```javascript
// main.js - Add to socket.on('live_price')
class LiveCandleBuilder {
    constructor(interval) {
        this.interval = interval;
        this.intervalMs = getIntervalMs(interval);
        this.currentCandle = null;
    }
    
    update(price, timestamp) {
        const candleStart = Math.floor(timestamp / this.intervalMs) * this.intervalMs;
        
        // New candle started?
        if (!this.currentCandle || candleStart > this.currentCandle.time) {
            // Save previous if exists
            if (this.currentCandle && this.currentCandle.time >= this.lastStoredTime) {
                this.addToChart(this.currentCandle);
            }
            
            this.currentCandle = {
                time: candleStart,
                open: price,
                high: price,
                low: price,
                close: price
            };
        } else {
            // Update current candle
            this.currentCandle.high = Math.max(this.currentCandle.high, price);
            this.currentCandle.low = Math.min(this.currentCandle.low, price);
            this.currentCandle.close = price;
        }
        
        this.renderCandle(this.currentCandle);
    }
    
    addToChart(candle) {
        // Append to chart trace if not already there
    }
    
    renderCandle(candle) {
        // Update last candle in chart
    }
}

let candleBuilder = null;

socket.on('live_price', (data) => {
    if (!candleBuilder) {
        candleBuilder = new LiveCandleBuilder(currentInterval);
    }
    candleBuilder.update(data.price, data.timestamp);
});
```

### Phase 3: Bridge Historical → Live

**Key Insight**: The last stored candle and the live candle being built might overlap or have a gap.

```javascript
function initializeLiveCandles(historicalData, currentPrice, currentTimestamp) {
    const latestHistoricalTime = historicalData.time[historicalData.time.length - 1];
    const latestHistoricalMs = new Date(latestHistoricalTime).getTime();
    const intervalMs = getIntervalMs(currentInterval);
    const expectedNextCandleTime = latestHistoricalMs + intervalMs;
    
    // Determine which candle the live price belongs to
    const liveCandelStart = Math.floor(currentTimestamp / intervalMs) * intervalMs;
    
    if (liveCandelStart === latestHistoricalMs) {
        // Live price is for the LAST historical candle (still forming)
        // Update that candle
        candleBuilder.currentCandle = {
            time: liveCandelStart,
            open: historicalData.open[historicalData.open.length - 1],
            high: historicalData.high[historicalData.high.length - 1],
            low: historicalData.low[historicalData.low.length - 1],
            close: currentPrice,
            isIncomplete: true
        };
    } else if (liveCandelStart > latestHistoricalMs) {
        // Live price belongs to FUTURE candle(s)
        // Create new incomplete candles for gap
        candleBuilder.currentCandle = {
            time: liveCandelStart,
            open: currentPrice,
            high: currentPrice,
            low: currentPrice,
            close: currentPrice,
            isIncomplete: true
        };
    }
    
    candleBuilder.lastStoredTime = latestHistoricalMs;
}
```

---

## Implementation Roadmap

### Step 1: Metadata Enhancement (5 min)
- [ ] Modify `/data` endpoint to return gap information
- [ ] Add gap detection in client

### Step 2: Visual Feedback (10 min)
- [ ] Show warning when gap detected
- [ ] Display "incomplete" indicator on live candle
- [ ] Color last candle differently

### Step 3: Live Candle Building (30 min)
- [ ] Implement `LiveCandleBuilder` class
- [ ] Track O, H, L, C per interval
- [ ] Update chart incrementally

### Step 4: Gap Filling (20 min)
- [ ] Auto-fetch missing candles from Binance
- [ ] OR: Build from accumulated live prices

### Step 5: Persistence (10 min)
- [ ] Save completed candles to Redis when they close
- [ ] Update metadata.latest when new candle closes

---

## Data Flow After Implementation

```
Historical Data (from background_fetcher)
    ↓
Latest Stored: 2025-11-20 14:00 UTC
    ↓
Gap Detection: 4h 45m missing
    ↓
Live Price Stream (websocket)
    ↓
LiveCandleBuilder aggregates prices
    ↓
Creates missing candles for gap period
    ↓
Renders incomplete current candle being formed
    ↓
When candle closes → Save to Redis
    ↓
Update metadata.latest
    ↓
Chart shows continuous data from historical → live
```

---

## Key Variables to Track

### Server-side (app.py):
```python
# NEW: Track live candles being built per symbol/interval
live_candle_state = {
    f"{symbol}_{interval}": {
        "time": candle_start_ms,
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "last_update": timestamp_ms,
        "is_complete": bool
    }
}
```

### Client-side (main.js):
```javascript
let candleBuilder = null;  // LiveCandleBuilder instance
let lastStoredCandleTime = null;  // Time of last historical candle
let gapDetected = false;  // Whether gap exists
let incompleteCandles = [];  // Candles being formed
```

---

## Critical Considerations

### 1. Timezone Handling
- Binance uses UTC timestamps (milliseconds)
- Candle boundaries must align to UTC, not local time
- `Math.floor(timestamp / intervalMs) * intervalMs` ensures correct alignment

### 2. Volume Data
- Live prices from websocket don't include volume
- Options:
  a. Query Binance API when candle closes
  b. Wait for background_fetcher to pull the candle
  c. Track trade count as proxy for volume

### 3. Data Consistency
- Live prices might arrive out of order (very rare)
- Add timestamp validation: `timestamp >= lastTimestamp`
- Handle duplicate price ticks gracefully

### 4. Chart Updates
- Use `Plotly.restyle()` not `Plotly.newPlot()` for live updates
- Only update last candle (most recent)
- Batch updates to avoid excessive rendering

### 5. Redis Persistence
- Completed candles should be saved immediately
- Use `upsert_klines_to_zset()` from app.py
- Update metadata.latest atomically
