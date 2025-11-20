# Implementation Plan: Safe Live Candle Storage

## Quick Answer to Your Question

**Q: Will saved live candles interfere with background_fetcher?**

**A: YES - if stored in same ZSET key. NO - if stored separately.**

### The Key Insight:
Your `upsert_klines_to_zset()` uses `nx=True` (NX flag):
```python
pipe.zadd(zset_key, {member: rounded_ts}, nx=True)
# ↑ "Only add if this timestamp doesn't already exist"
```

**Problem:** If live candle exists at timestamp 18:00, Binance's official 18:00 candle gets REJECTED.

**Solution:** Store live candles in a DIFFERENT Redis key:
```
live_candle:BTCUSDT:1h  ← Live incomplete (temp storage)
klines_z:BTCUSDT:1h     ← Official (permanent)
```

---

## Implementation Checklist

### Phase 1: Backend Changes (app.py + background_fetcher.py)

#### Step 1A: Add Live Candle Storage Function to background_fetcher.py

```python
def save_live_candle(
    symbol: str,
    interval: str,
    candle_data: dict,
    redis_client: redis.Redis,
    ttl_seconds: int = 86400
) -> bool:
    """
    Store an incomplete live candle in temporary Redis storage.
    
    Args:
        symbol: Trading symbol (e.g., 'BTCUSDT')
        interval: Timeframe (e.g., '1h')
        candle_data: Dict with {time, open, high, low, close, is_incomplete}
        redis_client: Redis connection
        ttl_seconds: Auto-expire time (default 24h)
    
    Returns:
        True if saved, False on error
    
    Note: This is SEPARATE from klines_z ZSET to avoid interference
    with background_fetcher NX operations.
    """
    try:
        redis_key = f"live_candle:{symbol}:{interval}"
        redis_client.set(
            redis_key,
            json.dumps(candle_data, separators=(",", ":")),
            ex=ttl_seconds
        )
        logging.getLogger(__name__).debug(
            f"Saved live candle: {symbol} {interval} time={candle_data.get('time')}"
        )
        return True
    except Exception as e:
        logging.getLogger(__name__).error(
            f"Failed to save live candle: {symbol} {interval} error={e}"
        )
        return False


def get_live_candle(
    symbol: str,
    interval: str,
    redis_client: redis.Redis
) -> Optional[dict]:
    """
    Retrieve the incomplete live candle (if it exists and is current).
    
    Returns:
        Dict with {time, open, high, low, close, is_incomplete} or None
    """
    try:
        redis_key = f"live_candle:{symbol}:{interval}"
        data = redis_client.get(redis_key)
        if data:
            return json.loads(data)
    except Exception:
        pass
    return None
```

#### Step 1B: Add Server Endpoint in app.py

```python
@app.route('/live-candle', methods=['GET'])
@login_required
def get_live_candle_endpoint():
    """
    Return the current incomplete live candle (being formed from price ticks).
    
    Query Parameters:
        symbol: Trading symbol (required)
        interval: Timeframe (required)
    
    Response:
        {
            "candle": {
                "time": timestamp_ms,
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "is_incomplete": true,
                "source": "live"
            }
        }
        or
        {
            "candle": null  # If no live candle exists
        }
    """
    symbol = request.args.get('symbol')
    interval = request.args.get('interval')
    
    if not symbol or not interval:
        return jsonify({"error": "Missing symbol or interval"}), 400
    
    from background_fetcher import get_live_candle
    
    try:
        candle = get_live_candle(symbol, interval, r)
        return jsonify({"candle": candle})
    except Exception as e:
        app.logger.error(f"Error getting live candle: {e}")
        return jsonify({"candle": None})


@app.route('/save-live-candle', methods=['POST'])
@login_required
def save_live_candle_endpoint():
    """
    Save a live candle (incomplete, being formed from prices).
    
    Request Body:
        {
            "symbol": "BTCUSDT",
            "interval": "1h",
            "candle": {
                "time": timestamp_ms,
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "is_incomplete": true
            }
        }
    """
    data = request.json or {}
    symbol = data.get('symbol')
    interval = data.get('interval')
    candle = data.get('candle')
    
    if not all([symbol, interval, candle]):
        return jsonify({"error": "Missing required fields"}), 400
    
    from background_fetcher import save_live_candle
    
    try:
        success = save_live_candle(symbol, interval, candle, r)
        if success:
            return jsonify({"status": "saved"})
        else:
            return jsonify({"error": "Failed to save"}), 500
    except Exception as e:
        app.logger.error(f"Error saving live candle: {e}")
        return jsonify({"error": str(e)}), 500
```

#### Step 1C: Modify `/data` Endpoint to Include Gap Info

```python
# In the /data endpoint, after loading historical data, add:

# Get live candle info
latest_stored_ts = get_latest_kline_time(symbol, interval)
now_ms = int(time.time() * 1000)
interval_ms = get_timeframe_seconds(interval) * 1000

gap_ms = now_ms - (latest_stored_ts or 0)
gap_candles = gap_ms // interval_ms

return jsonify({
    "time": times,
    "open": opens,
    "high": highs,
    "low": lows,
    "close": closes,
    "volume": volumes,
    # NEW: Metadata about gaps
    "metadata": {
        "latest_stored_ts": latest_stored_ts,
        "now_ms": now_ms,
        "interval_ms": interval_ms,
        "gap_ms": gap_ms,
        "gap_candles": gap_candles,
        "has_gap": gap_candles > 0
    }
})
```

---

### Phase 2: Client-side Implementation (main.js)

#### Step 2A: Create LiveCandleBuilder Class

```javascript
class LiveCandleBuilder {
    constructor(symbol, interval) {
        this.symbol = symbol;
        this.interval = interval;
        this.intervalMs = getIntervalMs(interval);
        this.currentCandle = null;
        this.lastStoredCandleTime = null;
        this.isActive = true;
    }

    /**
     * Update candle with new price.
     * Call this when live price arrives via websocket.
     */
    update(price, timestamp) {
        if (!this.isActive) return;

        const candleStart = Math.floor(timestamp / this.intervalMs) * this.intervalMs;

        // New candle started?
        if (!this.currentCandle || candleStart > this.currentCandle.time) {
            // Save previous candle to server
            if (this.currentCandle && this.currentCandle.time > (this.lastStoredCandleTime || 0)) {
                this.saveToServer(this.currentCandle);
            }

            // Start new candle
            this.currentCandle = {
                time: candleStart,
                open: price,
                high: price,
                low: price,
                close: price,
                is_incomplete: true,
                source: 'live'
            };
        } else {
            // Update current candle
            this.currentCandle.high = Math.max(this.currentCandle.high, price);
            this.currentCandle.low = Math.min(this.currentCandle.low, price);
            this.currentCandle.close = price;
        }

        // Update chart immediately
        this.updateChart();
    }

    /**
     * Initialize from historical data.
     * Call this when chart first loads with historical candles.
     */
    initializeFromHistorical(historicalTimes) {
        if (!historicalTimes || historicalTimes.length === 0) {
            this.lastStoredCandleTime = 0;
            return;
        }

        const lastTimeStr = historicalTimes[historicalTimes.length - 1];
        this.lastStoredCandleTime = new Date(lastTimeStr).getTime();
    }

    /**
     * Save candle to server's Redis temporary storage.
     */
    async saveToServer(candle) {
        try {
            const response = await fetch('/save-live-candle', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    symbol: this.symbol,
                    interval: this.interval,
                    candle: candle
                })
            });

            if (!response.ok) {
                console.error('[LiveCandleBuilder] Failed to save:', response.status);
            }
        } catch (error) {
            console.error('[LiveCandleBuilder] Error saving candle:', error);
        }
    }

    /**
     * Update the chart with current candle.
     */
    updateChart() {
        if (!this.currentCandle) return;

        const chartEl = document.getElementById('chart');
        if (!chartEl || !chartEl.data || !chartEl.data[0]) return;

        const candleTrace = chartEl.data[0];
        const candleTime = new Date(this.currentCandle.time).toISOString();

        // Find if this candle time already exists in chart
        let index = candleTrace.x.findIndex(t => t === candleTime);

        if (index === -1) {
            // New candle: append to chart
            candleTrace.x.push(candleTime);
            candleTrace.open.push(this.currentCandle.open);
            candleTrace.high.push(this.currentCandle.high);
            candleTrace.low.push(this.currentCandle.low);
            candleTrace.close.push(this.currentCandle.close);
            index = candleTrace.x.length - 1;
        } else {
            // Update existing candle (last one being formed)
            candleTrace.open[index] = this.currentCandle.open;
            candleTrace.high[index] = this.currentCandle.high;
            candleTrace.low[index] = this.currentCandle.low;
            candleTrace.close[index] = this.currentCandle.close;
        }

        // Update chart with restyle (efficient, no full redraw)
        Plotly.restyle(chartEl, {
            x: [candleTrace.x],
            open: [candleTrace.open],
            high: [candleTrace.high],
            low: [candleTrace.low],
            close: [candleTrace.close]
        }).catch(err => {
            console.error('[LiveCandleBuilder] Plotly.restyle error:', err);
        });
    }

    stop() {
        this.isActive = false;
        if (this.currentCandle) {
            this.saveToServer(this.currentCandle);
        }
    }
}
```

#### Step 2B: Integrate with Existing Code in main.js

Replace the current `socket.on('live_price')` handler:

```javascript
// Around line 270 in main.js
let liveCandleBuilder = null;  // Global instance

socket.on('live_price', (data) => {
    // Ignore errors or data without price
    if (data.error || !data.price) {
        return;
    }

    const symbol = data.symbol;
    const price = data.price;
    const timestamp = data.timestamp;

    // Only update if this is for the currently displayed symbol
    if (symbol === currentSymbol) {
        console.log(`[live_price] Received ${symbol}: $${price} at ${new Date(timestamp).toISOString()}`);

        currentLivePrice = {
            symbol: symbol,
            price: price,
            timestamp: timestamp
        };

        // NEW: Update live candle builder
        if (!liveCandleBuilder || liveCandleBuilder.symbol !== symbol || liveCandleBuilder.interval !== currentInterval) {
            liveCandleBuilder = new LiveCandleBuilder(symbol, currentInterval);
        }
        liveCandleBuilder.update(price, timestamp);

        // Keep existing live price line
        updateLivePriceLine(price);

        // Update live price display in right panel
        const livePriceDisplay = document.getElementById('live-price-display');
        if (livePriceDisplay) {
            livePriceDisplay.textContent = `$${price.toFixed(2)}`;
        }
    }
});
```

#### Step 2C: Initialize Builder When Chart Loads

In the chart loading function (around line 2173 where `Plotly.newPlot` is called), add:

```javascript
Plotly.newPlot('chart', traces, layout, config).then(() => {
    // ... existing code ...

    // NEW: Initialize live candle builder
    if (traces[0] && traces[0].x) {
        if (!liveCandleBuilder) {
            liveCandleBuilder = new LiveCandleBuilder(currentSymbol, currentInterval);
        }
        liveCandleBuilder.initializeFromHistorical(traces[0].x);
    }

    // ... rest of code ...
});
```

#### Step 2D: Handle Symbol/Interval Changes

When user changes symbol or interval, reset the builder:

```javascript
symbolSelect.addEventListener('change', function() {
    // ... existing code ...
    
    // NEW: Stop current builder and create new one for new symbol
    if (liveCandleBuilder) {
        liveCandleBuilder.stop();
    }
    liveCandleBuilder = null;
    
    // ... rest of code ...
});

intervalSelect.addEventListener('change', function() {
    // ... existing code ...
    
    // NEW: Stop current builder and create new one for new interval
    if (liveCandleBuilder) {
        liveCandleBuilder.stop();
    }
    liveCandleBuilder = null;
    
    // ... rest of code ...
});
```

---

### Phase 3: Integration with Data Load

#### Step 3A: Fetch and Merge Data on Page Load

Modify the chart loading logic to get both historical and live:

```javascript
async function loadChartDataMerged(symbol, interval, /* other params */) {
    // Fetch historical data
    const historicalResp = await fetch(`/data?symbol=${symbol}&interval=${interval}&...`);
    const historicalData = await historicalResp.json();

    // Fetch live candle
    const liveResp = await fetch(`/live-candle?symbol=${symbol}&interval=${interval}`);
    const liveData = await liveResp.json();

    // Merge: append or replace
    const times = [...historicalData.time];
    const opens = [...historicalData.open];
    const highs = [...historicalData.high];
    const lows = [...historicalData.low];
    const closes = [...historicalData.close];

    if (liveData.candle) {
        const liveTime = new Date(liveData.candle.time).toISOString();
        const lastHistTime = times[times.length - 1];

        if (liveTime > lastHistTime) {
            // Append: live candle is in future (next interval)
            times.push(liveTime);
            opens.push(liveData.candle.open);
            highs.push(liveData.candle.high);
            lows.push(liveData.candle.low);
            closes.push(liveData.candle.close);
        } else if (liveTime === lastHistTime) {
            // Replace: live candle updates the last historical candle
            times[times.length - 1] = liveTime;
            opens[opens.length - 1] = liveData.candle.open;
            highs[highs.length - 1] = liveData.candle.high;
            lows[lows.length - 1] = liveData.candle.low;
            closes[closes.length - 1] = liveData.candle.close;
        }
    }

    // Plot with merged data
    plotChart(times, opens, highs, lows, closes);
}
```

---

## No Changes Needed to background_fetcher.py!

Your existing code:
```python
pipe.zadd(zset_key, {member: rounded_ts}, nx=True)
```

Works PERFECTLY because:
- ✅ NX flag protects against duplicates
- ✅ Binance official data is safe
- ✅ Live candles are in different key
- ✅ No interference possible

---

## Testing Checklist

- [ ] Live prices update in real-time
- [ ] Candle OHLC updates smoothly
- [ ] Chart shows merged historical + live candles
- [ ] Background fetcher still inserts Binance data (check logs)
- [ ] When official candle arrives, chart updates automatically
- [ ] Symbol/interval changes reset live candle builder
- [ ] Live candle expires after 24h (Redis TTL)
- [ ] No data loss or corruption

---

## Summary

| Component | Change | Reason |
|-----------|--------|--------|
| background_fetcher.py | + save_live_candle() | Store temp live data |
| app.py | + /live-candle GET | Retrieve live candle |
| app.py | + /save-live-candle POST | Save live candle |
| app.py | + metadata to /data | Show gap info |
| main.js | + LiveCandleBuilder class | Build candles from prices |
| main.js | integrate with socket.on | Update builder on prices |
| main.js | merge data logic | Show historical + live |
| Redis | live_candle:{symbol}:{interval} | NEW key for temp storage |

**Key principle:** SEPARATE storage keys prevent interference!
