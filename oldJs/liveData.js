function updateOrAddRealtimePriceLine(gd, price, candleStartTimeMs, candleEndTimeMs, doRelayout = false) {
    if (!gd || !gd.layout) {
        console.warn("[PriceLine] Chart or layout not ready.");
        return;
    }

    let shapes = gd.layout.shapes || [];
    let annotations = gd.layout.annotations || [];

    shapes = shapes.filter(shape => shape.name !== CROSSHAIR_VLINE_NAME); // Assumes CROSSHAIR_VLINE_NAME is global

    let lineIndex = shapes.findIndex(shape => shape.name === REALTIME_PRICE_LINE_NAME); // Assumes REALTIME_PRICE_LINE_NAME is global
    try {
        // Determine the correct y-axis reference for the price chart
        // When indicators are present, price chart is on yaxis1, otherwise on y
        const hasIndicators = gd && gd.data && gd.data.some(trace => trace.type !== 'candlestick');
        const yref = hasIndicators ? 'yaxis1' : 'y';


        const lineDefinition = {
                type: 'line',
                name: REALTIME_PRICE_LINE_NAME,
                isSystemShape: true, // Mark as a system shape
                yref: yref,
                x0ref: 'x',
                x1ref: 'paper',
                x0: new Date(candleEndTimeMs),
                y0: price,
                x1: 1,
                y1: price,
                line: {
                    color: 'rgba(0, 0, 0, 0.9)',
                    width: 1.5,
                    dash: 'solid'
                },
                layer: 'above',
                editable: false // Explicitly make this system shape not editable by Plotly
        };

        if (typeof price !== 'number' || isNaN(price)) {
            console.error("[PriceLine] Invalid price for annotation:", price, "(type:", typeof price, ")");
            // Optionally remove any existing price annotation if the new price is invalid
            gd.layout.annotations = gd.layout.annotations.filter(ann => ann.name !== REALTIME_PRICE_TEXT_ANNOTATION_NAME);
            if (doRelayout) Plotly.relayout(gd, { shapes: gd.layout.shapes, annotations: gd.layout.annotations });
            return;
        }

        const annotationDefinition = {
            name: REALTIME_PRICE_TEXT_ANNOTATION_NAME, // From config.js
            text: price.toFixed(2), // Format the price, adjust precision as needed
            xref: 'paper',   // Relative to the entire plotting area
            yref: yref,      // Use the same y-axis reference as the line
            x: 1,            // Position slightly to the right of the plot area (e.g., 101%)
            y: price,        // Y position is the price itself
            showarrow: false,
            xanchor: 'right', // Anchor the text from its left side at the x-coordinate
            yanchor: 'middle',// Vertically center the text at the y-coordinate (price)
            font: {
                family: 'Arial, sans-serif',
                size: 20, // Made the font size 2 times bigger
                color: 'rgba(0,0,0,0.9)' // Match line color or choose another
            },
            bgcolor: 'rgba(255, 255, 255, 0.6)', // Optional: slight background for readability
            borderpad: 2,
            borderwidth: 0
        };

        if (lineIndex !== -1) {
            shapes[lineIndex] = lineDefinition;
        } else {
            shapes.push(lineDefinition);
        }

       // Remove old annotation before adding new one to prevent duplicates
        const existingAnnotationIndex = gd.layout.annotations.findIndex(ann => ann.name === REALTIME_PRICE_TEXT_ANNOTATION_NAME);
        if (existingAnnotationIndex !== -1) {
            gd.layout.annotations.splice(existingAnnotationIndex, 1);
        }
        gd.layout.annotations.push(annotationDefinition);


        gd.layout.shapes = shapes;
        // gd.layout.annotations is already updated by reference


    } catch (e) {
        console.error("[PriceLine] Error during shape modification:", e);
    }

    if (doRelayout) {
        Plotly.relayout(gd, { shapes: gd.layout.shapes, annotations: gd.layout.annotations });
    }
}

function removeRealtimePriceLine(gd, doRelayout = false) {
    if (!gd || !gd.layout || !gd.layout.shapes) {
        console.warn("[PriceLine] removeRealtimePriceLine: Chart or layout not ready or no shapes to remove.");
        return false;
    }
    const initialLength = gd.layout.shapes.length;
    let annotationsChanged = false;
    if (gd.layout.annotations) {
        const initialAnnotationLength = gd.layout.annotations.length;
        gd.layout.annotations = gd.layout.annotations.filter(ann => ann.name !== REALTIME_PRICE_TEXT_ANNOTATION_NAME);
        annotationsChanged = gd.layout.annotations.length < initialAnnotationLength;
    } else {
        gd.layout.annotations = []; // Ensure it's an array if it was null/undefined

    }

    gd.layout.shapes = gd.layout.shapes.filter(shape => shape.name !== REALTIME_PRICE_LINE_NAME && shape.name !== CROSSHAIR_VLINE_NAME);
    const removed = gd.layout.shapes.length < initialLength;

    if ((removed || annotationsChanged) && doRelayout) {
        Plotly.relayout(gd, { shapes: gd.layout.shapes, annotations: gd.layout.annotations });
    }
    return removed;
}

function closeWebSocket(reason = "Closing WebSocket") {
    if (liveWebSocket) { // Assumes liveWebSocket is global from state.js
        liveWebSocket.onclose = null;
        liveWebSocket.close(1000, reason);
        liveWebSocket = null;
        currentSymbolForStream = '';
        const gd = document.getElementById('chart'); // Or window.gd
        if (gd) {
            removeRealtimePriceLine(gd, true);
        } else {
            console.warn("closeWebSocket: Chart div 'gd' not found, cannot remove price line.");
        }
    }
}

function handleRealtimeKline(klineData) {
    if (!klineData || typeof klineData.time === 'undefined') {
        console.warn('WebSocket: Received invalid kline data:', klineData);
        return;
    }

    const gd = window.gd; // Assumes window.gd is set in main.js
    if (!gd) {
        console.error("handleRealtimeKline: Chart div 'gd' not found. Cannot proceed.");
        return;
    }

    // Lenient initialization check - just ensure chart exists and has basic structure
    // The chart update process may temporarily have inconsistent data during updates
    if (!gd.data || !Array.isArray(gd.data)) {
        console.warn('WebSocket: Chart data structure not available or invalid, cannot apply real-time update.');
        return;
    }

    // Ensure layout exists
    if (!gd.layout) {
        console.warn('WebSocket: Chart layout not available, cannot apply real-time update.');
        return;
    }

    const currentChartResolution = window.resolutionSelect.value; // Assumes window.resolutionSelect is set
    const timeframeSeconds = getTimeframeSecondsJS(currentChartResolution); // From utils.js
    if (!timeframeSeconds) {
        console.error("WebSocket: Unknown resolution for timeframeSecondsJS:", currentChartResolution);
        return;
    }

    // Find the candlestick trace (price data) - it should be the main price chart
    let priceTraceIndex = -1;
    let trace = null;

    try {
        // Debug: Log all available traces

        // First try to find by exact symbol match
        priceTraceIndex = gd.data.findIndex(trace => trace.type === 'candlestick' && trace.name === window.symbolSelect.value);

        // If not found, try to find any candlestick trace
        if (priceTraceIndex === -1) {
            priceTraceIndex = gd.data.findIndex(trace => trace.type === 'candlestick');
            if (priceTraceIndex !== -1) {
            }
        }

        if (priceTraceIndex === -1) {
            console.warn('WebSocket: Could not find candlestick trace for price data. Symbol:', window.symbolSelect.value);
            return;
        }

        trace = gd.data[priceTraceIndex];
    } catch (e) {
        console.warn('WebSocket: Error accessing chart data, skipping update:', e.message);
        return;
    }

    const liveUpdateTimeSec = Number(klineData.time);
    const livePrice = parseFloat(klineData.price !== undefined ? klineData.price : klineData.close);
    const livePriceFromRedis = klineData.live_price ? parseFloat(klineData.live_price) : null;

    if (isNaN(livePrice) || isNaN(liveUpdateTimeSec)) {
        console.warn("WebSocket: Invalid price or time in live data", klineData);
        return;
    }

    // Additional safety check for trace data
    if (!trace || !trace.x || trace.x.length === 0) {
        console.warn('WebSocket: Trace data not ready, skipping update');
        return;
    }

    const lastCandleIndex = trace.x.length - 1;
    let lastCandleOpenTimeSec = -1;
    if (lastCandleIndex >= 0) {
        const lastCandleTime = trace.x[lastCandleIndex];
        lastCandleOpenTimeSec = (lastCandleTime instanceof Date) ? lastCandleTime.getTime() / 1000 : Number(lastCandleTime) / 1000;
    }

    const currentPeriodStartSec = Math.floor(liveUpdateTimeSec / timeframeSeconds) * timeframeSeconds;
    const currentPeriodStartTime = new Date(currentPeriodStartSec * 1000);
    const candleStartTimeMsForLine = currentPeriodStartSec * 1000;
    const candleEndTimeMsForLine = (currentPeriodStartSec + timeframeSeconds) * 1000;

    try {
        // Additional validation before Plotly operations
        if (isNaN(livePrice) || !isFinite(livePrice)) {
            console.error("WebSocket: Invalid live price:", livePrice);
            return;
        }

        if (isNaN(liveUpdateTimeSec) || !isFinite(liveUpdateTimeSec)) {
            console.error("WebSocket: Invalid timestamp:", liveUpdateTimeSec);
            return;
        }

        if (isNaN(currentPeriodStartSec) || !isFinite(currentPeriodStartSec)) {
            console.error("WebSocket: Invalid period start:", currentPeriodStartSec);
            return;
        }

        if (lastCandleIndex < 0 || currentPeriodStartSec > lastCandleOpenTimeSec) {
            const newCandleData = {
                x: [[currentPeriodStartTime]],
                open: [[livePrice]],
                high: [[livePrice]],
                low: [[livePrice]],
                close: [[livePrice]],
                volume: [[0]] // Assuming new candles start with 0 volume from live tick
            };

            // Validate new candle data
            if (!Array.isArray(newCandleData.x) || newCandleData.x[0][0] === undefined) {
                console.error("WebSocket: Invalid new candle x data");
                return;
            }

            // Use live price from Redis if available, otherwise use the WebSocket price
            const priceToUse = livePriceFromRedis !== null ? livePriceFromRedis : livePrice;
            // updateOrAddRealtimePriceLine(gd, priceToUse, candleStartTimeMsForLine, candleEndTimeMsForLine, false);
            // Plotly.extendTraces(gd, newCandleData, [priceTraceIndex], MAX_LIVE_CANDLES);
            // Plotly.relayout(gd, { shapes: gd.layout.shapes, annotations: gd.layout.annotations });
        } else if (currentPeriodStartSec === lastCandleOpenTimeSec) {
            const candleTrace = gd.data[priceTraceIndex];

            // Validate existing candle data
            if (!candleTrace || !Array.isArray(candleTrace.high) || !Array.isArray(candleTrace.low) || !Array.isArray(candleTrace.close)) {
                console.error("WebSocket: Invalid candle trace data");
                return;
            }

            if (lastCandleIndex >= candleTrace.high.length) {
                console.error("WebSocket: Candle index out of bounds");
                return;
            }

            const currentHigh = candleTrace.high[lastCandleIndex];
            const currentLow = candleTrace.low[lastCandleIndex];
            const prevPrice = candleTrace.close[lastCandleIndex];

            if (isNaN(currentHigh) || isNaN(currentLow)) {
                console.error("WebSocket: Invalid existing candle high/low values");
                return;
            }

            candleTrace.high[lastCandleIndex] = Math.max(currentHigh, livePrice);
            candleTrace.low[lastCandleIndex] = Math.min(currentLow, livePrice);
            candleTrace.close[lastCandleIndex] = livePrice;


            // Use live price from Redis if available, otherwise use the WebSocket price
            const priceToUse = livePriceFromRedis !== null ? livePriceFromRedis : livePrice;
            updateOrAddRealtimePriceLine(gd, priceToUse, candleStartTimeMsForLine, candleEndTimeMsForLine, false);

            // Ensure arrays are copied to trigger reactivity
            // candleTrace.high = [...candleTrace.high];
            // candleTrace.low = [...candleTrace.low];
            // candleTrace.close = [...candleTrace.close];

            Plotly.react(gd, gd.data, gd.layout).then(() => {
                // Re-add trade history markers after live data update
                if (window.tradeHistoryData && window.tradeHistoryData.length > 0 && window.updateTradeHistoryVisualizations) {
                    window.updateTradeHistoryVisualizations();
                }
            });
        } else {
            const message = `Ignored out-of-sequence live tick. Tick time: ${new Date(liveUpdateTimeSec * 1000).toLocaleString()}, Last chart candle: ${new Date(lastCandleOpenTimeSec * 1000).toLocaleString()}`;
            console.warn('[LiveUpdate] ' + message, 'Raw Data:', klineData);
            logEventToPanel(`Live Data Warning: ${message}. Raw: ${JSON.stringify(klineData)}`, 'WARN');
        }
    } catch (plotlyError) {
        console.error("WebSocket: Error during Plotly update:", plotlyError, "Data:", klineData);
        // Don't rethrow - we want to continue processing other messages
    }
}

function setupWebSocket(symbolToStream) {
    // Live data is always enabled now

    if (liveWebSocket) { // Assumes liveWebSocket is global from state.js
        if (currentSymbolForStream !== symbolToStream || liveWebSocket.readyState !== WebSocket.OPEN) { // Assumes currentSymbolForStream is global
            closeWebSocket(`Switching to new symbol ${symbolToStream} or re-establishing.`);
        } else {
            return;
        }
    }

    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsHost = window.location.host;
    const streamUrl = `${wsProtocol}//${wsHost}/stream/live/${symbolToStream}`;

    liveWebSocket = new WebSocket(streamUrl);

    liveWebSocket.onopen = () => {
        currentSymbolForStream = symbolToStream;
    };

    liveWebSocket.onmessage = (event) => {
        try {
            if (symbolToStream !== currentSymbolForStream) { // currentSymbolForStream from state.js
                console.warn(`WebSocket: Message received for ${symbolToStream}, but current active stream is for ${currentSymbolForStream}. Discarding.`);
                return;
            }

            // Validate event.data before parsing
            if (!event.data || typeof event.data !== 'string') {
                console.warn("WebSocket: Received invalid event.data:", event.data);
                return;
            }

            const klineData = JSON.parse(event.data);

            // Additional validation of parsed data
            if (!klineData || typeof klineData !== 'object') {
                console.warn("WebSocket: Parsed data is not a valid object:", klineData);
                return;
            }

            // Validate required fields
            if (typeof klineData.symbol !== 'string' || typeof klineData.time !== 'number' ||
                (typeof klineData.price !== 'number' && typeof klineData.close !== 'number')) {
                console.warn("WebSocket: Invalid kline data structure:", klineData);
                return;
            }

            handleRealtimeKline(klineData);
        } catch (e) {
            console.error("WebSocket: Error parsing message data", e, "Raw data:", event.data);
        }
    };

    liveWebSocket.onerror = (error) => {
        console.error(`WebSocket: Live stream error for ${symbolToStream}:`, error);
    };

    liveWebSocket.onclose = (event) => {

        // Check if this is the WebSocket instance we care about before nullifying
        if (liveWebSocket === event.target) {
            liveWebSocket = null; // From state.js
            if (currentSymbolForStream === symbolToStream) { // currentSymbolForStream from state.js
                currentSymbolForStream = '';
            }
        }

        // Attempt to reconnect if not a clean close and symbol matches
        if (event.code !== 1000 && window.symbolSelect.value === symbolToStream && !liveWebSocket) {
            setTimeout(() => {
                if (window.symbolSelect.value === symbolToStream && !liveWebSocket) {
                    setupWebSocket(symbolToStream);
                } else {
                }
            }, 5000);
        }
    };
}
