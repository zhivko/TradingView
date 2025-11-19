async function debounce(func, delayMs) {
    let timeoutId;
    return function(...args) {
        clearTimeout(timeoutId);
        timeoutId = setTimeout(async () => {
            await func.apply(this, args);
        }, delayMs);
    };
}

function getAxisLayoutKey(axisRef, axisTypePrefix) { // axisTypePrefix is 'xaxis' or 'yaxis'
    if (!axisRef) { // Undefined, null, or empty string
        return axisTypePrefix; // Default to 'xaxis' or 'yaxis'
    }
    if (axisRef === axisTypePrefix[0]) { // 'x' or 'y'
        return axisTypePrefix; // 'xaxis' or 'yaxis'
    }
    // Handles 'x2', 'y3', etc. -> 'xaxis2', 'yaxis3'
    if (axisRef.startsWith(axisTypePrefix[0]) && axisRef.length > 1 && !axisRef.startsWith(axisTypePrefix)) {
        return axisTypePrefix + axisRef.substring(1);
    }
    return axisRef;
}

function determineSubplotNameForShape(shape) {
    const currentSymbol = window.symbolSelect.value;
    if (!shape || !shape.yref) {
        return currentSymbol; // Default to main chart if no yref
    }
    const yrefFromShape = shape.yref; // e.g., 'y', 'y2', 'y3' when using layout.grid

    // Main chart is on 'y' (Plotly might also use 'y1' for the first axis in a grid context)
    if (yrefFromShape === 'y' || yrefFromShape === 'y1') {
        return currentSymbol;
    }

    // Indicators will have yAxisRef like 'y2', 'y3' in active_indicatorsState
    // (matching the yrefFromShape directly)
    const indicator = window.active_indicatorsState && window.active_indicatorsState.find(ind => ind.yAxisRef === yrefFromShape);
    if (indicator) {
        return `${currentSymbol}-${indicator.id}`;
    }
    // Fallback
    return currentSymbol;
}

window.populateActiveIndicatorsState = function(activeIndicatorIds) {
    // Populate active_indicatorsState with correct yAxisRef mapping
    // This should be called whenever indicators change and layout is created
    window.active_indicatorsState = activeIndicatorIds.map((indicatorId, index) => ({
        id: indicatorId,
        yAxisRef: `y${index + 2}` // y2, y3, y4, etc. matching Plotly layout
    }));

};

window.determineSubplotNameForShape = determineSubplotNameForShape; // Export to global scope

function distSq(p1, p2) {
    return (p1.x - p2.x)**2 + (p1.y - p2.y)**2;
}

function distToSegmentSquared(p, v, w) {
    const l2 = distSq(v, w);
    if (l2 === 0) return distSq(p, v);
    let t = ((p.x - v.x) * (w.x - v.x) + (p.y - v.y) * (w.y - v.y)) / l2;
    t = Math.max(0, Math.min(1, t));
    const projection = { x: v.x + t * (w.x - v.x), y: v.y + t * (w.y - v.y) };
    return distSq(p, projection);
}

function getTimeframeSecondsJS(resolution) {
    const multipliers = {"1m": 60, "5m": 5 * 60, "1h": 60 * 60, "1d": 24 * 60 * 60, "1w": 7 * 24 * 60 * 60 };
    return multipliers[resolution];
}

// Promise-based delay function to replace setTimeout
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Make delay globally available
window.delay = delay;

// Debug function to export Plotly data as CSV - available immediately
window.exportPlotlyDataAsCSV = function() {

    if (!window.gd || !window.gd.data) {
        return;
    }

    try {
        // Prepare CSV data
        const csvRows = [];
        const headers = ['timestamp', 'iso_timestamp'];

        // Add OHLC data headers
        headers.push('open', 'high', 'low', 'close', 'volume');

        // Add indicator headers
        const allIndicatorNames = [];
        window.gd.data.forEach(trace => {
            if (trace.name === 'MACD') allIndicatorNames.push('macd');
            else if (trace.name === 'MACD Signal') allIndicatorNames.push('macd_signal');
            else if (trace.name === 'MACD Histogram') allIndicatorNames.push('macd_histogram');
            else if (trace.name === 'RSI') allIndicatorNames.push('rsi');
            else if (trace.name === 'RSI_SMA14') allIndicatorNames.push('rsi_sma14');
            else if (trace.name.startsWith('Stoch K')) allIndicatorNames.push('stoch_k');
            else if (trace.name.startsWith('Stoch D')) allIndicatorNames.push('stoch_d');
        });

        allIndicatorNames.forEach(name => headers.push(name));

        csvRows.push(headers.join(','));

        // Get price trace
        const priceTrace = window.gd.data.find(t => t.type === 'candlestick');
        if (!priceTrace || !priceTrace.x) {
            return;
        }

        // Process each data point
        for (let i = 0; i < priceTrace.x.length; i++) {
            const timestamp = Math.floor(priceTrace.x[i].getTime() / 1000); // Convert to seconds
            const isoTimestamp = priceTrace.x[i].toISOString();

            const row = [timestamp, isoTimestamp];

            // Add OHLC data
            row.push(
                priceTrace.open[i] || '',
                priceTrace.high[i] || '',
                priceTrace.low[i] || '',
                priceTrace.close[i] || '',
                priceTrace.volume[i] || ''
            );

            // Add indicator data
            allIndicatorNames.forEach(indicatorName => {
                let value = '';

                // Find corresponding indicator trace
                let targetTrace = null;
                if (indicatorName === 'macd') {
                    targetTrace = window.gd.data.find(t => t.name === 'MACD');
                } else if (indicatorName === 'macd_signal') {
                    targetTrace = window.gd.data.find(t => t.name === 'MACD Signal');
                } else if (indicatorName === 'macd_histogram') {
                    targetTrace = window.gd.data.find(t => t.name === 'MACD Histogram');
                } else if (indicatorName === 'rsi') {
                    targetTrace = window.gd.data.find(t => t.name === 'RSI');
                } else if (indicatorName === 'rsi_sma14') {
                    targetTrace = window.gd.data.find(t => t.name === 'RSI_SMA14');
                } else if (indicatorName === 'stoch_k') {
                    targetTrace = window.gd.data.find(t => t.name.startsWith('Stoch K'));
                } else if (indicatorName === 'stoch_d') {
                    targetTrace = window.gd.data.find(t => t.name.startsWith('Stoch D'));
                }

                if (targetTrace && targetTrace.y) {
                    // Check if the current index is valid for this trace
                    if (i < targetTrace.y.length) {
                        const traceValue = targetTrace.y[i];
                        // If value is a number or valid value, use it; otherwise empty string
                        value = (typeof traceValue === 'number' && !isNaN(traceValue)) ? traceValue : '';
                    } else {
                        // Index out of bounds - likely due to filtering removing early data points
                        value = '';
                    }
                } else {
                    value = '';
                }

                row.push(value);
            });

            csvRows.push(row.join(','));
        }

        // Create and download CSV
        const csvContent = csvRows.join('\n');
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');

        if (link.download !== undefined) {
            const url = URL.createObjectURL(blob);
            link.setAttribute('href', url);
            const filename = `plotly_data_${window.symbolSelect ? window.symbolSelect.value : 'trading'}_${new Date().getTime()}.csv`;
            link.setAttribute('download', filename);
            link.style.visibility = 'hidden';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);

        } else {
        }

        return csvContent;

    } catch (error) {
        console.error('❌ Error exporting CSV:', error);
        return null;
    }
};

