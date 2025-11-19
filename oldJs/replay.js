let isReplaying = false;
let replayIntervalId = null;
let currentReplayTimeSec = 0;
let replayStartTimeSec = 0;
let replayToTimeSec = 0;
let replayResolution = '';
let replaySymbol = '';
let replaySpeedSetting = 1;

async function fetchNextReplayCandle() {
    if (!isReplaying || currentReplayTimeSec >= replayToTimeSec) {
        stopReplay(currentReplayTimeSec >= replayToTimeSec ? "Reached end of replay period." : "Replay stopped.");
        return;
    }

    const timeframeSeconds = getTimeframeSecondsJS(replayResolution); // Assumes getTimeframeSecondsJS is global
    if (!timeframeSeconds) {
        console.error("Replay: Invalid resolution for timeframe calculation.");
        stopReplay("Invalid resolution.");
        return;
    }

    const from_ts = currentReplayTimeSec;
    const to_ts = currentReplayTimeSec + timeframeSeconds - 1;

    // 1. Fetch Candlestick Data
    const historyUrl = `/history?symbol=${replaySymbol}&resolution=${replayResolution}&from_ts=${from_ts}&to_ts=${to_ts}&simulation=true`;
    try {
        const response = await fetch(historyUrl);
        if (!response.ok) throw new Error(`History fetch failed: ${response.status} ${await response.text()}`);
        const candleData = await response.json();

        if (candleData && candleData.s === 'ok' && candleData.t && candleData.t.length > 0) {
            if (window.gd && window.gd.data && window.gd.data[0]) {
                Plotly.extendTraces(window.gd, {
                    x: [[new Date(candleData.t[0] * 1000)]], open: [[candleData.o[0]]], high: [[candleData.h[0]]], low: [[candleData.l[0]]], close: [[candleData.c[0]]],
                }, [0]);
            }
        } else {
            const candleDateStr = new Date(from_ts * 1000).toISOString().split('T')[0];
            console.warn(`Replay: OHLC data MISSING for candle on ${candleDateStr} (${replaySymbol} ${replayResolution}). Server response:`, candleData);
        }
    } catch (error) {
        console.error("Replay: Error fetching candle data:", error);
        stopReplay("Error fetching candle data.");
        return;
    }

    // 2. Fetch Indicator Data
    const activeIndicatorCheckboxes = document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]:checked');
    let currentIndicatorTraceBaseIndex = 1;

    if (activeIndicatorCheckboxes.length > 0) {
        const activeIndicatorIds = Array.from(activeIndicatorCheckboxes).map(cb => cb.value);
        const indicatorUrl = `/indicatorHistory?symbol=${replaySymbol}&resolution=${replayResolution}&from_ts=${from_ts}&to_ts=${to_ts}&indicator_id=${activeIndicatorIds.join(',')}&simulation=true`;

        try {
            const response = await fetch(indicatorUrl);
            if (!response.ok) throw new Error(`Combined indicator fetch failed: ${response.status} ${await response.text()}`);
            const allIndicatorsResponse = await response.json();

            if (allIndicatorsResponse && allIndicatorsResponse.s === 'ok' && allIndicatorsResponse.data) {
                activeIndicatorCheckboxes.forEach(checkbox => {
                    const indicatorId = checkbox.value;
                    const indicatorResult = allIndicatorsResponse.data[indicatorId];

                    if (indicatorResult && indicatorResult.s === 'ok' && indicatorResult.t && indicatorResult.t.length > 0) {
                        const xVal = [new Date(indicatorResult.t[0] * 1000)];
                        if (indicatorId === 'macd') {
                            if (indicatorResult.signal && indicatorResult.signal.length > 0) Plotly.extendTraces(window.gd, { x: [xVal], y: [[indicatorResult.signal[0]]] }, [currentIndicatorTraceBaseIndex]);
                            currentIndicatorTraceBaseIndex++;
                        } else if (indicatorId === 'rsi') {
                            if (indicatorResult.rsi && indicatorResult.rsi.length > 0) Plotly.extendTraces(window.gd, { x: [xVal], y: [[indicatorResult.rsi[0]]] }, [currentIndicatorTraceBaseIndex]);
                            currentIndicatorTraceBaseIndex++;
                        } else if (indicatorId.startsWith('stochrsi')) {
                            if (indicatorResult.stoch_d && indicatorResult.stoch_d.length > 0) Plotly.extendTraces(window.gd, { x: [xVal], y: [[indicatorResult.stoch_d[0]]] }, [currentIndicatorTraceBaseIndex]);
                            currentIndicatorTraceBaseIndex++;
                        } else if (indicatorId === 'open_interest') {
                            if (indicatorResult.open_interest && indicatorResult.open_interest.length > 0) Plotly.extendTraces(window.gd, { x: [xVal], y: [[indicatorResult.open_interest[0]]] }, [currentIndicatorTraceBaseIndex]);
                            currentIndicatorTraceBaseIndex++;
                        }
                    } else {
                        console.warn(`Replay: No data or error for indicator ${indicatorId}.`);
                        currentIndicatorTraceBaseIndex++;
                    }
                });
            } else {
                console.error("Replay: Error in combined indicator response structure.", allIndicatorsResponse);
                activeIndicatorCheckboxes.forEach(() => currentIndicatorTraceBaseIndex++);
            }
        } catch (error) {
            console.error(`Replay: Error fetching combined indicator data:`, error);
            activeIndicatorCheckboxes.forEach(() => currentIndicatorTraceBaseIndex++);
        }
    }

    // 3. Update X-axis & Y-axes
    if (window.gd && window.gd.data && window.gd.data[0] && window.gd.data[0].x.length > 0) {
        const mainTraceX = window.gd.data[0].x;
        const visibleCandles = 100;
        const lastPlottedCandleTime = mainTraceX[mainTraceX.length - 1].getTime();
        const windowEndTime = new Date(lastPlottedCandleTime + timeframeSeconds * 1000);
        let windowStartTime = mainTraceX.length < visibleCandles ? mainTraceX[0] : mainTraceX[mainTraceX.length - visibleCandles];

        const layoutUpdate = { 'xaxis.range': [windowStartTime, windowEndTime] };
        if (window.gd.layout.yaxis) layoutUpdate['yaxis.autorange'] = true;

        let indicatorSubplotIndex = 2;
        activeIndicatorCheckboxes.forEach(() => {
            if (window.gd.layout[`yaxis${indicatorSubplotIndex}`]) {
                layoutUpdate[`yaxis${indicatorSubplotIndex}.autorange`] = true;
            }
            indicatorSubplotIndex++;
        });
        Plotly.relayout(window.gd, layoutUpdate);
    }

    // 4. Advance replay time
    currentReplayTimeSec += timeframeSeconds;
}

function stopReplay(reason = "User stopped replay.") {
    if (!isReplaying && !replayIntervalId) return;
    isReplaying = false;
    clearInterval(replayIntervalId);
    replayIntervalId = null;

    window.startReplayButton.disabled = false; // Assumes startReplayButton is global
    window.stopReplayButton.disabled = true;  // Assumes stopReplayButton is global
    window.symbolSelect.disabled = false;
    window.resolutionSelect.disabled = false;
    window.rangeSelect.disabled = false;
    document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]').forEach(cb => cb.disabled = false);
    document.getElementById('replay-from').disabled = false;
    document.getElementById('replay-to').disabled = false;
    document.getElementById('replay-speed').disabled = false;
}

async function initializeReplayControls() {
    window.startReplayButton.addEventListener('click', async () => {
        if (isReplaying) return;
        const fromDateStr = document.getElementById('replay-from').value;
        const toDateStr = document.getElementById('replay-to').value;
        replaySpeedSetting = parseFloat(document.getElementById('replay-speed').value) || 1;

        if (!fromDateStr || !toDateStr) {
            alert("Please select 'Replay From' and 'Replay To' dates.");
            return;
        }

        replayStartTimeSec = Math.floor(new Date(fromDateStr).getTime() / 1000);
        replayToTimeSec = Math.floor(new Date(toDateStr).getTime() / 1000);

        if (isNaN(replayStartTimeSec) || isNaN(replayToTimeSec)) {
            alert("Invalid date format for replay period.");
            return;
        }
        if (replayToTimeSec <= replayStartTimeSec) {
            alert("'Replay To' date must be after 'Replay From' date.");
            return;
        }

        replaySymbol = window.symbolSelect.value;
        replayResolution = window.resolutionSelect.value;
        const timeframeSeconds = getTimeframeSecondsJS(replayResolution);
        if (!timeframeSeconds) {
            alert("Invalid resolution selected for replay.");
            return;
        }


        isReplaying = true;
        currentReplayTimeSec = replayStartTimeSec;

        // Disable UI controls
        window.startReplayButton.disabled = true;
        window.stopReplayButton.disabled = false;
        window.symbolSelect.disabled = true;
        window.resolutionSelect.disabled = true;
        window.rangeSelect.disabled = true;
        document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]').forEach(cb => cb.disabled = true);
        document.getElementById('replay-from').disabled = true;
        document.getElementById('replay-to').disabled = true;
        document.getElementById('replay-speed').disabled = true;


        // Prepare initial chart view: Load data up to replayStartTimeSec
        const lookbackCandles = 100; // Number of candles to show before replay starts
        const initialFetchStartTs = replayStartTimeSec - (lookbackCandles * timeframeSeconds);
        const initialFetchEndTs = replayStartTimeSec - 1; // Fetch up to the candle *before* the first replay candle

        try {
            // Use a simplified version of updateChart or direct Plotly calls for initial setup
            // For simplicity, we'll fetch and plot initial candles, then start the replay interval.
            // A more complete solution would use chartUpdater's logic for indicators too.
            const historyUrl = `/history?symbol=${replaySymbol}&resolution=${replayResolution}&from_ts=${initialFetchStartTs}&to_ts=${initialFetchEndTs}&simulation=true`;
            const response = await fetch(historyUrl);
            if (!response.ok) throw new Error(`Initial history fetch failed: ${response.status}`);
            const initialData = await response.json();

            const initialTrace = {
                x: initialData.s === 'ok' ? initialData.t.map(t => new Date(t * 1000)) : [],
                open: initialData.s === 'ok' ? initialData.o : [],
                high: initialData.s === 'ok' ? initialData.h : [],
                low: initialData.s === 'ok' ? initialData.l : [],
                close: initialData.s === 'ok' ? initialData.c : [],
                type: 'candlestick', name: replaySymbol
            };
            Plotly.react(window.gd, [initialTrace], { ...layout, xaxis: {...layout.xaxis, autorange: true}, yaxis: {...layout.yaxis, autorange: true} }, config); // Use global layout & config

            replayIntervalId = setInterval(fetchNextReplayCandle, Math.max(50, 1000 / replaySpeedSetting)); // Ensure interval is not too fast
        } catch (error) {
            console.error("Error setting up initial chart for replay:", error);
            alert("Error starting replay: " + error.message);
            stopReplay("Error during replay initialization.");
        }
    });

    window.stopReplayButton.addEventListener('click', () => stopReplay());
    window.stopReplayButton.disabled = true; // Initial state
}