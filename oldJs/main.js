document.addEventListener('DOMContentLoaded', async () => {
    // Make DOM elements globally available (or pass them as needed)
    window.symbolSelect = document.getElementById('symbol-select');
    window.resolutionSelect = document.getElementById('resolution-select');
    window.rangeSelect = document.getElementById('range-select');
    window.chartDiv = document.getElementById('chart');
    window.eventOutput = document.getElementById('event-output');
    window.xAxisMinDisplay = document.getElementById('x-axis-min-display');
    window.xAxisMaxDisplay = document.getElementById('x-axis-max-display');
    window.yAxisMinDisplay = document.getElementById('y-axis-min-display');
    window.yAxisMaxDisplay = document.getElementById('y-axis-max-display');
    window.selectedShapeInfoDiv = document.getElementById('selected-shape-info');
    window.deleteShapeBtn = document.getElementById('delete-shape-btn');
    window.editShapeBtn = document.getElementById('edit-shape-btn');
    window.deleteAllShapesBtn = document.getElementById('delete-all-shapes-btn');
    window.cursorTimeDisplay = document.getElementById('cursor-time-display');
    window.cursorPriceDisplay = document.getElementById('cursor-price-display');
    window.startReplayButton = document.getElementById('start-replay'); 
    window.stopReplayButton = document.getElementById('stop-replay');
    window.aiSuggestionButton = document.getElementById('ai-suggestion-btn');
    window.aiSuggestionTextarea = document.getElementById('ai-suggestion');
    window.useLocalOllamaCheckbox = document.getElementById('use-local-ollama-checkbox');
    window.localOllamaModelDiv = document.getElementById('local-ollama-model-selection-div');
    window.localOllamaModelSelect = document.getElementById('local-ollama-model-select');

    // Programmatic setting flag to prevent event handlers from running when settings are applied programmatically
    window.isProgrammaticallySetting = false;

    // Stream delta slider elements
    window.streamDeltaSlider = document.getElementById('stream-delta-slider');
    window.streamDeltaValueDisplay = document.getElementById('stream-delta-value-display');

    // Trade filter slider elements
    window.minValueSlider = document.getElementById('min-value-slider');
    window.minValueDisplay = document.getElementById('min-volume-value');

    // Timer for debouncing min value changes
    window.minValueChangeTimeout = null;

    // Initialize max trade value for filtering
    window.maxTradeValue = 0;

// Initialize trade filter slider to 0 by default to show all trades
    if (window.minValueSlider) {
        // Set slider properties for percentage-based filtering (0-1 range, 0.1 steps)
        window.minValueSlider.min = 0;
        window.minValueSlider.max = 1; // 1 (100%) default max
        window.minValueSlider.step = 0.1;
        window.minValueSlider.value = 0; // Always start at 0 to show all trades
        updateMinValueDisplay(); // Sync display with initial value
        updateMaxValueDisplay(); // Sync max display with initial range

        // Trade filter slider event listener
        if (minValueSlider) {
            minValueSlider.addEventListener('input', handleMinValueChange);
            minValueSlider.value = 0; // Set default to 0
            updateMinValueDisplay(); // Sync display with initial value}
        }
    }

    // Trade visualization checkbox elements
    window.showVolumeProfileCheckbox = document.getElementById('show-volume-profile-checkbox');
    window.showTradeMarkersCheckbox = document.getElementById('show-trade-markers-checkbox');
    window.enableTradeTraceCheckbox = document.getElementById('enable-trade-trace-checkbox');
    window.enableMyTradesCheckbox = document.getElementById('enable-my-trades-checkbox');

    // Sync the trade trace checkboxes so they are the same
    document.getElementById('enable-trade-trace-checkbox').addEventListener('change', (e) => {
      document.getElementById('show-trade-markers-checkbox').checked = e.target.checked;
    });
    document.getElementById('show-trade-markers-checkbox').addEventListener('change', (e) => {
      document.getElementById('enable-trade-trace-checkbox').checked = e.target.checked;
    });

    // Initialize debounced functions
    window.debouncedUpdateShapeVisuals = debounce(updateShapeVisuals, VISUAL_UPDATE_DEBOUNCE_DELAY); // VISUAL_UPDATE_DEBOUNCE_DELAY from config.js
    // Crosshair functionality disabled due to panning interference
    window.debouncedUpdateCrosshair = () => {}; // No-op function

    // Bar limit configuration
    const MAX_BARS = 500;

    // Resolution to seconds mapping
    const RESOLUTION_SECONDS = {
        '1m': 60,
        '5m': 300,
        '1h': 3600,
        '1d': 86400,
        '1w': 604800
    };

    // Range to seconds mapping
    const RANGE_SECONDS = {
        '1h': 3600,
        '8h': 8 * 3600,
        '24h': 24 * 3600,
        '3d': 3 * 24 * 3600,
        '7d': 7 * 24 * 3600,
        '30d': 30 * 24 * 3600,
        '3m': 90 * 24 * 3600,  // 3 months approx
        '6m': 180 * 24 * 3600, // 6 months approx
        '1y': 365 * 24 * 3600, // 1 year approx
        '3y': 3 * 365 * 24 * 3600 // 3 years approx
    };

    // Function to calculate number of bars for given time range and resolution
    function calculateBars(timeRangeSeconds, resolution) {
        const resolutionSeconds = RESOLUTION_SECONDS[resolution];
        if (!resolutionSeconds) {
            console.warn(`Unknown resolution: ${resolution}`);
            return 0;
        }
        return Math.ceil(timeRangeSeconds / resolutionSeconds);
    }

    // Function to adjust time range to limit bars to MAX_BARS
    function adjustTimeRangeForMaxBars(fromTs, toTs, resolution) {
        const timeRangeSeconds = (toTs - fromTs) / 1000; // Convert to seconds
        const bars = calculateBars(timeRangeSeconds, resolution);

        if (bars <= MAX_BARS) {
            return { fromTs, toTs, bars };
        }

        // Calculate new fromTs to get exactly MAX_BARS
        const resolutionSeconds = RESOLUTION_SECONDS[resolution];
        const newTimeRangeSeconds = MAX_BARS * resolutionSeconds;
        const newFromTs = toTs - (newTimeRangeSeconds * 1000); // Convert back to milliseconds


        return { fromTs: newFromTs, toTs, bars: MAX_BARS };
    }

    // 🔧 TIMESTAMP SYNCHRONIZATION HELPER FUNCTION
    window.getSynchronizedTimestamps = function(fromTs, toTs) {
        // Convert to ISO timestamp strings for WebSocket
        let wsFromTs = fromTs;
        let wsToTs = toTs;

        // If timestamps are in milliseconds (values > 1e12), convert to ISO strings
        if (fromTs > 1e12) {
            wsFromTs = new Date(fromTs).toISOString();
        }
        if (toTs > 1e12) {
            wsToTs = new Date(toTs).toISOString();
        }

        // If we have saved X-axis range from settings, use it to ensure consistency
        if (window.currentXAxisRange && window.currentXAxisRange.length === 2) {
            wsFromTs = new Date(window.currentXAxisRange[0]).toISOString();
            wsToTs = new Date(window.currentXAxisRange[1]).toISOString();

        }

        return { fromTs: wsFromTs, toTs: wsToTs };
    };


    // Initialize Plotly chart (empty initially)
    const initialLayout = {
        dragmode: 'pan', // Set default dragmode to pan
        margin: { t: 0, b: 0, l: 0, r: 0 }, // Tight margins for more space
        newshape: {
            line: { color: DEFAULT_DRAWING_COLOR, width: 2 },
            fillcolor: 'rgba(0, 0, 255, 0.1)',
            opacity: 0.5,
            layer: 'above'
        },
        xaxis: {
            rangeslider: { visible: false },
            type: 'date',
            autorange: true
        },
        yaxis: {
            title: 'Price',
            autorange: true
        },
        showlegend: false
    }; // Basic layout - full layout configuration handled in combinedData.js

    // Ensure newshape is in the layout for drawing to work
    initialLayout.newshape = {
        line: { color: DEFAULT_DRAWING_COLOR, width: 2 },
        fillcolor: 'rgba(0, 0, 255, 0.1)',
        opacity: 0.5,
        layer: 'above'
    };

    console.log('[INIT] Creating Plotly chart with config:', config);
    console.log('[INIT] Initial layout:', initialLayout);

    Plotly.newPlot('chart', [], initialLayout, config).then(function(gd) { // layout & config from config.js
        window.gd = gd; // Make Plotly graph div object global
        console.log('[INIT] Plotly chart created successfully');
        console.log('[INIT] Final layout after creation:', gd.layout);
        console.log('[INIT] Final config after creation:', gd.config);

        // Check if modebar is present
        const modebarElement = document.querySelector('.js-plotly-plot .modebar');
        console.log('[INIT] Modebar element found:', !!modebarElement);
        if (modebarElement) {
            console.log('[INIT] Modebar visibility:', window.getComputedStyle(modebarElement).visibility);
            console.log('[INIT] Modebar position:', window.getComputedStyle(modebarElement).position);
        }

        // Initialize Plotly specific event handlers after chart is ready
        initializePlotlyEventHandlers(gd); // From plotlyEventHandlers.js

        // Ensure mobile hover is disabled after chart creation
        disableMobileHover(gd);
        forceHideHoverElements(); // Force hide hover elements via CSS

        // Enable mobile pinch zoom features
        enableMobilePinchZoom(gd);

        // Ensure mouse wheel zoom is ALWAYS enabled after chart creation
        // FIX: Call ensureScrollZoomEnabled immediately and also ensure it gets called on ALL chart operations
        window.ensureScrollZoomEnabled();

        // Update toolbar button states to reflect default pan mode
        updateToolbarButtonStates();

        // OVERRIDE Plotly.relayout to ensure scrollZoom stays enabled after ALL chart operations
        const originalRelayout = Plotly.relayout;
        Plotly.relayout = function(gd, update, layout) {
            return originalRelayout.apply(this, arguments).then(function(result) {
                // Always ensure mouse wheel zoom stays enabled after any relayout operation
                if (window.ensureScrollZoomEnabled) {
                    window.ensureScrollZoomEnabled();
                }
                return result;
            });
        };

        // Initialize trade history after chart is ready
        // initializeTradeHistory();

    }).catch(err => {
        console.error('[DEBUG] Plotly initialization error:', err);
        console.error('Full error details:', err);
    });
    // General UI event listeners
    window.symbolSelect.addEventListener('change', async () => {
        if (window.isProgrammaticallySetting) return;

        const selectedSymbol = window.symbolSelect.value;

        // Save the symbol change to Redis
        // saveSettingsInner();

        // Fallback to page redirect
        window.location.href = `/${selectedSymbol}`;

    }); // Replaced .onchange with addEventListener('change',...)
    window.resolutionSelect.addEventListener('change', () => {
        if (window.isProgrammaticallySetting) return;

        const newResolution = window.resolutionSelect.value;

        // Update global currentResolution for audio recording
        window.currentResolution = newResolution;

        // Get current time range or use default
        let currentFromTs, currentToTs;
        const currentTime = new Date().getTime();

        if (window.currentXAxisRange && window.currentXAxisRange.length === 2) {
            currentFromTs = window.currentXAxisRange[0];
            currentToTs = window.currentXAxisRange[1];
        } else {
            // Use default 30-day range
            currentFromTs = currentTime - 30 * 86400 * 1000;
            currentToTs = currentTime;
        }

        // Calculate bars for current time range with new resolution
        const timeRangeSeconds = (currentToTs - currentFromTs) / 1000;
        const estimatedBars = calculateBars(timeRangeSeconds, newResolution);


        // Adjust time range if bars exceed limit
        let adjustedFromTs = currentFromTs;
        let adjustedToTs = currentToTs;

        if (estimatedBars > MAX_BARS) {
            const adjusted = adjustTimeRangeForMaxBars(currentFromTs, currentToTs, newResolution);
            adjustedFromTs = adjusted.fromTs;
            adjustedToTs = adjusted.toTs;
        }

        // Update current axis range
        window.currentXAxisRange = [adjustedFromTs, adjustedToTs];
        window.xAxisMinDisplay.textContent = `${new Date(adjustedFromTs).toISOString()}`;
        window.xAxisMaxDisplay.textContent = `${new Date(adjustedToTs).toISOString()}`;

        window.currentYAxisRange = null; // Reset Y-axis range for new data
        window.yAxisMinDisplay.textContent = 'Auto';
        window.yAxisMaxDisplay.textContent = 'Auto';
        window.activeShapeForPotentialDeletion = null;
        updateSelectedShapeInfoPanel(null);

        // Clear the entire chart before changing resolution
        if (window.gd) {
            removeRealtimePriceLine(window.gd);
            // Clear all chart data and reset to empty state
            Plotly.react(window.gd, [], window.gd.layout || {});
        }

        // Update WebSocket with new resolution and adjusted time range
        const symbol = window.symbolSelect.value;
        const active_indicators = Array.from(document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]:checked')).map(cb => cb.value);
        if (symbol && newResolution) {
            updateCombinedResolution(newResolution);

            // Convert to seconds for WebSocket
            const wsFromTs = Math.floor(adjustedFromTs / 1000);
            const wsToTs = Math.floor(adjustedToTs / 1000);

            // If WebSocket is open, send new config, otherwise establish new connection
            if (window.combinedWebSocket && window.combinedWebSocket.readyState === WebSocket.OPEN) {
                setupCombinedWebSocket(symbol, active_indicators, newResolution, wsFromTs, wsToTs);
            } else {
                delay(100).then(() => {
                    setupCombinedWebSocket(symbol, active_indicators, newResolution, wsFromTs, wsToTs);
                });
            }
        }
        if (window.isProgrammaticallySetting == false)
            saveSettingsInner(); // Save the resolution change to Redis immediately
    }); // Replaced .onchange with addEventListener('change',...)
    window.rangeSelect.addEventListener('change', () => {
        if (window.isProgrammaticallySetting) return;

        const rangeDropdownValue = window.rangeSelect.value;

        // Use current time as base for range calculations (not hardcoded 2022)
        const currentTime = new Date().getTime(); // Keep in milliseconds
        let requestedFromTs;
        switch(rangeDropdownValue) {
            case '1h': requestedFromTs = currentTime - 3600 * 1000; break;
            case '8h': requestedFromTs = currentTime - 8 * 3600 * 1000; break;
            case '24h': requestedFromTs = currentTime - 86400 * 1000; break;
            case '3d': requestedFromTs = currentTime - 3 * 86400 * 1000; break;
            case '7d': requestedFromTs = currentTime - 7 * 86400 * 1000; break;
            case '30d': requestedFromTs = currentTime - 30 * 86400 * 1000; break;
            case '3m': requestedFromTs = currentTime - 90 * 86400 * 1000; break;
            case '6m': requestedFromTs = currentTime - 180 * 86400 * 1000; break;
            case '1y': requestedFromTs = currentTime - 365 * 86400 * 1000; break;
            case '3y': requestedFromTs = currentTime - 3 * 365 * 86400 * 1000; break;
            default: requestedFromTs = currentTime - 30 * 86400 * 1000;
        }
        const requestedToTs = currentTime;

        // Get current resolution
        const currentResolution = window.resolutionSelect.value;

        // Calculate bars for requested time range with current resolution
        const timeRangeSeconds = (requestedToTs - requestedFromTs) / 1000;
        const estimatedBars = calculateBars(timeRangeSeconds, currentResolution);


        // Adjust time range if bars exceed limit
        let finalFromTs = requestedFromTs;
        let finalToTs = requestedToTs;

        if (estimatedBars > MAX_BARS) {
            const adjusted = adjustTimeRangeForMaxBars(requestedFromTs, requestedToTs, currentResolution);
            finalFromTs = adjusted.fromTs;
            finalToTs = adjusted.toTs;
        }

        window.currentXAxisRange = [finalFromTs, finalToTs]; // Keep in milliseconds
        window.xAxisMinDisplay.textContent = `${new Date(finalFromTs).toISOString()}`;
        window.xAxisMaxDisplay.textContent = `${new Date(finalToTs).toISOString()}`;

        window.currentYAxisRange = null; // Reset Y-axis range for new data
        window.yAxisMinDisplay.textContent = 'Auto';
        window.yAxisMaxDisplay.textContent = 'Auto';
        window.activeShapeForPotentialDeletion = null;
        updateSelectedShapeInfoPanel(null);
        if (window.cancelRequests) window.cancelRequests("Range change initiated by user");

        // Clear the entire chart before changing range
        if (window.gd) {
            removeRealtimePriceLine(window.gd);
            // Clear all chart data and reset to empty state
            Plotly.react(window.gd, [], window.gd.layout || {});
        }

        // Update WebSocket with new time range (don't close/reopen, just send new config)
        const symbol = window.symbolSelect.value;
        const resolution = window.resolutionSelect.value;
        const active_indicators = Array.from(document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]:checked')).map(cb => cb.value);
        if (symbol && resolution) {

            // Convert to seconds for WebSocket
            const wsFromTs = Math.floor(finalFromTs / 1000);
            const wsToTs = Math.floor(finalToTs / 1000);

            // If WebSocket is open, send new config with updated time range, otherwise establish new connection
            if (window.combinedWebSocket && window.combinedWebSocket.readyState === WebSocket.OPEN) {
                setupCombinedWebSocket(symbol, active_indicators, resolution, wsFromTs, wsToTs);
            } else {
                delay(100).then(() => {
                    setupCombinedWebSocket(symbol, active_indicators, resolution, wsFromTs, wsToTs);
                });
            }
        }
        saveSettingsInner(); // Save the range change to Redis immediately
    }); // Replaced .onchange with addEventListener('change',...)


    const indicatorCheckboxes = document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]');
    indicatorCheckboxes.forEach(checkbox => {
        checkbox.addEventListener('change', () => {
            if (window.isProgrammaticallySetting === true) return;

            saveSettingsInner();

            // Update combined WebSocket with new indicators
            const symbol = window.symbolSelect.value;
            const resolution = window.resolutionSelect.value;
            const active_indicators = Array.from(document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]:checked')).map(cb => cb.value);

            if (symbol && resolution) {
                // CRITICAL FIX: Always update chart layout when indicators change
                // This ensures subplots are created/destroyed immediately when indicators are selected/deselected
                if (window.gd && window.updateLayoutForIndicators) {
                    const layout = window.updateLayoutForIndicators(active_indicators);
                    Plotly.relayout(window.gd, layout);
                }

                // Update indicators and send new config if WebSocket is connected
                updateCombinedIndicators(active_indicators);

                // If WebSocket is open, send updated config with new indicators
                if (window.combinedWebSocket && window.combinedWebSocket.readyState === WebSocket.OPEN) {
                    // Use current time for range calculations
                    const currentTime = new Date().getTime();
                    let wsFromTs = new Date(currentTime - 30 * 86400 * 1000).toISOString(); // 30 days before current time
                    let wsToTs = new Date(currentTime + 30 * 86400 * 1000).toISOString(); // 30 days after current time

                    // 🔧 FIX TIMESTAMP SYNCHRONIZATION: Use saved X-axis range if available
                    if (window.currentXAxisRange && Array.isArray(window.currentXAxisRange) && window.currentXAxisRange.length === 2) {
                        wsFromTs = new Date(window.currentXAxisRange[0]).toISOString();
                        wsToTs = new Date(window.currentXAxisRange[1]).toISOString();
                    }

                    setupCombinedWebSocket(symbol, active_indicators, resolution, wsFromTs, wsToTs);
                }
            }
        });
    });

    // Event listener for "Show Agent Trades" checkbox
    document.getElementById('showAgentTradesCheckbox').addEventListener('change', () => {
        if (window.isProgrammaticallySetting) return;

        saveSettingsInner();
        // Agent trades will be handled by the combined WebSocket when data is refreshed
        const symbol = window.symbolSelect.value;
        const resolution = window.resolutionSelect.value;
        if (symbol && resolution) {
            const active_indicators = Array.from(document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]:checked')).map(cb => cb.value);

            // Calculate time range for agent trades update
            // Use current time for range calculations
            const currentTime = new Date().getTime();
            let wsFromTs = new Date(currentTime - 30 * 86400 * 1000).toISOString(); // 30 days before current time
            let wsToTs = new Date(currentTime + 30 * 86400 * 1000).toISOString(); // 30 days after current time

            // 🔧 FIX TIMESTAMP SYNCHRONIZATION: Use saved X-axis range if available
            if (window.currentXAxisRange && Array.isArray(window.currentXAxisRange) && window.currentXAxisRange.length === 2) {
                wsFromTs = new Date(window.currentXAxisRange[0]).toISOString();
                wsToTs = new Date(window.currentXAxisRange[1]).toISOString();
            }

            setupCombinedWebSocket(symbol, active_indicators, resolution, wsFromTs, wsToTs);
        }
    });


    // Event listener for "Delete line" button
    if (window.deleteShapeBtn) {
        window.deleteShapeBtn.addEventListener('click', async () => {
            if (!window.activeShapeForPotentialDeletion || !window.activeShapeForPotentialDeletion.id) {
                alert("No shape selected for deletion.");
                return;
            }

            const symbol = window.symbolSelect.value;
            if (!symbol) {
                alert("Please select a symbol first.");
                return;
            }

            const drawingId = window.activeShapeForPotentialDeletion.id;
            // Removed confirmation dialog - delete immediately

            try {
                if (window.wsAPI && window.wsAPI.connected) {
                    await new Promise((resolve, reject) => {
                        const timeout = setTimeout(() => {
                            reject(new Error('Timeout waiting for delete response'));
                        }, 5000); // 5 second timeout

                        const requestId = Date.now().toString();

                        const messageHandler = (message) => {
                            if ((message.type === 'shape_success' || message.type === 'error') && message.request_id === requestId) {
                                clearTimeout(timeout);
                                window.wsAPI.offMessage(message.type, messageHandler);
                                if (message.type === 'shape_success' && message.data && message.data.id === drawingId) {
                                    resolve();
                                } else if (message.type === 'error') {
                                    reject(new Error(message.message || 'Failed to delete shape'));
                                }
                            }
                        };

                        // Listen for both success and error messages with the same request ID
                        window.wsAPI.onMessage('shape_success', messageHandler);
                        window.wsAPI.onMessage('error', messageHandler);

                        // Send delete shape message
                        window.wsAPI.sendMessage({
                            type: 'shape',
                            action: 'delete',
                            data: {
                                drawing_id: drawingId,
                                symbol: symbol
                            },
                            request_id: requestId
                        });
                    });
                } else {
                    throw new Error('WebSocket not connected');
                }

                // Remove the shape from the chart
                if (window.gd && window.gd.layout && window.gd.layout.shapes) {
                    const shapes = window.gd.layout.shapes.filter(shape => shape.id !== drawingId);
                    Plotly.relayout(window.gd, { shapes: shapes });
                }

                // Remove associated volume profile traces for rectangles
                if (window.gd && window.gd.data) {
                    const filteredData = window.gd.data.filter(trace =>
                        !trace.name || !trace.name.startsWith(`VP-${drawingId}`)
                    );
                    if (filteredData.length !== window.gd.data.length) {
                        window.gd.data = filteredData;
                        Plotly.react(window.gd, window.gd.data, window.gd.layout).then(() => {
                            // Re-add trade history markers after shape deletion
                            if (window.tradeHistoryData && window.tradeHistoryData.length > 0 && window.updateTradeHistoryVisualizations) {
                                addTradeHistoryMarkersToChart(filteredTrades, window.symbolSelect ? window.symbolSelect.value : 'UNKNOWN');
                                window.updateTradeHistoryVisualizations();
                            }
                        });
                    }
                }

                // Clear the active shape state
                window.activeShapeForPotentialDeletion = null;
                updateSelectedShapeInfoPanel(null);
                hoveredShapeBackendId = null;

            } catch (error) {
                console.error(`Error deleting drawing ${drawingId}:`, error);
                alert(`Failed to delete drawing: ${error.message}`);
            }
        });
    }

    // Event listener for "Edit line" button
    if (window.editShapeBtn) {
        window.editShapeBtn.addEventListener('click', async () => {
            console.log('[DEBUG] Edit button clicked');
            console.log('[DEBUG] activeShapeForPotentialDeletion:', window.activeShapeForPotentialDeletion);

            if (!window.activeShapeForPotentialDeletion || !window.activeShapeForPotentialDeletion.id) {
                alert("No shape selected for editing.");
                return;
            }

            // Show the dialog first
            const dialog = document.getElementById('shape-properties-dialog');
            console.log('[DEBUG] Dialog element found:', !!dialog);

            if (dialog) {
                dialog.style.display = 'block';
                console.log('[DEBUG] Dialog displayed, now populating...');

                // Then populate the dialog with current shape properties
                await populateShapePropertiesDialog(window.activeShapeForPotentialDeletion);
                console.log('[DEBUG] Dialog population completed');
            } else {
                console.error('[DEBUG] Dialog element not found!');
            }
        });
    }


    // Event listener for "Delete ALL Drawings" button
    if (window.deleteAllShapesBtn) {
        window.deleteAllShapesBtn.addEventListener('click', async () => {
            const symbol = window.symbolSelect.value;
            if (!symbol) {
                alert("Please select a symbol first.");
                return;
            }
            // Removed confirmation dialog - delete all immediately

            try {
                // Get all drawings first to identify volume profile traces to remove
                const drawingsResponse = await fetch(`/get_drawings/${symbol}`);
                const drawingsResult = await drawingsResponse.json();
                const drawingIds = [];

                if (drawingsResponse.ok && drawingsResult.status === 'success') {
                    const drawings = drawingsResult.drawings || [];
                    drawingIds.push(...drawings.map(d => d.id));
                }

                // Delete all drawings individually via WebSocket
                const deletePromises = drawingIds.map(async (drawingId) => {
                    try {
                        if (window.wsAPI && window.wsAPI.connected) {
                            await new Promise((resolve, reject) => {
                                const timeout = setTimeout(() => {
                                    reject(new Error('Timeout waiting for delete response'));
                                }, 5000); // 5 second timeout

                                const requestId = Date.now().toString() + '_' + drawingId;

                                const messageHandler = (message) => {
                                    if ((message.type === 'shape_success' || message.type === 'error') && message.request_id === requestId) {
                                        clearTimeout(timeout);
                                        window.wsAPI.offMessage(message.type, messageHandler);
                                        if (message.type === 'shape_success' && message.data && message.data.id === drawingId) {
                                            resolve();
                                        } else if (message.type === 'error') {
                                            reject(new Error(message.message || 'Failed to delete shape'));
                                        }
                                    }
                                };

                                // Listen for both success and error messages with the same request ID
                                window.wsAPI.onMessage('shape_success', messageHandler);
                                window.wsAPI.onMessage('error', messageHandler);

                                // Send delete shape message
                                window.wsAPI.sendMessage({
                                    type: 'shape',
                                    action: 'delete',
                                    data: {
                                        drawing_id: drawingId,
                                        symbol: symbol
                                    },
                                    request_id: requestId
                                });
                            });
                            return drawingId;
                        } else {
                            throw new Error('WebSocket not connected');
                        }
                    } catch (error) {
                        console.error(`Error deleting drawing ${drawingId} via delete all:`, error);
                        return null; // Return null for failed deletions
                    }
                });

                await Promise.all(deletePromises);

                if (window.gd && window.gd.layout) {
                    window.gd.layout.shapes = [];
                    Plotly.relayout(window.gd, { shapes: [] });
                }

                // Remove associated volume profile traces for all deleted drawings
                if (window.gd && window.gd.data && drawingIds.length > 0) {
                    const filteredData = window.gd.data.filter(trace => {
                        if (!trace.name) return true;
                        return !drawingIds.some(id => trace.name.startsWith(`VP-${id}`));
                    });
                    if (filteredData.length !== window.gd.data.length) {
                        window.gd.data = filteredData;
                        Plotly.react(window.gd, window.gd.data, window.gd.layout).then(() => {
                            // Re-add trade history markers after shape deletion
                            if (window.tradeHistoryData && window.tradeHistoryData.length > 0 && window.updateTradeHistoryVisualizations) {
                                window.updateTradeHistoryVisualizations();
                            }
                        });
                    }
                }

                activeShapeForPotentialDeletion = null; // from state.js
                updateSelectedShapeInfoPanel(null); // from uiUpdaters.js
                hoveredShapeBackendId = null; // from state.js
            } catch (error) {
                console.error(`Error deleting all drawings for ${symbol}:`, error);
                alert(`Failed to delete all drawings: ${error.message}`);
                loadDrawingsAndRedraw(symbol); // from this file (main.js)
            }
        });
    }

    // Add event listeners for replay controls and Ollama settings to save settings on change
    document.getElementById('replay-from').addEventListener('change', () => {
        if (window.isProgrammaticallySetting) return;
        saveSettingsInner();
    });
    document.getElementById('replay-to').addEventListener('change', () => {
        if (window.isProgrammaticallySetting) return;
        saveSettingsInner();
    });
    document.getElementById('replay-speed').addEventListener('input', () => {
        if (window.isProgrammaticallySetting) return;
        saveSettingsInner();
    });
    window.useLocalOllamaCheckbox.addEventListener('change', () => { // Already has a listener in aiFeatures.js, ensure saveSettings is also called
        if (window.isProgrammaticallySetting) return;

        saveSettingsInner(); // Call saveSettings from settingsManager.js
    });
    window.localOllamaModelSelect.addEventListener('change', () => {
        if (window.isProgrammaticallySetting) return;
        saveSettingsInner();
    });

    // Stream delta slider event listener
    if (window.streamDeltaSlider && window.streamDeltaValueDisplay) {
        window.streamDeltaSlider.addEventListener('input', () => {
            if (window.isProgrammaticallySetting) return;

            // Update the display value
            window.streamDeltaValueDisplay.textContent = window.streamDeltaSlider.value;
            // Save settings when slider value changes
            saveSettingsInner();
        });
    }

    // Trade filter slider event listener - only if tradeHistory.js hasn't set it up
    if (window.minValueSlider && window.minValueDisplay && !window.tradeHistoryInitialized) {
        window.minValueSlider.addEventListener('input', () => {
            if (window.isProgrammaticallySetting) return;

            // Update the display value
            window.minValueDisplay.textContent = window.minValueSlider.value;
            // Note: Settings are not saved for min value filter - it's a temporary filter
        });
    }

    // Trade visualization checkboxes event listeners - only if tradeHistory.js hasn't set it up
    if (window.showVolumeProfileCheckbox && !window.tradeHistoryInitialized) {
        window.showVolumeProfileCheckbox.addEventListener('change', () => {
            if (window.isProgrammaticallySetting) return;
            saveSettingsInner();
        });
    }
    if (window.showTradeMarkersCheckbox && !window.tradeHistoryInitialized) {
        window.showTradeMarkersCheckbox.addEventListener('change', () => {
            if (window.isProgrammaticallySetting) return;
            saveSettingsInner();
        });
    }
    if (window.enableTradeTraceCheckbox) {
        window.enableTradeTraceCheckbox.addEventListener('change', () => {
            if (window.isProgrammaticallySetting) return;
            saveSettingsInner();
            // Re-render only regular trades
            if (window.regularTradeHistoryData && window.regularTradeHistoryData.length > 0) {
                // Update trade visualizations
                if (window.addTradeHistoryMarkersToChart) {
                    const symbol = window.currentSymbolForStream;
                    window.addTradeHistoryMarkersToChart(window.regularTradeHistoryData, symbol);
                }
            }
        });
    }
    if (window.enableMyTradesCheckbox) {
        window.enableMyTradesCheckbox.addEventListener('change', () => {
            if (window.isProgrammaticallySetting) return;
            saveSettingsInner();
            // Re-render only my trades
            if (window.myTradeHistoryData && window.myTradeHistoryData.length > 0) {
                // Process my trades separately
                if (window.addMyTradeHistoryMarkersToChart) {
                    const symbol = window.currentSymbolForStream;
                    window.addMyTradeHistoryMarkersToChart(window.myTradeHistoryData, symbol);
                }
            }
        });
    }

    // Initialize other features
    initializeReplayControls(); // From replay.js
    initializeAIFeatures();   // From aiFeatures.js
    // initializeLogStream(); // Log streaming disabled

    // Check for symbol from template FIRST (highest priority)
    let initialSymbol = null;
    if (window.initialSymbol && window.initialSymbol !== 'BTCUSDT') {
        initialSymbol = window.initialSymbol;
        window.symbolSelect.value = initialSymbol;
    } else {
        // Check for requested symbol from URL path (second priority)
        const urlPath = window.location.pathname;
        if (urlPath && urlPath !== '/' && urlPath.length > 1) {
            initialSymbol = urlPath.substring(1).toUpperCase();
            window.symbolSelect.value = initialSymbol;
        }
    }

    // If no initial symbol found, use default BTCUSDT
    initialSymbol = initialSymbol || 'BTCUSDT';
    window.symbolSelect.value = initialSymbol;

    // Store the initial symbol for comparison
    window.initialSymbol = initialSymbol;

    // Initialize global currentSymbolForStream for audio recording
    window.currentSymbolForStream = initialSymbol;

    // Apply URL chart view parameters if available (before WebSocket initialization)
    applyUrlChartViewParameters();

    // Chart interactions are now initialized immediately after chart creation

    // Initialize WebSocket connection - this will handle both initialization and settings loading
    await initializeWebSocketClient();

// WebSocket client initialization and settings handling
async function initializeWebSocketClient() {
    console.log('🚀 Initializing WebSocket client...');

    try {
        // Connect to WebSocket
        await window.wsAPI.connect();
        console.log('✅ WebSocket connected');

        // Set up message handlers for different message types
        window.wsAPI.onMessage('init_success', handleInitSuccess);
        window.wsAPI.onMessage('historical', handleHistoricalData);
        window.wsAPI.onMessage('drawings', handleDrawingsData);
        window.wsAPI.onMessage('trade_history', handleTradeHistoryData);
        window.wsAPI.onMessage('trade_history_success', handleTradeHistorySuccess);
        window.wsAPI.onMessage('trading_sessions', handleTradingSessionsData);
        window.wsAPI.onMessage('get_trading_sessions_response', handleGetTradingSessionsResponse);
        window.wsAPI.onMessage('volume_profile_success', handleVolumeProfileData);
        window.wsAPI.onMessage('live', handleLiveData);
        window.wsAPI.onMessage('history_success', handleHistorySuccess);
        window.wsAPI.onMessage('history_update', handleHistoryUpdate);
        window.wsAPI.onMessage('config_success', handleConfigSuccess);
        window.wsAPI.onMessage('trade_update', handleTradeUpdate);
        window.wsAPI.onMessage('error', handleWebSocketError);

        // Send init message to server with email and symbol
        const initMessage = {
            type: "init",
            data: {
                email: window.userEmail || '',
                symbol: window.initialSymbol || 'BTCUSDT'
            }
        };

        console.log('📤 Sending init message to server:', initMessage);
        await window.wsAPI.sendMessage(initMessage);

    } catch (error) {
        console.error('❌ Failed to initialize WebSocket client:', error);
        // Fallback to legacy HTTP initialization
        initializeFallbackInitialization();
    }
}

// Handle successful initialization - settings are delivered here
async function handleInitSuccess(message) {
    console.log('📥 Received init_success from server:');
    console.log(JSON.stringify(message, null, "\t"));

    window.isProgrammaticallySetting = true;

    const data = message.data || {};
    const authenticated = data.authenticated || false;
    const config = data.config || {};
    const email = config.email || null;
    const symbol = config.symbol || 'BTCUSDT';
    const settings = data.config || {};

    // Store authentication info
    window.userAuthenticated = authenticated;
    window.userEmail = email;

    // Store symbols globally from init_success message for populateDropdowns
    window.availableSymbols = data.symbols || [];

    // Update symbol display if different
    if (symbol !== window.symbolSelect.value) {
        window.symbolSelect.value = symbol;
    }

    // Update global currentSymbolForStream for audio recording
    window.currentSymbolForStream = symbol;

    // Now that we have authentication confirmed, proceed with settings-dependent initialization
    console.log('🔧 Server confirmed authentication and symbol:', { authenticated, email, symbol });
    console.log('⚙️ Settings received in init_response:', settings);

    // Apply settings received from the server
    if (settings && Object.keys(settings).length > 0) {
        try {
            // Apply settings to UI components (this replaces the separate loadSettings call)
            applySettingsToUI(settings, symbol, email);
            console.log('✅ Settings applied from init_response');
        } catch (settingsError) {
            console.error('❌ Error applying settings from init_response:', settingsError);
        }
    } else {
        console.warn('⚠️ No settings received in init_response, using defaults');
    }

    window.isProgrammaticallySetting = false;
}

// Apply settings received from server to UI components
function applySettingsToUI(settings, symbol, email) {
    console.log('Applying settings to UI:', settings, 'symbol:', symbol, 'email:', email);

    window.isProgrammaticallySetting = true;

    // Apply symbol restoration
    if (settings.last_selected_symbol) {
        if (window.symbolSelect.value != settings.last_selected_symbol) {
            window.symbolSelect.value = settings.last_selected_symbol;
            // Update global currentSymbolForStream for audio recording
            window.currentSymbolForStream = settings.last_selected_symbol;
        }
    }

    // Apply resolution settings
    if (settings.resolution && window.resolutionSelect &&
        window.resolutionSelect.value !== settings.resolution) {
        window.resolutionSelect.value = settings.resolution;
        // Update global currentResolution for audio recording
        window.currentResolution = settings.resolution;
        if(window.isProgrammaticallySetting == false)
            window.resolutionSelect.dispatchEvent(new Event('change'));
    }

    // Apply range settings
    if (settings.range && window.rangeSelect &&
        window.rangeSelect.value !== settings.range) {
        window.rangeSelect.value = settings.range;
        if(window.isProgrammaticallySetting == false)
            window.rangeSelect.dispatchEvent(new Event('change'));
    }

    // Apply indicator checkboxes
    if (settings.active_indicators && Array.isArray(settings.active_indicators)) {
        // First uncheck all
        document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]').forEach(cb => {
            cb.checked = false;
        });

        // Then check the ones from settings
        settings.active_indicators.forEach(indicator => {
            const checkbox = document.querySelector(`#indicator-checkbox-list input[type="checkbox"][value="${indicator}"]`);
            if (checkbox) {
                checkbox.checked = true;
            }
        });
    }

    // Apply axis ranges
    if (settings.xAxisMin !== null && settings.xAxisMax !== null) {
        window.currentXAxisRange = [settings.xAxisMin, settings.xAxisMax];
        window.xAxisMinDisplay.textContent = `${new Date(settings.xAxisMin).toISOString()}`;
        window.xAxisMaxDisplay.textContent = `${new Date(settings.xAxisMax).toISOString()}`;
    }
    if (settings.yAxisMin !== null && settings.yAxisMax !== null) {
        window.currentYAxisRange = [settings.yAxisMin, settings.yAxisMax];
        window.yAxisMinDisplay.textContent = settings.yAxisMin.toFixed(2);
        window.yAxisMaxDisplay.textContent = settings.yAxisMax.toFixed(2);
    }

    // Apply AI settings
    if (settings.useLocalOllama !== undefined && window.useLocalOllamaCheckbox) {
        window.useLocalOllamaCheckbox.checked = settings.useLocalOllama;
    }
    if (settings.localOllamaModel && window.localOllamaModelSelect) {
        window.localOllamaModelSelect.value = settings.localOllamaModel;
    }

    // Apply streaming settings
    if (settings.streamDelta !== undefined && window.streamDeltaSlider) {
        window.streamDeltaSlider.value = settings.streamDelta;
        window.streamDeltaValueDisplay.textContent = settings.streamDelta;
    }

    // Apply visualization settings
     if (settings.showAgentTrades !== undefined) {
         document.getElementById('showAgentTradesCheckbox').checked = settings.showAgentTrades;
     }
     if (settings.showVolumeProfile !== undefined && window.showVolumeProfileCheckbox) {
         window.showVolumeProfileCheckbox.checked = settings.showVolumeProfile;
     }
     if (settings.showTradeMarkers !== undefined && window.showTradeMarkersCheckbox) {
         window.showTradeMarkersCheckbox.checked = settings.showTradeMarkers;
     }
     if (settings.enableTradeTrace !== undefined && window.enableTradeTraceCheckbox) {
         window.enableTradeTraceCheckbox.checked = settings.enableTradeTrace;
     } else if (window.enableTradeTraceCheckbox) {
         window.enableTradeTraceCheckbox.checked = true; // Default to true if not set
     }
     if (settings.enableMyTrades !== undefined && window.enableMyTradesCheckbox) {
         window.enableMyTradesCheckbox.checked = settings.enableMyTrades;
     } else if (window.enableMyTradesCheckbox) {
         window.enableMyTradesCheckbox.checked = true; // Default to true if not set
     }

    // Apply trade filter settings
    if (settings.minValueFilter !== undefined && window.minValueSlider) {
        window.minValueSlider.value = settings.minValueFilter;
        updateMinValueDisplay();
    }

    console.log('📊 Settings applied successfully to UI components');

    window.isProgrammaticallySetting = false;

    // Get current min value filter percentage (0-1)
    const minValueSlider = document.getElementById('min-value-slider');
    let minPercentage = minValueSlider ? parseFloat(minValueSlider.value) || 0 : 0;

    // Get current chart settings for history request
    const currentResolution = window.resolutionSelect ? window.resolutionSelect.value : '1h';
    const active_indicators = Array.from(document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]:checked')).map(cb => cb.value);

    // Update global currentResolution for audio recording
    window.currentResolution = currentResolution;

    // Get time range from current axis or use defaults
    let fromTs, toTs;
    if (window.currentXAxisRange && window.currentXAxisRange.length === 2) {
        // currentXAxisRange stores timestamps in seconds (as received from backend)
        fromTs = Math.floor(window.currentXAxisRange[0]);
        toTs = Math.floor(window.currentXAxisRange[1]);
        console.log('📤 History message timestamps from currentXAxisRange:', {
            currentXAxisRange: window.currentXAxisRange,
            fromTs: fromTs,
            toTs: toTs,
            fromTsISO: new Date(fromTs * 1000).toISOString(),
            toTsISO: new Date(toTs * 1000).toISOString()
        });
    } else {
        // Default to last 30 days
        const currentTime = Math.floor(Date.now() / 1000);
        fromTs = currentTime - (30 * 24 * 60 * 60);
        toTs = currentTime;
        console.log('📤 History message timestamps using defaults:', {
            fromTs: fromTs,
            toTs: toTs,
            fromTsISO: new Date(fromTs * 1000).toISOString(),
            toTsISO: new Date(toTs * 1000).toISOString()
        });
    }

    window.updateLayoutForIndicators(active_indicators);


    const historyMessage = {
        type: "history",
        data: {
            symbol: symbol,
            email: email,
            minValuePercentage: minPercentage,
            from_ts: fromTs,
            to_ts: toTs,
            resolution: currentResolution,
            active_indicators: active_indicators
        }
    };

    console.log('📤 Sending history message after initSuccess:', historyMessage);
    try {
        window.wsAPI.sendMessage(historyMessage);
        console.log('✅ History message sent successfully');
    } catch (error) {
        console.error('❌ Failed to send history message:', error);
    }

    // Send trade_history message to load trade data initially
    const tradeHistoryMessage = {
        type: "trade_history",
        data: {
            symbol: symbol,
            email: email,
            minValuePercentage: minPercentage,
            from_ts: fromTs,
            to_ts: toTs
        }
    };

    console.log('📤 Sending trade_history message after initSuccess:', tradeHistoryMessage);
    try {
        window.wsAPI.sendMessage(tradeHistoryMessage);
        console.log('✅ Trade history message sent successfully');
    } catch (error) {
        console.error('❌ Failed to send trade_history message:', error);
    }

    // Also send get_trading_sessions message to display trading sessions on chart
    // Only if x-axis range is below 3 days (3 * 24 * 60 * 60 = 259200 seconds)
    const timeRangeSeconds = toTs - fromTs;
    const threeDaysSeconds = 3 * 24 * 60 * 60; // 259200 seconds

    if (timeRangeSeconds < threeDaysSeconds) {
        const tradingSessionsMessage = {
            type: "get_trading_sessions",
            data: {
                symbol: symbol,
                from_ts: fromTs,
                to_ts: toTs
            }
        };

        console.log('📤 Sending trading sessions message after initSuccess:', tradingSessionsMessage);
        try {
            window.wsAPI.sendMessage(tradingSessionsMessage);
            console.log('✅ Trading sessions message sent successfully');
        } catch (error) {
            console.error('❌ Failed to send trading sessions message:', error);
        }
    } else {
        console.log('📤 Skipping trading sessions message - time range too large:', timeRangeSeconds, 'seconds');
    }
}

// Handle historical data from WebSocket
function handleHistoricalData(message) {
    console.log('📈 Received historical data:', message);

    const data = message.data || [];
    const symbol = message.symbol || 'UNKNOWN';

    if (data && data.length > 0) {
        // Process the historical data similar to how combinedData.js handles it
        window.accumulatedHistoricalData = data;
        window.historicalDataSymbol = symbol;

        // Trigger chart update
        if (window.processHistoricalDataForChart) {
            window.processHistoricalDataForChart(data, symbol);
        }
    }
}

// Handle drawings data from WebSocket
function handleDrawingsData(message) {
    console.log('🎨 Received drawings data:', message);

    const data = message.data || {};
    const drawings = data.drawings || [];

    if (drawings.length > 0) {
        // Process drawings
        window.currentDrawings = drawings;

        // Update chart shapes
        updateChartShapes(drawings, message.symbol || 'UNKNOWN');
    }
}

// Handle trade history data from WebSocket
function handleTradeHistoryData(message) {
    console.log('💰 Received trade history data:', message);

    const data = message.data || [];
    window.tradeHistoryData = data;

    // Separate mytrades from regular trades
    const myTrades = data.filter(trade => trade.exchange === 'APEX' || trade.exchange === 'APEXPRO');
    const regularTrades = data.filter(trade => trade.exchange !== 'APEX' && trade.exchange !== 'APEXPRO');

    // Store separately for efficient access
    window.myTradeHistoryData = myTrades;
    window.regularTradeHistoryData = regularTrades;

    // Update trade visualizations
    if (window.addTradeHistoryMarkersToChart) {
        const symbol = message.symbol || window.symbolSelect.value;
        window.addTradeHistoryMarkersToChart(regularTrades, symbol);
    }

    // Process my trades separately
    if (window.addMyTradeHistoryMarkersToChart && myTrades.length > 0) {
        const symbol = message.symbol || window.symbolSelect.value;
        window.addMyTradeHistoryMarkersToChart(myTrades, symbol);
    }

    // Update slider range based on new data
    updateMinValueSliderRange();
}

// Handle trade history success from WebSocket (filtered data)
function handleTradeHistorySuccess(message) {
    console.log('💰 Received trade history success (filtered):', message);

    const data = message.data || {};
    window.tradeHistoryData = data.trades || [];

    // Separate mytrades from regular trades
    const myTrades = window.tradeHistoryData.filter(trade => trade.exchange === 'APEX' || trade.exchange === 'APEXPRO');
    const regularTrades = window.tradeHistoryData.filter(trade => trade.exchange !== 'APEX' && trade.exchange !== 'APEXPRO');

    // Remove all existing trade markers before adding new ones
    if (window.gd && window.gd.data) {
        // Remove trade traces
        window.gd.data = window.gd.data.filter(trace =>
            trace.name !== 'Buy Trades' && trace.name !== 'Sell Trades'
        );
    }

    // Remove trade shapes
    if (window.gd && window.gd.layout && window.gd.layout.shapes) {
        window.gd.layout.shapes = window.gd.layout.shapes.filter(shape =>
            !shape.name || (!shape.name.startsWith('trade_') && !shape.name.startsWith('my_trade_'))
        );
    }

    // Remove trade annotations
    if (window.gd && window.gd.layout && window.gd.layout.annotations) {
        window.gd.layout.annotations = window.gd.layout.annotations.filter(ann =>
            !ann.name || (!ann.name.startsWith('trade_annotation_') && !ann.name.startsWith('my_trade_annotation_'))
        );
    }

    // Update trade visualizations
    if (window.addTradeHistoryMarkersToChart) {
        const symbol = message.symbol || window.symbolSelect.value;
        window.addTradeHistoryMarkersToChart(regularTrades, symbol);
    }

    // Process my trades separately
    if (window.addMyTradeHistoryMarkersToChart && myTrades.length > 0) {
        const symbol = message.symbol || window.symbolSelect.value;
        window.addMyTradeHistoryMarkersToChart(myTrades, symbol);
    }

    // Update slider range based on new data
    //updateMinValueSliderRange();
}

// Handle trading sessions data from WebSocket
function handleTradingSessionsData(message) {
    console.log('📊 Received trading sessions data:', message);

    const data = message.data || {};
    const sessions = data.sessions || [];

    if (sessions.length > 0 && window.addTradingSessionsToChart) {
        window.addTradingSessionsToChart(sessions, message.symbol || 'UNKNOWN');
    }
}

// Handle get_trading_sessions_response message from WebSocket
function handleGetTradingSessionsResponse(message) {
    console.log('📊 Received get_trading_sessions_response:', message);

    // Handle the response from get_trading_sessions request
    if (message.data && message.data.sessions) {
        // Wrap sessions in the format expected by handleTradingSessionsMessage
        const sessionsMessage = {
            ...message,
            data: message.data.sessions
        };
        handleTradingSessionsMessage(sessionsMessage);
    } else {
        console.warn('📈 Combined WebSocket: Invalid get_trading_sessions_response format - missing data.sessions');
    }
}

// Handle volume profile data from WebSocket
function handleVolumeProfileData(message) {
    console.log('📊 Received volume profile data:', JSON.stringify(message));

    // Use the existing handleVolumeProfileMessage function from combinedData.js
    if (window.handleVolumeProfileMessage) {
        window.handleVolumeProfileMessage(message);
    } else {
        console.warn('📊 handleVolumeProfileMessage function not available');
    }
}

// Handle live price updates from WebSocket
function handleLiveData(message) {
    // console.log('⚡ Received live data:', message);

    const data = message.data || {};
    const livePrice = data.live_price;

    if (livePrice && typeof livePrice === 'number') {
        // Update real-time price line
        if (window.updateRealtimePriceLine) {
            window.updateRealtimePriceLine(livePrice, message.symbol || 'UNKNOWN');
        }
    }
}

// Create a wrapper function for updateRealtimePriceLine that provides appropriate parameters
window.updateRealtimePriceLine = function(price, symbol) {
    if (!window.gd) {
        console.warn("[PriceLine] Chart not available for real-time price line update");
        return;
    }

    // Check if currently dragging a shape - skip live price updates during dragging
    // console.log('🔄 LIVE UPDATE CHECK (main.js): isDraggingShape =', window.isDraggingShape, 'typeof:', typeof window.isDraggingShape);
    if (window.isDraggingShape) {
        console.log('🔄 LIVE UPDATE BLOCKED (main.js): Skipping live price update during shape dragging');
        console.warn('🚫 LIVE PRICE UPDATE PREVENTED DUE TO DRAGGING (main.js)');
        return;
    }

    // Calculate candle timing parameters
    const currentTime = Date.now();
    const timeframeSeconds = window.resolutionSelect ? getTimeframeSecondsJS(window.resolutionSelect.value) : 3600;
    const candleStartTimeMs = Math.floor(currentTime / (timeframeSeconds * 1000)) * (timeframeSeconds * 1000);
    const candleEndTimeMs = candleStartTimeMs + (timeframeSeconds * 1000);

    // Call the actual function with proper parameters
    if (window.updateOrAddRealtimePriceLine) {
        window.updateOrAddRealtimePriceLine(window.gd, price, candleStartTimeMs, candleEndTimeMs, true);
    } else {
        console.warn("[PriceLine] updateOrAddRealtimePriceLine function not available");
    }
};

// Helper function to get timeframe seconds (assuming from combinedData.js or utils.js)
function getTimeframeSecondsJS(timeframe) {
    const multipliers = {
        "1m": 60,
        "5m": 300,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
        "1w": 604800
    };
    return multipliers[timeframe] || 3600;
}

// Function to update chart with OHLCV and indicators data
function updateChartWithOHLCVAndIndicators(ohlcv, active_indicators) {
    if (!window.gd) {
        console.warn('Chart not ready for OHLCV and indicators update');
        return;
    }

    // Get existing traces to preserve non-OHLCV/indicator traces
    const existingTraces = window.gd.data || [];

    // Build set of trace names that will be replaced
    const tracesToReplace = new Set(['Price']);

    // Indicator configurations for plotting
    const indicatorConfigs = {
        'macd': [
            { key: 'macd', name: 'MACD', color: 'blue', type: 'scatter', mode: 'lines' },
            { key: 'signal', name: 'MACD Signal', color: 'red', type: 'scatter', mode: 'lines' },
            { key: 'histogram', name: 'MACD Histogram', color: 'green', type: 'bar' }
        ],
        'rsi': [
            { key: 'rsi', name: 'RSI', color: 'purple', type: 'scatter', mode: 'lines' }
        ],
        'stochrsi_9_3': [
            { key: 'stoch_k', name: 'StochRSI K (9,3)', color: 'orange', type: 'scatter', mode: 'lines' },
            { key: 'stoch_d', name: 'StochRSI D (9,3)', color: 'brown', type: 'scatter', mode: 'lines' }
        ],
        'stochrsi_14_3': [
            { key: 'stoch_k', name: 'StochRSI K (14,3)', color: 'cyan', type: 'scatter', mode: 'lines' },
            { key: 'stoch_d', name: 'StochRSI D (14,3)', color: 'magenta', type: 'scatter', mode: 'lines' }
        ],
        'stochrsi_40_4': [
            { key: 'stoch_k', name: 'StochRSI K (40,4)', color: 'lime', type: 'scatter', mode: 'lines' },
            { key: 'stoch_d', name: 'StochRSI D (40,4)', color: 'teal', type: 'scatter', mode: 'lines' }
        ],
        'stochrsi_60_10': [
            { key: 'stoch_k', name: 'StochRSI K (60,10)', color: 'pink', type: 'scatter', mode: 'lines' },
            { key: 'stoch_d', name: 'StochRSI D (60,10)', color: 'navy', type: 'scatter', mode: 'lines' }
        ],
        'open_interest': [
            { key: 'open_interest', name: 'Open Interest', color: 'black', type: 'scatter', mode: 'lines' }
        ],
        'jma': [
            { key: 'jma', name: 'JMA', color: 'blue', type: 'scatter', mode: 'lines' }
        ],
        'cto_line': [
            { key: 'cto_upper', name: 'CTO Upper', color: 'darkgreen', type: 'scatter', mode: 'lines' },
            { key: 'cto_lower', name: 'CTO Lower', color: 'darkred', type: 'scatter', mode: 'lines' }
        ]
    };

    // Add indicator trace names to the set of traces to replace
    if (active_indicators && Array.isArray(active_indicators)) {
        active_indicators.forEach(indicatorName => {
            const config = indicatorConfigs[indicatorName];
            if (config) {
                config.forEach(subConfig => {
                    tracesToReplace.add(subConfig.name);
                });
            }
        });

        // DEBUG: Log active indicators and OI specifically
        console.log('[DEBUG OI] Active indicators:', active_indicators);
        console.log('[DEBUG OI] OI in active indicators:', active_indicators.includes('open_interest'));
        console.log('[DEBUG OI] OI config exists:', !!indicatorConfigs['open_interest']);
    }

    // Preserve existing traces that are not being replaced
    const preservedTraces = existingTraces.filter(trace => !tracesToReplace.has(trace.name));

    // Create candlestick trace from OHLCV data
    const candlestickTrace = {
        x: ohlcv.map(point => new Date(point.time * 1000)),
        open: ohlcv.map(point => point.ohlc.open),
        high: ohlcv.map(point => point.ohlc.high),
        low: ohlcv.map(point => point.ohlc.low),
        close: ohlcv.map(point => point.ohlc.close),
        volume: ohlcv.map(point => point.ohlc.volume),
        type: 'candlestick',
        name: 'Price',
        xaxis: 'x',
        yaxis: 'y',
        increasing: { line: { color: 'green' } },
        decreasing: { line: { color: 'red' } }
    };

    // DEBUG: Log candlestick trace data to check for null values
    console.log('🔍 Candlestick trace data:');
    console.log('  - Total data points:', candlestickTrace.x.length);
    console.log('  - First 5 x values:', candlestickTrace.x.slice(0, 5));
    console.log('  - First 5 open values:', candlestickTrace.open.slice(0, 5));
    console.log('  - First 5 close values:', candlestickTrace.close.slice(0, 5));
    console.log('  - Last 5 x values:', candlestickTrace.x.slice(-5));
    console.log('  - Last 5 open values:', candlestickTrace.open.slice(-5));
    console.log('  - Last 5 close values:', candlestickTrace.close.slice(-5));

    // Check for null/undefined values in first few points
    const firstFewPoints = ohlcv.slice(0, 10);
    console.log('🔍 First 10 OHLCV points validation:');
    firstFewPoints.forEach((point, index) => {
        console.log(`  Point ${index}: time=${point.time}, open=${point.ohlc?.open}, high=${point.ohlc?.high}, low=${point.ohlc?.low}, close=${point.ohlc?.close}, volume=${point.ohlc?.volume}`);
    });

    // Start with preserved traces and add the candlestick trace
    const traces = [...preservedTraces, candlestickTrace];

    // Process active indicators
    if (active_indicators && Array.isArray(active_indicators) && active_indicators.length > 0) {
        console.log(`[DEBUG] Processing ${active_indicators.length} active indicators:`, active_indicators);
        // FORCE indicator order to match backend configuration
        const forcedIndicatorOrder = ['macd', 'rsi', 'stochrsi_9_3', 'stochrsi_14_3', 'stochrsi_40_4', 'stochrsi_60_10', 'open_interest', 'jma', 'cto_line'];
        const orderedActiveIndicators = forcedIndicatorOrder.filter(indicatorId => active_indicators.includes(indicatorId));
        console.log(`[DEBUG] Ordered active indicators:`, orderedActiveIndicators);

        orderedActiveIndicators.forEach((indicatorName, indicatorIndex) => {
            const config = indicatorConfigs[indicatorName];
            if (config) {
                const yAxisName = `y${indicatorIndex + 2}`; // y2, y3, y4, etc.
                console.log(`[DEBUG] Processing indicator ${indicatorName} on ${yAxisName}`);

                config.forEach(subConfig => {
                    // Extract data for this sub-indicator
                    const yData = ohlcv.map(point => {
                        const indicatorData = point.indicators && point.indicators[indicatorName];
                        return indicatorData ? indicatorData[subConfig.key] : null;
                    }).filter(val => val !== null); // Filter out nulls, but keep for plotting

                    console.log(`[DEBUG] ${indicatorName}.${subConfig.key} - data points: ${yData.length}, first few values:`, yData.slice(0, 5));

                    // Only add trace if we have data
                    if (yData.some(val => val !== null)) {
                        const indicatorTrace = {
                            x: ohlcv.map(point => new Date(point.time * 1000)),
                            y: ohlcv.map(point => {
                                const indicatorData = point.indicators && point.indicators[indicatorName];
                                return indicatorData ? indicatorData[subConfig.key] : null;
                            }),
                            type: subConfig.type,
                            mode: subConfig.mode,
                            name: subConfig.name,
                            ...(subConfig.type !== 'bar' && { line: { color: subConfig.color } }),
                            ...(subConfig.type === 'bar' && { marker: { color: subConfig.color } }),
                            xaxis: 'x',
                            yaxis: yAxisName // Assign to correct y-axis based on indicator order
                        };
                        traces.push(indicatorTrace);
                        console.log(`[DEBUG] Added ${indicatorName} trace: ${subConfig.name} on ${yAxisName}, data points: ${indicatorTrace.y.filter(v => v !== null).length}`);
                    } else {
                        console.log(`[DEBUG] Skipping ${indicatorName} trace: ${subConfig.name} - no valid data points`);
                    }
                });
            } else {
                console.log(`[DEBUG] No config found for indicator: ${indicatorName}`);
            }
        });
    }

    // Update layout if indicators are present (use existing updateLayoutForIndicators function)
    let updatedLayout = window.gd.layout;
    if (active_indicators && active_indicators.length > 0 && window.updateLayoutForIndicators) {
        // FORCE indicator order to match backend configuration for layout
        const forcedIndicatorOrder = ['macd', 'rsi', 'stochrsi_9_3', 'stochrsi_14_3', 'stochrsi_40_4', 'stochrsi_60_10', 'open_interest', 'jma', 'cto_line'];
        const orderedActiveIndicators = forcedIndicatorOrder.filter(indicatorId => active_indicators.includes(indicatorId));
        console.log(`[DEBUG] Calling updateLayoutForIndicators with:`, orderedActiveIndicators);
        updatedLayout = window.updateLayoutForIndicators(orderedActiveIndicators);
        console.log(`[DEBUG] Layout updated for indicators:`, updatedLayout.yaxis2 ? 'yaxis2 present' : 'no yaxis2');
    }

    // Preserve existing shapes when updating chart with OHLCV and indicators
    if (window.gd && window.gd.layout && window.gd.layout.shapes) {
        updatedLayout.shapes = window.gd.layout.shapes;
    }

    // Update the chart with traces and the updated layout
    Plotly.react(window.gd, traces, updatedLayout).then(() => {
        console.log(`✅ Chart updated with ${ohlcv.length} OHLCV points and ${active_indicators ? active_indicators.length : 0} active indicators`);
    }).catch(error => {
        console.error('❌ Error updating chart with OHLCV and indicators:', error);
    });
}


// Handle history success message from server
function handleHistorySuccess(message) {
    console.log('📥 Received history_success from server:\t');
    // console.log(JSON.stringify(message, null, "\t"));

    const data = message.data || {};
    const symbol = message.symbol;
    const email = message.email;
    const ohlcv = data.ohlcv || [];
    const indicators = data.indicators || [];
    const trades = data.trades || [];
    const mytrades = data.mytrades || [];
    const drawings = data.drawings || [];

    console.log(`📊 Processing history_success: ${ohlcv.length} OHLCV points, ${trades.length} trades, ${mytrades.length} mytrades, ${drawings.length} drawings, indicators: ${indicators.join(', ')}`);

    // DEBUG: Log OHLCV data details to check for trimming
    if (ohlcv.length > 0) {
        console.log('🔍 OHLCV data details:');
        console.log('  - Total points:', ohlcv.length);
        console.log('  - First point:', ohlcv[0]);
        console.log('  - Last point:', ohlcv[ohlcv.length - 1]);
        console.log('  - First 5 timestamps:', ohlcv.slice(0, 5).map(p => new Date(p.time * 1000).toISOString()));
        console.log('  - Last 5 timestamps:', ohlcv.slice(-5).map(p => new Date(p.time * 1000).toISOString()));
    } else {
        console.log('🔍 No OHLCV data received');
    }

    // Store the historical data globally for chart updates
    window.accumulatedHistoricalData = ohlcv;
    window.historicalDataSymbol = symbol;

    // Store trade history data
    window.tradeHistoryData = trades;

    // Calculate max trade value from all trades for filtering
    if (trades && trades.length > 0) {
        const tradeValues = trades.map(trade => {
            const price = parseFloat(trade.price) || 0;
            const amount = parseFloat(trade.amount) || 0;
            return price * amount;
        }).filter(value =>
            typeof value === 'number' && !isNaN(value) && value > 0
        );
        if (tradeValues.length > 0) {
            window.maxTradeValue = Math.max(...tradeValues);
            console.log(`Calculated max trade value: ${window.maxTradeValue.toLocaleString()}`);
        }
    }

    // Update chart in the correct order to preserve shapes
    // Combined chart update to avoid multiple Plotly.relayout calls
    updateChartWithDataAndShapes(ohlcv, data.active_indicators || [], drawings, symbol);

    // Process volume profiles from rectangle drawings
    if (drawings && drawings.length > 0) {
        drawings.forEach(drawing => {
            if ((drawing.type === 'rect' || drawing.type === 'rectangle') && drawing.volume_profile) {
                const volumeProfileMessage = {
                    type: 'volume_profile',
                    data: {
                        volume_profile: drawing.volume_profile.volume_profile,
                        rectangle_id: drawing.id,
                        symbol: symbol
                    }
                };
                handleVolumeProfileData(volumeProfileMessage);
            }
        });
    }

    // Process mytrades if available
    if (mytrades && mytrades.length > 0) {
        console.log(`💰 Processing ${mytrades.length} mytrades from history_success`);
        if (window.addMyTradeHistoryMarkersToChart) {
            window.addMyTradeHistoryMarkersToChart(mytrades, symbol);
        }
    }

    // Check if x-axis min or max is set to "Auto" and scale accordingly
    if (window.xAxisMinDisplay && window.xAxisMaxDisplay &&
        (window.xAxisMinDisplay.textContent === 'Auto' || window.xAxisMaxDisplay.textContent === 'Auto')) {

        // Get min and max timestamps from history data
        if (ohlcv && ohlcv.length > 0) {
            const timestamps = ohlcv.map(point => point.time);
            const xmin = Math.min(...timestamps) * 1000; // Convert to milliseconds
            const xmax = Math.max(...timestamps) * 1000;

            // Set the x-axis range
            window.currentXAxisRange = [xmin, xmax];
            window.xAxisMinDisplay.textContent = `${new Date(xmin).toISOString()}`;
            window.xAxisMaxDisplay.textContent = `${new Date(xmax).toISOString()}`;

            // Update chart x-axis range
            if (window.gd) {
                Plotly.relayout(window.gd, {
                    'xaxis.range': [new Date(xmin), new Date(xmax)],
                    'xaxis.autorange': false
                });
            }

            console.log(`📊 Auto-scaled x-axis from history data: ${new Date(xmin).toISOString()} to ${new Date(xmax).toISOString()}`);
        }
    }

    // Check if y-axis min or max is set to "Auto" and scale accordingly for price chart
    if (window.yAxisMinDisplay && window.yAxisMaxDisplay &&
        (window.yAxisMinDisplay.textContent === 'Auto' || window.yAxisMaxDisplay.textContent === 'Auto')) {

        // Get min and max prices from history data (OHLC values)
        if (ohlcv && ohlcv.length > 0) {
            let yMin = Infinity, yMax = -Infinity;
            ohlcv.forEach(point => {
                const ohlc = point.ohlc;
                if (ohlc) {
                    yMin = Math.min(yMin, ohlc.open, ohlc.high, ohlc.low, ohlc.close);
                    yMax = Math.max(yMax, ohlc.open, ohlc.high, ohlc.low, ohlc.close);
                }
            });

            if (yMin !== Infinity && yMax !== -Infinity) {
                // Add padding (5% like in autoscale)
                const yPadding = (yMax - yMin) * 0.05;
                const finalYMin = yMin - yPadding;
                const finalYMax = yMax + yPadding;

                // Set the y-axis range
                window.currentYAxisRange = [finalYMin, finalYMax];
                window.yAxisMinDisplay.textContent = finalYMin.toFixed(2);
                window.yAxisMaxDisplay.textContent = finalYMax.toFixed(2);

                // Update chart y-axis range
                if (window.gd) {
                    Plotly.relayout(window.gd, {
                        'yaxis.range': [finalYMin, finalYMax],
                        'yaxis.autorange': false
                    });
                }

                console.log(`📊 Auto-scaled y-axis from history data: ${finalYMin.toFixed(2)} to ${finalYMax.toFixed(2)}`);
            }
        }
    }

    window.updateTradeHistoryVisualizations();

    console.log('✅ History data processed and chart updated');
}

// Function to preserve existing indicators when merging new data
function preserveIndicatorsInNewData(existingData, newDataPoints) {
    console.log('🔄 [INDICATOR_PRESERVATION] Starting indicator preservation process...');
    console.log('🔄 [INDICATOR_PRESERVATION] Input validation:', {
        existingDataCount: existingData.length,
        newDataPointsCount: newDataPoints.length,
        existingDataHasNulls: existingData.some(d => d === null || d === undefined),
        newDataPointsHasNulls: newDataPoints.some(d => d === null || d === undefined)
    });

    // Create a map of timestamps to existing indicators
    const existingIndicatorsByTimestamp = new Map();

    existingData.forEach((point, index) => {
        if (point && point.indicators && Object.keys(point.indicators).length > 0) {
            existingIndicatorsByTimestamp.set(point.time, point.indicators);
        }
        if (index < 3) {
            console.log(`🔄 [INDICATOR_PRESERVATION] Existing point ${index}:`, {
                time: point?.time,
                hasIndicators: !!(point?.indicators),
                indicatorsKeys: point?.indicators ? Object.keys(point.indicators) : []
            });
        }
    });

    console.log('🔄 [INDICATOR_PRESERVATION] Found existing indicators for timestamps:', Array.from(existingIndicatorsByTimestamp.keys()));

    // Process new data points and merge with existing indicators
    const processedNewData = newDataPoints.map((newPoint, index) => {
        if (!newPoint) {
            console.error(`🔄 [INDICATOR_PRESERVATION] Null newPoint at index ${index}`);
            return null;
        }

        const timestamp = newPoint.time;
        const existingIndicators = existingIndicatorsByTimestamp.get(timestamp);

        if (existingIndicators) {
            console.log(`🔄 [INDICATOR_PRESERVATION] Preserving indicators for timestamp ${timestamp}:`, existingIndicators);
            const mergedPoint = {
                ...newPoint,
                indicators: {
                    ...existingIndicators,
                    ...(newPoint.indicators || {})
                }
            };
            console.log(`🔄 [INDICATOR_PRESERVATION] Merged point for timestamp ${timestamp}:`, {
                hasNewIndicators: !!(newPoint.indicators),
                hasExistingIndicators: !!existingIndicators,
                finalIndicatorsKeys: Object.keys(mergedPoint.indicators)
            });
            return mergedPoint;
        }

        if (index < 3) {
            console.log(`🔄 [INDICATOR_PRESERVATION] New point ${index} (no existing indicators):`, {
                time: newPoint.time,
                hasIndicators: !!(newPoint.indicators),
                indicatorsKeys: newPoint.indicators ? Object.keys(newPoint.indicators) : []
            });
        }
        return newPoint;
    });

    console.log('🔄 [INDICATOR_PRESERVATION] Processed new data validation:', {
        processedCount: processedNewData.length,
        hasNulls: processedNewData.some(d => d === null || d === undefined)
    });

    // Merge existing data with processed new data, ensuring no duplicates
    const mergedData = [...existingData];
    processedNewData.forEach((newPoint, index) => {
        if (!newPoint) {
            console.error(`🔄 [INDICATOR_PRESERVATION] Skipping null newPoint at index ${index}`);
            return;
        }

        const existingIndex = mergedData.findIndex(point => point && point.time === newPoint.time);
        if (existingIndex !== -1) {
            // Update existing point
            console.log(`🔄 [INDICATOR_PRESERVATION] Updating existing point at index ${existingIndex} with timestamp ${newPoint.time}`);
            mergedData[existingIndex] = newPoint;
        } else {
            // Add new point
            console.log(`🔄 [INDICATOR_PRESERVATION] Adding new point with timestamp ${newPoint.time}`);
            mergedData.push(newPoint);
        }
    });

    // Debug: Log each point before filtering to identify which ones are being removed
    console.log('🔍 [INDICATOR_PRESERVATION] Pre-filter validation - checking each merged point:');
    mergedData.forEach((point, index) => {
        const isValid = point !== null &&
                       point !== undefined &&
                       point.ohlc !== null &&
                       point.ohlc !== undefined &&
                       point.ohlc.open !== null &&
                       point.ohlc.open !== undefined &&
                       point.ohlc.close !== null &&
                       point.ohlc.close !== undefined;

        if (!isValid) {
            console.log(`❌ [INDICATOR_PRESERVATION] Point ${index} will be filtered out:`, {
                point: point,
                isNull: point === null,
                isUndefined: point === undefined,
                ohlcNull: point?.ohlc === null,
                ohlcUndefined: point?.ohlc === undefined,
                openNull: point?.ohlc?.open === null,
                openUndefined: point?.ohlc?.open === undefined,
                closeNull: point?.ohlc?.close === null,
                closeUndefined: point?.ohlc?.close === undefined,
                openValue: point?.ohlc?.open,
                closeValue: point?.ohlc?.close
            });
        } else if (index < 3 || index > mergedData.length - 4) { // Log first 3 and last 3 valid points
            console.log(`✅ [INDICATOR_PRESERVATION] Point ${index} is valid:`, {
                time: point.time,
                isoTime: new Date(point.time * 1000).toISOString(),
                hasOHLC: !!point.ohlc,
                open: point.ohlc?.open,
                close: point.ohlc?.close,
                hasIndicators: !!point.indicators
            });
        }
    });

    // Accept all data points - no trimming of first OHLCV values
    const filteredMergedData = mergedData.filter(point => {
        return point !== null &&
               point !== undefined;
    });

    console.log('🔄 [INDICATOR_PRESERVATION] Final merged data validation:', {
        originalMergedCount: mergedData.length,
        filteredMergedCount: filteredMergedData.length,
        removedNulls: mergedData.length - filteredMergedData.length,
        hasNulls: filteredMergedData.some(d => d === null || d === undefined),
        samplePoints: filteredMergedData.slice(0, 3).map((d, i) => ({
            index: i,
            time: d?.time,
            hasOHLC: !!(d?.ohlc),
            hasIndicators: !!(d?.indicators)
        }))
    });

    return filteredMergedData;
}

// Handle history update messages from WebSocket
function handleHistoryUpdate(message) {
    console.log('📥 [HISTORY_UPDATE] Received history_update message:', {
        ohlcvCount: message.data.ohlcv ? message.data.ohlcv.length : 0,
        symbol: message.symbol,
        hasOhlcv: !!message.data.ohlcv,
        ohlcvIsArray: Array.isArray(message.data.ohlcv)
    });

    if (!message.data.ohlcv || !Array.isArray(message.data.ohlcv)) {
        console.warn('📈 Combined WebSocket: Invalid history update data format');
        return;
    }

    if (message.data.ohlcv.length === 0) {
        console.log('📥 [HISTORY_UPDATE] No ohlcv in message, skipping');
        return;
    }

    // DEBUG: Log OHLCV data details to check for trimming
    const ohlcv = message.data.ohlcv;
    console.log('🔍 [HISTORY_UPDATE] OHLCV data details:');
    console.log('  - Total points:', ohlcv.length);
    console.log('  - First point:', ohlcv[0]);
    console.log('  - Last point:', ohlcv[ohlcv.length - 1]);
    console.log('  - First 5 timestamps:', ohlcv.slice(0, 5).map(p => new Date(p.time * 1000).toISOString()));
    console.log('  - Last 5 timestamps:', ohlcv.slice(-5).map(p => new Date(p.time * 1000).toISOString()));

    // Check if chart is ready
    const chartElement = document.getElementById('chart');
    if (!chartElement || !window.gd || !window.gd.layout) {
        console.warn('📈 Combined WebSocket: Chart not ready for history update');
        return;
    }

    console.log('📊 [HISTORY_UPDATE] Processing new data points:', {
        newOhlcvCount: message.data.ohlcv.length,
        firstOhlcv: message.data.ohlcv[0],
        lastOhlcv: message.data.ohlcv[message.data.ohlcv.length - 1]
    });

    // Process the new data points and update the chart
    // Use the same logic as handleHistoricalData but for smaller batches
    // Filter out invalid ohlcv with null OHLC values
    console.log('📊 [HISTORY_UPDATE] Raw ohlcv data:', message.data.ohlcv);
    const newDataPoints = message.data.ohlcv.filter(ohlcv => {
        const hasOhlc = ohlcv.ohlc && typeof ohlcv.ohlc === 'object';
        const hasOpen = hasOhlc && ohlcv.ohlc.open !== null && ohlcv.ohlc.open !== undefined;
        const hasHigh = hasOhlc && ohlcv.ohlc.high !== null && ohlcv.ohlc.high !== undefined;
        const hasLow = hasOhlc && ohlcv.ohlc.low !== null && ohlcv.ohlc.low !== undefined;
        const hasClose = hasOhlc && ohlcv.ohlc.close !== null && ohlcv.ohlc.close !== undefined;
        const hasVolume = hasOhlc && (ohlcv.ohlc.volume !== null && ohlcv.ohlc.volume !== undefined) || (ohlcv.vol !== null && ohlcv.vol !== undefined);

        console.log(`📊 [HISTORY_UPDATE] Validating ohlcv:`, {
            time: ohlcv.time,
            hasOhlc,
            hasOpen,
            hasHigh,
            hasLow,
            hasClose,
            hasVolume,
            ohlc: ohlcv.ohlc
        });

        // Accept all data points - no trimming of first OHLCV values
        return true;
    });

    console.log(`📊 [HISTORY_UPDATE] Filtered ${message.data.ohlcv.length - newDataPoints.length} invalid ohlcv with null OHLC values`);

    // DEBUG: Log filtered data details
    if (newDataPoints.length > 0) {
        console.log('🔍 [HISTORY_UPDATE] Filtered OHLCV data details:');
        console.log('  - Filtered points:', newDataPoints.length);
        console.log('  - First filtered point:', newDataPoints[0]);
        console.log('  - Last filtered point:', newDataPoints[newDataPoints.length - 1]);
        console.log('  - First 5 filtered timestamps:', newDataPoints.slice(0, 5).map(p => new Date(p.time * 1000).toISOString()));
        console.log('  - Last 5 filtered timestamps:', newDataPoints.slice(-5).map(p => new Date(p.time * 1000).toISOString()));
    } else {
        console.log('🔍 [HISTORY_UPDATE] No valid OHLCV data after filtering');
    }

    // Get existing chart data to merge with
    const existingTrace = window.gd.data.find(trace => trace.type === 'candlestick');
    let existingData = [];

    if (existingTrace && existingTrace.x && existingTrace.x.length > 0) {
        // Convert existing chart data back to our format
        existingData = existingTrace.x.map((timestamp, index) => ({
            time: timestamp.getTime() / 1000, // Convert back to seconds
            ohlc: {
                open: existingTrace.open[index],
                high: existingTrace.high[index],
                low: existingTrace.low[index],
                close: existingTrace.close[index],
                volume: existingTrace.volume ? existingTrace.volume[index] : 0
            },
            indicators: {} // Will be populated from existing chart traces
        }));

        // Extract indicator data from existing chart traces
        const indicatorTraces = window.gd.data.filter(trace => trace.type !== 'candlestick');

        // Create a map of timestamps to their indices for quick lookup
        const timestampToIndex = new Map();
        existingTrace.x.forEach((timestamp, index) => {
            timestampToIndex.set(timestamp.getTime(), index);
        });

        // Process each indicator trace
        indicatorTraces.forEach(trace => {
            if (!trace.x || !trace.y) return;

            // Determine indicator type from trace name
            let indicatorId = null;
            let valueKey = null;

            if (trace.name === 'MACD') {
                indicatorId = 'macd';
                valueKey = 'macd';
            } else if (trace.name === 'MACD Signal') {
                indicatorId = 'macd';
                valueKey = 'signal';
            } else if (trace.name === 'MACD Histogram') {
                indicatorId = 'macd';
                valueKey = 'histogram';
            } else if (trace.name === 'RSI') {
                indicatorId = 'rsi';
                valueKey = 'rsi';
            } else if (trace.name === 'RSI_SMA14') {
                indicatorId = 'rsi';
                valueKey = 'rsi_sma14';
            } else if (trace.name.startsWith('Stoch K')) {
                // Extract variant from trace name (e.g., 'Stoch K (14,3)' -> 'stochrsi_14_3')
                const variantMatch = trace.name.match(/Stoch K \((\d+),(\d+)\)/);
                if (variantMatch) {
                    indicatorId = `stochrsi_${variantMatch[1]}_${variantMatch[2]}`;
                } else {
                    // Fallback to first stochrsi variant if no variant found
                    indicatorId = window.combinedIndicators ? window.combinedIndicators.find(id => id.startsWith('stochrsi')) : null;
                }
                valueKey = 'stoch_k';
            } else if (trace.name.startsWith('Stoch D')) {
                // Extract variant from trace name (e.g., 'Stoch D (14,3)' -> 'stochrsi_14_3')
                const variantMatch = trace.name.match(/Stoch D \((\d+),(\d+)\)/);
                if (variantMatch) {
                    indicatorId = `stochrsi_${variantMatch[1]}_${variantMatch[2]}`;
                } else {
                    // Fallback to first stochrsi variant if no variant found
                    indicatorId = window.combinedIndicators ? window.combinedIndicators.find(id => id.startsWith('stochrsi')) : null;
                }
                valueKey = 'stoch_d';
            } else if (trace.name === 'JMA') {
                indicatorId = 'jma';
                valueKey = 'jma';
            } else if (trace.name === 'Open Interest') {
                indicatorId = 'open_interest';
                valueKey = 'open_interest';
            }

            if (!indicatorId || !valueKey) return;

            // Add indicator values to existing data points
            trace.x.forEach((timestamp, traceIndex) => {
                const timestampMs = timestamp.getTime();
                const dataIndex = timestampToIndex.get(timestampMs);

                if (dataIndex !== undefined && trace.y[traceIndex] !== null && trace.y[traceIndex] !== undefined) {
                    if (!existingData[dataIndex].indicators[indicatorId]) {
                        existingData[dataIndex].indicators[indicatorId] = {};
                    }
                    existingData[dataIndex].indicators[indicatorId][valueKey] = trace.y[traceIndex];
                }
            });
        });

    }

    // Preserve existing indicators when merging new data
    console.log('🔄 [HISTORY_UPDATE] Preserving existing indicators...');

    // Output last 5 bars before adding new OHLC data
    const last5Before = existingData.slice(-5).map(d => d.ohlc);
    console.log('📊 [HISTORY_UPDATE] Last 5 OHLC bars before adding new data:', last5Before);

    // Debug: Check newDataPoints for any null values
    console.log('🔍 [HISTORY_UPDATE] newDataPoints validation:', {
        count: newDataPoints.length,
        hasNulls: newDataPoints.some(d => d === null || d === undefined),
        nullIndices: newDataPoints.map((d, i) => d === null || d === undefined ? i : -1).filter(i => i !== -1),
        sampleData: newDataPoints.slice(0, 3)
    });

    const mergedData = preserveIndicatorsInNewData(existingData, newDataPoints);

    console.log('📊 [HISTORY_UPDATE] Final merged data:', {
        mergedCount: mergedData.length,
        hasIndicators: mergedData.some(d => d.indicators && Object.keys(d.indicators).length > 0),
        sampleIndicators: mergedData.find(d => d.indicators)?.indicators,
        hasNulls: mergedData.some(d => d === null || d === undefined || (d.ohlc && (d.ohlc.open === null || d.ohlc.close === null)))
    });

    // Output last 5 bars after adding new OHLC data
    const last5After = mergedData.slice(-5).map(d => d && d.ohlc ? d.ohlc : null);
    console.log('📊 [HISTORY_UPDATE] Last 5 OHLC bars after adding new data:', last5After);

    // Additional validation: check if any of the last 5 have null OHLC
    const last5Detailed = mergedData.slice(-5).map((d, i) => ({
        index: mergedData.length - 5 + i,
        time: d?.time,
        hasData: !!d,
        hasOHLC: !!(d?.ohlc),
        ohlc: d?.ohlc || null
    }));
    console.log('🔍 [HISTORY_UPDATE] Last 5 detailed validation:', last5Detailed);

    // Update chart with merged data
    if (window.updateChartWithHistoricalData) {
        console.log('📈 [HISTORY_UPDATE] Calling updateChartWithHistoricalData...');
        window.updateChartWithHistoricalData(mergedData, message.symbol);

        // Add logging after chart update to verify traces
        setTimeout(() => {
            if (window.gd && window.gd.data) {
                const candlestickTrace = window.gd.data.find(t => t.type === 'candlestick');
                const indicatorTraces = window.gd.data.filter(t => t.type !== 'candlestick');

                console.log('✅ [HISTORY_UPDATE] Chart update verification:', {
                    totalTraces: window.gd.data.length,
                    candlestickPoints: candlestickTrace ? candlestickTrace.x.length : 0,
                    indicatorTraces: indicatorTraces.length,
                    indicatorNames: indicatorTraces.map(t => t.name),
                    hasIndicators: indicatorTraces.some(t => t.y && t.y.some(val => val !== null && val !== undefined))
                });
            }
        }, 100);

        console.log('✅ [HISTORY_UPDATE] Chart update completed');
    } else {
        console.warn('⚠️ [HISTORY_UPDATE] updateChartWithHistoricalData function not available');
    }
}

// Handle config success message from server
function handleConfigSuccess(message) {
    console.log('📥 Received config_success from server:', message);

    const data = message.data || {};
    const symbol = message.symbol;
    const email = message.email;
    const ohlcv = data.ohlcv || [];
    const indicators = data.indicators || [];
    const trades = data.trades || [];
    const mytrades = data.mytrades || [];
    const drawings = data.drawings || [];

    console.log(`📊 Processing config_success: ${ohlcv.length} OHLCV points, ${trades.length} trades, ${mytrades.length} mytrades, ${drawings.length} drawings, indicators: ${indicators.join(', ')}`);

    // DEBUG: Log OHLCV data details to check for trimming
    if (ohlcv.length > 0) {
        console.log('🔍 OHLCV data details:');
        console.log('  - Total points:', ohlcv.length);
        console.log('  - First point:', ohlcv[0]);
        console.log('  - Last point:', ohlcv[ohlcv.length - 1]);
        console.log('  - First 5 timestamps:', ohlcv.slice(0, 5).map(p => new Date(p.time * 1000).toISOString()));
        console.log('  - Last 5 timestamps:', ohlcv.slice(-5).map(p => new Date(p.time * 1000).toISOString()));
    } else {
        console.log('🔍 No OHLCV data received');
    }

    // Store the historical data globally for chart updates
    window.accumulatedHistoricalData = ohlcv;
    window.historicalDataSymbol = symbol;

    // Store trade history data
    window.tradeHistoryData = trades;

    // Combined chart update to avoid multiple Plotly.relayout calls
    updateChartWithDataAndShapes(ohlcv, data.active_indicators || [], drawings, symbol);

    // Process volume profiles from rectangle drawings
    if (drawings && drawings.length > 0) {
        drawings.forEach(drawing => {
            if ((drawing.type === 'rect' || drawing.type === 'rectangle') && drawing.volume_profile) {
                const volumeProfileMessage = {
                    type: 'volume_profile',
                    data: {
                        volume_profile: drawing.volume_profile,
                        rectangle_id: drawing.id,
                        symbol: symbol
                    }
                };
                handleVolumeProfileData(volumeProfileMessage);
            }
        });
    }

    // Process mytrades if available
    if (mytrades && mytrades.length > 0) {
        console.log(`💰 Processing ${mytrades.length} mytrades from config_success`);
        if (window.addMyTradeHistoryMarkersToChart) {
            window.addMyTradeHistoryMarkersToChart(mytrades, symbol);
        }
    }

    updateTradeHistoryVisualizations();

    console.log('✅ Config data processed and chart updated');
}

// Handle trade update messages from WebSocket
function handleTradeUpdate(message) {
    console.log('💰 Received trade update:', message);

    const trades = message.trades || (message.trade ? [message.trade] : []);
    const symbol = message.symbol;
    const exchange = message.exchange;

    if (!trades || trades.length === 0 || !symbol) {
        console.warn('Invalid trade update message:', message);
        return;
    }

    // Process each trade in the array
    const validTrades = [];
    for (const trade of trades) {
        // Check if trade should be displayed based on current filter settings
        const minValueSlider = document.getElementById('min-value-slider');
        if (minValueSlider && window.maxTradeValue) {
            const minPercentage = parseFloat(minValueSlider.value) || 0;
            const tradePrice = parseFloat(trade.price) || 0;
            const tradeAmount = parseFloat(trade.amount) || 0;
            const tradeValue = tradePrice * tradeAmount;
            const maxTradeValue = parseFloat(window.maxTradeValue) || 0;
            const minValueThreshold = minPercentage * maxTradeValue;

            if (tradeValue < minValueThreshold) {
                // console.log(`Trade filtered out: value ${tradeValue} < threshold ${minValueThreshold}`);
                continue; // Skip this trade
            }
        }

        // Convert trade format if needed
        const formattedTrade = {
            timestamp: trade.timestamp,
            price: trade.price,
            amount: trade.amount || trade.quantity,
            side: trade.side.toUpperCase(),
            exchange: exchange,
            symbol: symbol
        };

        validTrades.push(formattedTrade);
    }

    // Add valid trades to chart
    if (validTrades.length > 0 && window.addTradeHistoryMarkersToChart) {
        // Add to existing trade history data
        if (!window.tradeHistoryData) {
            window.tradeHistoryData = [];
        }
        window.tradeHistoryData.push(...validTrades);

        // Update chart visualization
        window.addTradeHistoryMarkersToChart(validTrades, symbol);

        console.log(`Added ${validTrades.length} real-time trade(s): ${symbol} ${exchange}`);
    }
}

// Handle WebSocket errors
function handleWebSocketError(message) {
    console.error('❌ WebSocket error:', message);
    const errorMessage = message.message || 'Unknown WebSocket error';

    // Show user-friendly error message
    if (window.showToast || alert) {
        const showError = window.showToast || alert;
        showError(`Connection Error: ${errorMessage}`);
    }
}



// Fallback initialization in case WebSocket fails
async function initializeFallbackInitialization() {
    console.warn('⚠️ WebSocket initialization failed, falling back to legacy HTTP initialization');

    try {
        // Fall back to traditional HTTP-based initialization
        const symbolToLoad = window.initialSymbol || window.symbolSelect.value;
        if (symbolToLoad) {
            await loadSettings(symbolToLoad);
        }

        // Initialize chart with legacy combined WebSocket
        const selectedSymbol = window.symbolSelect.value;
        const selectedResolution = window.resolutionSelect.value;

        if (selectedSymbol && selectedResolution) {
            const active_indicators = Array.from(document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]:checked')).map(cb => cb.value);

            const currentTime = new Date().getTime();
            const fromTs = new Date(currentTime - 30 * 86400 * 1000).toISOString();
            const toTs = new Date(currentTime).toISOString();

            setupCombinedWebSocket(selectedSymbol, active_indicators, selectedResolution, fromTs, toTs);
        }
    } catch (error) {
        console.error('❌ Fallback initialization also failed:', error);
        alert('Failed to initialize application. Please refresh the page.');
    }
}

        // Initialize chart interactions immediately after chart is ready
        if (window.initializeChartInteractions) {
            window.initializeChartInteractions(); // From chartInteractions.js
        }

    // Handle browser back/forward navigation
    window.addEventListener('popstate', (event) => {
        const newSymbol = window.location.pathname.substring(1).toUpperCase();

        if (newSymbol && newSymbol !== window.symbolSelect.value) {

            // Update dropdown without triggering change event
            window.isProgrammaticallySetting = true;
            window.symbolSelect.value = newSymbol;
            // Update global currentSymbolForStream for audio recording
            window.currentSymbolForStream = newSymbol;
            window.isProgrammaticallySetting = false;

            // Perform symbol switch
            window.currentXAxisRange = null;
            window.currentYAxisRange = null;
            window.xAxisMinDisplay.textContent = 'Auto';
            window.xAxisMaxDisplay.textContent = 'Auto';
            window.yAxisMinDisplay.textContent = 'Auto';
            window.yAxisMaxDisplay.textContent = 'Auto';
            window.activeShapeForPotentialDeletion = null;

            // Clear chart and close WebSocket
            if (window.gd) {
                removeRealtimePriceLine(window.gd);
                Plotly.react(window.gd, [], window.gd.layout || {});
            }
            closeCombinedWebSocket("Browser navigation - switching symbols");

            // Reset state
            window.combinedSymbol = '';
            window.combinedIndicators = [];
            window.combinedResolution = '1h';
            window.combinedFromTs = null;
            window.combinedToTs = null;
            window.accumulatedHistoricalData = [];
            window.isAccumulatingHistorical = false;
        window.historicalDataSymbol = '';

        // Clear trade history data for the old symbol
        window.tradeHistoryData = [];

        updateSelectedShapeInfoPanel(null);
        loadSettings(newSymbol);


            // Establish new connection
            const active_indicators = Array.from(document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]:checked')).map(cb => cb.value);
            const resolution = window.resolutionSelect.value;
            // Use current time for range calculations
            const currentTime = new Date().getTime();
            let wsFromTs = new Date(currentTime - 30 * 86400 * 1000).toISOString();
            let wsToTs = new Date(currentTime + 30 * 86400 * 1000).toISOString();

            // 🔧 FIX TIMESTAMP SYNCHRONIZATION: Use saved X-axis range if available
            if (window.currentXAxisRange && Array.isArray(window.currentXAxisRange) && window.currentXAxisRange.length === 2) {
                wsFromTs = new Date(window.currentXAxisRange[0]).toISOString();
                wsToTs = new Date(window.currentXAxisRange[1]).toISOString();
            }

            delay(200).then(() => {
                setupCombinedWebSocket(newSymbol, active_indicators, resolution, wsFromTs, wsToTs);
            });
        }
    });

    // Explicitly handle window resize
    const debouncedPlotlyResize = debounce(function() {
        if (window.gd) {
            Plotly.Plots.resize(window.gd);
        }
    }, 250);
    window.addEventListener('resize', debouncedPlotlyResize);
});

/**
 * Applies autoscaling to the chart, focusing on historical data
 * and ignoring the full extent of a specified live data trace for scaling.
 */
function applyAutoscale(gdFromClick) { // Added gdFromClick argument
    console.info("Autoscale INITIATED.");
    // Ensure we are using the Plotly graph object, not just the DOM element.
    // gdFromClick is expected to be the graph object from the modebar button.
    // window.gd is the global reference to the graph object.
    const plotlyGraphObj = gdFromClick || window.gd;

    if (!plotlyGraphObj) {
        console.error("Autoscale: Plotly graph object not available (gdFromClick or window.gd is null/undefined).");
        return;
    }
    // Check for essential properties of a fully initialized Plotly graph object.
    if (!plotlyGraphObj.data || !plotlyGraphObj.layout || !plotlyGraphObj._fullLayout) {
        console.warn("Autoscale: Plotly graph object not fully initialized (missing data, layout, or _fullLayout). Proceeding anyway to keep button functional.");
        // Add more detailed logging for debugging this state:
        // Continue execution instead of returning to keep button functional
    }

   const fullData = plotlyGraphObj.data;
   const inputLayout = plotlyGraphObj.layout;
   const layoutUpdate = {};

   console.log("Autoscale: Full data traces count:", fullData.length);
   fullData.forEach((trace, idx) => {
       console.log(`Trace ${idx}: name=${trace.name}, type=${trace.type}, yaxis=${trace.yaxis}, hasY=${!!trace.y}, hasOpen=${!!trace.open}`);
   });

   // --- X-AXIS AUTOSCALE DISABLED ---
   // Autoscale no longer modifies xaxis min and max values
// --- Y-AXES AUTOSCALE ---
// Collect y values for primary y-axis (y) from visible traces
let yMin = Infinity, yMax = -Infinity;
let yDataFound = false;
let yPadding = 0; // Declare yPadding here to make it accessible throughout the function

// Track min/max for each indicator y-axis
let indicatorRanges = {};


   // Get current X-axis range to filter visible data points
   const currentXAxisRange = inputLayout.xaxis && inputLayout.xaxis.range;
   let xMinVisible = null;
   let xMaxVisible = null;


   if (currentXAxisRange && currentXAxisRange.length === 2) {
       // Plotly stores timestamps as milliseconds since epoch
       xMinVisible = typeof currentXAxisRange[0] === 'number' ? currentXAxisRange[0] : new Date(currentXAxisRange[0]).getTime();
       xMaxVisible = typeof currentXAxisRange[1] === 'number' ? currentXAxisRange[1] : new Date(currentXAxisRange[1]).getTime();
       console.log("Autoscale: X-axis range:", new Date(xMinVisible).toISOString(), "to", new Date(xMaxVisible).toISOString());
   } else {
       console.log("Autoscale: No X-axis range found, processing all data points");
   }

   fullData.forEach(trace => {
        if (trace.name === 'Buy Signal') return; // Skip traces named "Buy Signal"
        // Skip trade-related traces that shouldn't affect price scaling
        if (trace.name === 'Buy Trades' || trace.name === 'Sell Trades') return;

        if (trace.yaxis === 'y') { // Ensure y-values are present AND for main chart
            console.log(`Autoscale: Processing main y-axis trace: ${trace.name || 'unnamed'}, type: ${trace.type}`);
            // Iterate through open, high, low, close values for candlestick traces
            const ohlc = [trace.open, trace.high, trace.low, trace.close];
            ohlc.forEach((yValues, ohlcIndex) => {
                if (yValues && Array.isArray(yValues)) {
                    console.log(`Autoscale: Processing OHLC ${['open','high','low','close'][ohlcIndex]}, ${yValues.length} values`);
                    yValues.forEach((yVal, index) => {
                        if (typeof yVal === 'number' && !isNaN(yVal)) {
                            // Check if this data point is within the visible X-axis range
                            let isVisible = true;
                            if (xMinVisible !== null && xMaxVisible !== null && trace.x && trace.x[index]) {
                                const xVal = trace.x[index] instanceof Date ? trace.x[index].getTime() : new Date(trace.x[index]).getTime();
                                isVisible = xVal >= xMinVisible && xVal <= xMaxVisible;
                            }

                            if (isVisible) {
                                if (yVal < yMin) yMin = yVal;
                                if (yVal > yMax) yMax = yVal;
                                yDataFound = true;
                            }
                        }
                    });
                } else {
                    if (trace.name) {
                        console.warn(`Autoscale: OHLC value is not an array in candlestick trace`, trace.name);
                    }
                }
            });

            // Also check trace.y for other trace types on main y-axis (e.g., lines, scatters)
            if (trace.y && Array.isArray(trace.y)) {
                console.log(`Autoscale: Processing trace.y for ${trace.name || 'unnamed'}, ${trace.y.length} values`);
                trace.y.forEach((yVal, index) => {
                    if (typeof yVal === 'number' && !isNaN(yVal)) {
                        // Check if this data point is within the visible X-axis range
                        let isVisible = true;
                        if (xMinVisible !== null && xMaxVisible !== null && trace.x && trace.x[index]) {
                            const xVal = trace.x[index] instanceof Date ? trace.x[index].getTime() : new Date(trace.x[index]).getTime();
                            isVisible = xVal >= xMinVisible && xVal <= xMaxVisible;
                        }

                        if (isVisible) {
                            if (yVal < yMin) yMin = yVal;
                            if (yVal > yMax) yMax = yVal;
                            yDataFound = true;
                        }
                    }
                });
            }
        } else if (trace.yaxis && trace.yaxis !== 'y' && trace.y && Array.isArray(trace.y) && trace.y.length > 0) { // Handle indicator subplots
            console.log(`Autoscale: Processing indicator trace: ${trace.name || 'unnamed'} on ${trace.yaxis}`);
            
            // Initialize min/max for this y-axis if not already done
            const yAxisName = trace.yaxis;
            if (!indicatorRanges[yAxisName]) {
                indicatorRanges[yAxisName] = {
                    min: Infinity,
                    max: -Infinity,
                    dataFound: false
                };
            }
            
            trace.y.forEach((yVal, index) => {
                if (typeof yVal === 'number' && !isNaN(yVal)) {
                    // Check if this data point is within the visible X-axis range
                    let isVisible = true;
                    if (xMinVisible !== null && xMaxVisible !== null && trace.x && trace.x[index]) {
                        const xVal = trace.x[index] instanceof Date ? trace.x[index].getTime() : new Date(trace.x[index]).getTime();
                        isVisible = xVal >= xMinVisible && xVal <= xMaxVisible;
                    }

                    if (isVisible) {
                        if (yVal < indicatorRanges[yAxisName].min) indicatorRanges[yAxisName].min = yVal;
                        if (yVal > indicatorRanges[yAxisName].max) indicatorRanges[yAxisName].max = yVal;
                        indicatorRanges[yAxisName].dataFound = true;
                    }
                }
            });
        } else {
            console.log(`Autoscale: Skipping trace: ${trace.name || 'unnamed'}, yaxis: ${trace.yaxis}`);
        }
   });

   console.log(`Autoscale: After processing - yDataFound: ${yDataFound}, yMin: ${yMin}, yMax: ${yMax}`);


    // Combine the Y-axis ranges
    let priceChartYMin = Infinity;
    let priceChartYMax = -Infinity;

    if (yDataFound) {
        priceChartYMin = Math.min(priceChartYMin, yMin);
        priceChartYMax = Math.max(priceChartYMax, yMax);
    }

    console.log(`Autoscale: Final price range - yDataFound: ${yDataFound}, priceChartYMin: ${priceChartYMin}, priceChartYMax: ${priceChartYMax}`);

    if (priceChartYMin !== Infinity && priceChartYMax !== -Infinity) {
        if (priceChartYMin === priceChartYMax) {
            yPadding = Math.abs(priceChartYMin) * 0.1 || 0.1; // 10% of the price or a default value
        } else {
            yPadding = (priceChartYMax - priceChartYMin) * 0.05; // 5% padding
        }

        const finalYMin = priceChartYMin - yPadding;
        const finalYMax = priceChartYMax + yPadding;

        console.log(`Autoscale: Calculated range - yPadding: ${yPadding}, finalYMin: ${finalYMin}, finalYMax: ${finalYMax}`);

        // Validate that the calculated Y-axis values are finite
        if (!isFinite(finalYMin) || !isFinite(finalYMax)) {
            console.error("Autoscale: Invalid Y-axis range calculated - priceChartYMin:", priceChartYMin, "priceChartYMax:", priceChartYMax, "yPadding:", yPadding);
            console.error("Autoscale: Skipping Y-axis autoscale due to invalid range");
            return; // Exit early to prevent the error
        }

        layoutUpdate['yaxis.range[0]'] = finalYMin;
        layoutUpdate['yaxis.range[1]'] = finalYMax;
        layoutUpdate['yaxis.autorange'] = false;
        console.log("Autoscale: Setting Y-axis range to:", finalYMin, "to", finalYMax);
    } else {
        // If no data is found, force a default range. This prevents errors.
        layoutUpdate['yaxis.range[0]'] = 0;
        layoutUpdate['yaxis.range[1]'] = 100;
        layoutUpdate['yaxis.autorange'] = true;
        console.log("Autoscale: No data found, setting default range 0-100");
    }



    // Log indicator ranges that were found
    for (const [yAxisName, rangeData] of Object.entries(indicatorRanges)) {
        console.log(`Autoscale: Indicator range on ${yAxisName} - dataFound: ${rangeData.dataFound}, min: ${rangeData.min}, max: ${rangeData.max}`);
    }

    // Process and set ranges for indicator y-axes
    for (const [yAxisName, rangeData] of Object.entries(indicatorRanges)) {
        if (rangeData.dataFound && rangeData.min !== Infinity && rangeData.max !== -Infinity) {
            let indicatorPadding;
            if (rangeData.min === rangeData.max) {
                // If min and max are the same, add some padding based on the value
                indicatorPadding = Math.abs(rangeData.min) * 0.1 || 0.1; // 10% of the value or a default value
            } else {
                indicatorPadding = (rangeData.max - rangeData.min) * 0.05; // 5% padding
            }

            const finalIndicatorMin = rangeData.min - indicatorPadding;
            const finalIndicatorMax = rangeData.max + indicatorPadding;

            // Validate that the calculated Y-axis values are finite
            if (isFinite(finalIndicatorMin) && isFinite(finalIndicatorMax)) {
                // Use correct Plotly layout key format (yaxis2, yaxis3, etc.)
                const axisKey = yAxisName.replace('y', 'yaxis');
                layoutUpdate[`${axisKey}.range[0]`] = finalIndicatorMin;
                layoutUpdate[`${axisKey}.range[1]`] = finalIndicatorMax;
                layoutUpdate[`${axisKey}.autorange`] = false;
                console.log(`Autoscale: Setting ${yAxisName} (${axisKey}) range to:`, finalIndicatorMin, "to", finalIndicatorMax);
            } else {
                console.error(`Autoscale: Invalid ${yAxisName} range calculated - min:`, rangeData.min, "max:", rangeData.max, "padding:", indicatorPadding);
                console.error(`Autoscale: Skipping ${yAxisName} autoscale due to invalid range`);
                // Apply autorange as fallback
                const axisKey = yAxisName.replace('y', 'yaxis');
                layoutUpdate[`${axisKey}.autorange`] = true;
            }
        } else {
            // If no data is found for this y-axis, set autorange to true
            layoutUpdate[`${yAxisName}.autorange`] = true;
            console.log(`Autoscale: No data found for ${yAxisName}, setting autorange to true`);
        }
    }


    if (Object.keys(layoutUpdate).length > 0) {
        console.log("Autoscale: Applying layout update:", layoutUpdate);
        try {
            Plotly.relayout(plotlyGraphObj, layoutUpdate);

            // Save the new axis ranges after applying autoscale
            // Note: X-axis range is no longer modified by autoscale

            // Save Y-axis range if it was calculated
            if (priceChartYMin !== Infinity && priceChartYMax !== -Infinity) {
                window.currentYAxisRange = [priceChartYMin - yPadding, priceChartYMax + yPadding];

                // Update display elements
                if (window.yAxisMinDisplay) {
                    window.yAxisMinDisplay.textContent = (priceChartYMin - yPadding).toFixed(2);
                }
                if (window.yAxisMaxDisplay) {
                    window.yAxisMaxDisplay.textContent = (priceChartYMax + yPadding).toFixed(2);
                }

                console.log("Autoscale: Updated display elements and saved Y-axis range to window.currentYAxisRange");
            }

            // Save the new ranges to Redis
            if (typeof saveSettingsInner === 'function') {
                saveSettingsInner();
            }
            console.log("Autoscale: Settings saved");

        } catch (e) {
            console.error("Autoscale: Error during Plotly.relayout:", e);
            console.error("Autoscale: Failed layoutUpdate was:", JSON.stringify(layoutUpdate, null, 2));
            console.error("Autoscale: Current plotlyGraphObj state (data and layout):", { data: plotlyGraphObj.data, layout: plotlyGraphObj.layout, _fullLayout: plotlyGraphObj._fullLayout });
            throw e;
        }
    } else {
        console.info("Autoscale: No layout changes to apply.");
    }

    console.log("Autoscale: COMPLETED");
}


async function getLastSelectedSymbol() {
    try {
        const response = await fetch(`/get_last_symbol`, {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });
        if (response.ok) {
            const data = await response.json();
            if (data.status === 'success') {
                return data.symbol;
            } else {
                console.warn(`Could not get last selected symbol: ${data.message || 'Unknown error'}`);
                return null;
            }
        }
        console.error(`Failed to get last selected symbol: ${response.status} ${await response.text()}`);
    } catch (error) {
        console.error("Error getting last selected symbol:", error);
    }
    return null;
}

window.applyAutoscale = applyAutoscale; // Ensure it's globally available if called from config.js

// Zoom In function
function zoomIn(gd) {
    if (!gd || !gd.layout) {
        console.warn("Zoom In: Chart not available");
        return;
    }

    const currentXRange = gd.layout.xaxis.range;
    const currentYRange = gd.layout.yaxis.range;

    if (!currentXRange || !currentYRange) {
        console.warn("Zoom In: No current range available");
        return;
    }

    // Normalize ranges to numbers (handle both Date objects and numbers)
    let xMin = typeof currentXRange[0] === 'number' ? currentXRange[0] : new Date(currentXRange[0]).getTime();
    let xMax = typeof currentXRange[1] === 'number' ? currentXRange[1] : new Date(currentXRange[1]).getTime();
    let yMin = currentYRange[0];
    let yMax = currentYRange[1];

    // Calculate zoom factor (reduce range by 20%)
    const zoomFactor = 0.8;

    const xCenter = (xMin + xMax) / 2;
    const xRange = xMax - xMin;
    const newXMin = xCenter - (xRange * zoomFactor) / 2;
    const newXMax = xCenter + (xRange * zoomFactor) / 2;

    const yCenter = (yMin + yMax) / 2;
    const yRange = yMax - yMin;
    const newYMin = yCenter - (yRange * zoomFactor) / 2;
    const newYMax = yCenter + (yRange * zoomFactor) / 2;

    // For Plotly, x-axis ranges should be Date objects if the axis is date type
    const newXRange = gd.layout.xaxis.type === 'date' ?
        [new Date(newXMin), new Date(newXMax)] : [newXMin, newXMax];
    const newYRange = [newYMin, newYMax];

    Plotly.relayout(gd, {
        'xaxis.range': newXRange,
        'yaxis.range': newYRange,
        'xaxis.autorange': false,
        'yaxis.autorange': false
    });

    // Update display elements
    if (window.xAxisMinDisplay && window.xAxisMaxDisplay) {
        window.xAxisMinDisplay.textContent = new Date(newXMin).toISOString();
        window.xAxisMaxDisplay.textContent = new Date(newXMax).toISOString();
    }
    if (window.yAxisMinDisplay && window.yAxisMaxDisplay) {
        window.yAxisMinDisplay.textContent = newYMin.toFixed(2);
        window.yAxisMaxDisplay.textContent = newYMax.toFixed(2);
    }

    // Save the new ranges as numbers (milliseconds for x-axis)
    window.currentXAxisRange = [newXMin, newXMax];
    window.currentYAxisRange = [newYMin, newYMax];

    console.log("Zoom In: Applied zoom factor", zoomFactor);
}

// Zoom Out function
function zoomOut(gd) {
    if (!gd || !gd.layout) {
        console.warn("Zoom Out: Chart not available");
        return;
    }

    const currentXRange = gd.layout.xaxis.range;
    const currentYRange = gd.layout.yaxis.range;

    if (!currentXRange || !currentYRange) {
        console.warn("Zoom Out: No current range available");
        return;
    }

    // Normalize ranges to numbers (handle both Date objects and numbers)
    let xMin = typeof currentXRange[0] === 'number' ? currentXRange[0] : new Date(currentXRange[0]).getTime();
    let xMax = typeof currentXRange[1] === 'number' ? currentXRange[1] : new Date(currentXRange[1]).getTime();
    let yMin = currentYRange[0];
    let yMax = currentYRange[1];

    // Calculate zoom factor (increase range by 25%)
    const zoomFactor = 1.25;

    const xCenter = (xMin + xMax) / 2;
    const xRange = xMax - xMin;
    const newXMin = xCenter - (xRange * zoomFactor) / 2;
    const newXMax = xCenter + (xRange * zoomFactor) / 2;

    const yCenter = (yMin + yMax) / 2;
    const yRange = yMax - yMin;
    const newYMin = yCenter - (yRange * zoomFactor) / 2;
    const newYMax = yCenter + (yRange * zoomFactor) / 2;

    // For Plotly, x-axis ranges should be Date objects if the axis is date type
    const newXRange = gd.layout.xaxis.type === 'date' ?
        [new Date(newXMin), new Date(newXMax)] : [newXMin, newXMax];
    const newYRange = [newYMin, newYMax];

    Plotly.relayout(gd, {
        'xaxis.range': newXRange,
        'yaxis.range': newYRange,
        'xaxis.autorange': false,
        'yaxis.autorange': false
    });

    // Update display elements
    if (window.xAxisMinDisplay && window.xAxisMaxDisplay) {
        window.xAxisMinDisplay.textContent = new Date(newXMin).toISOString();
        window.xAxisMaxDisplay.textContent = new Date(newXMax).toISOString();
    }
    if (window.yAxisMinDisplay && window.yAxisMaxDisplay) {
        window.yAxisMinDisplay.textContent = newYMin.toFixed(2);
        window.yAxisMaxDisplay.textContent = newYMax.toFixed(2);
    }

    // Save the new ranges as numbers (milliseconds for x-axis)
    window.currentXAxisRange = [newXMin, newXMax];
    window.currentYAxisRange = [newYMin, newYMax];

    console.log("Zoom Out: Applied zoom factor", zoomFactor);
}

// Shape Properties Dialog Functions
async function populateShapePropertiesDialog(activeShape) {
    console.log('[DEBUG] populateShapePropertiesDialog called with:', activeShape);

    if (!activeShape || !activeShape.id) {
        console.warn('[DEBUG] No activeShape or activeShape.id provided');
        return;
    }

    console.log('[DEBUG] Active shape ID:', activeShape.id);

    // Set the shape ID display
    const shapeIdDisplay = document.getElementById('shape-id-display');
    if (shapeIdDisplay) {
        shapeIdDisplay.textContent = activeShape.id;
        console.log('[DEBUG] Set shape ID display to:', activeShape.id);
    }

    // Get symbol
    const symbol = window.symbolSelect ? window.symbolSelect.value : null;
    console.log('[DEBUG] Current symbol:', symbol);

    if (!symbol) {
        console.warn('[DEBUG] No symbol selected, cannot fetch shape properties');
        return;
    }

    // Set default values initially
    const startPrice = document.getElementById('start-price');
    const endPrice = document.getElementById('end-price');
    const buyOnCross = document.getElementById('buy-on-cross');
    const sellOnCross = document.getElementById('sell-on-cross');
    const amount = document.getElementById('amount');
    const amountPercent = document.getElementById('amount-percent');
    const amountUsdt = document.getElementById('amount-usdt');
    const sendEmailOnCross = document.getElementById('send-email-on-cross');
    const emailSent = document.getElementById('email-sent');
    const emailDateDisplay = document.getElementById('email-date-display');

    console.log('[DEBUG] DOM elements found:');
    console.log('[DEBUG] startPrice element:', startPrice);
    console.log('[DEBUG] endPrice element:', endPrice);
    console.log('[DEBUG] buyOnCross element:', buyOnCross);
    console.log('[DEBUG] sellOnCross element:', sellOnCross);
    console.log('[DEBUG] sendEmailOnCross element:', sendEmailOnCross);

    console.log('[DEBUG] Setting default values...');

    // Load current Y values from the shape properties endpoint
    if (startPrice && endPrice) {
        console.log('[DEBUG] startPrice and endPrice elements found');
    } else {
        console.log('[DEBUG] startPrice or endPrice elements NOT found');
    }

    if (buyOnCross) {
        buyOnCross.checked = false;
        console.log('[DEBUG] Set buyOnCross default to false');
    } else {
        console.log('[DEBUG] buyOnCross element NOT found');
    }
    if (sellOnCross) {
        sellOnCross.checked = false;
        console.log('[DEBUG] Set sellOnCross default to false');
    } else {
        console.log('[DEBUG] sellOnCross element NOT found');
    }
    if (amount) {
        amount.value = '';
        console.log('[DEBUG] Set amount default to empty');
    }
    if (amountPercent) {
        amountPercent.value = '';
        console.log('[DEBUG] Set amountPercent default to empty');
    }
    if (amountUsdt) {
        amountUsdt.value = '';
        console.log('[DEBUG] Set amountUsdt default to empty');
    }
    if (sendEmailOnCross) {
        sendEmailOnCross.checked = true;
        console.log('[DEBUG] Set sendEmailOnCross default to true');
    } else {
        console.log('[DEBUG] sendEmailOnCross element NOT found');
    }
    if (emailSent) {
        emailSent.checked = false;
        console.log('[DEBUG] Set emailSent default to false');
    }
    if (emailDateDisplay) {
        emailDateDisplay.textContent = 'Not sent yet';
        console.log('[DEBUG] Set emailDateDisplay default text');
    }

    console.log('[DEBUG] Default values set, now fetching properties...');

    // Fetch existing properties from backend via WebSocket
    try {
        console.log('[DEBUG] WebSocket connected?', window.wsAPI && window.wsAPI.connected);

        if (window.wsAPI && window.wsAPI.connected) {
            const requestId = Date.now().toString();
            console.log('[DEBUG] Sending WebSocket request with ID:', requestId);

            // Set up promise to wait for response
            const fetchPromise = new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    console.error('[DEBUG] Timeout waiting for shape properties response');
                    reject(new Error('Timeout waiting for shape properties'));
                }, 5000); // 5 second timeout

                const messageHandler = (message) => {
                    console.log('[DEBUG] Received WebSocket message:', message.type, message.request_id);

                    if (message.type === 'shape_properties_response' && message.request_id === requestId) {
                        console.log('[DEBUG] Received shape_properties_response:', message.data);
                        clearTimeout(timeout);
                        window.wsAPI.offMessage('shape_properties_response', messageHandler);
                        window.wsAPI.offMessage('error', messageHandler);
                        resolve(message.data);
                    } else if (message.type === 'error' && message.request_id === requestId) {
                        console.error('[DEBUG] Received error response:', message.message);
                        clearTimeout(timeout);
                        window.wsAPI.offMessage('shape_properties_response', messageHandler);
                        window.wsAPI.offMessage('error', messageHandler);
                        reject(new Error(message.message || 'Failed to fetch shape properties'));
                    }
                };

                // Listen for both success and error messages
                window.wsAPI.onMessage('shape_properties_response', messageHandler);
                window.wsAPI.onMessage('error', messageHandler);
            });

            // Send fetch properties message
            const requestMessage = {
                type: 'shape',
                action: 'get_properties',
                data: {
                    symbol: symbol,
                    drawing_id: activeShape.id
                },
                request_id: requestId
            };

            console.log('[DEBUG] Sending request message:', requestMessage);
            window.wsAPI.sendMessage(requestMessage);

            const result = await fetchPromise;
            console.log('[DEBUG] Fetch promise resolved with result:', result);

            if (result && result.properties) {
                const properties = result.properties;
                console.log('[DEBUG] Properties received:', properties);

                // Populate Y values from the shape object itself, not from properties
                console.log('[DEBUG] Shape object:', activeShape.shape);
                if (startPrice && activeShape.shape) {
                    if (activeShape.shape.y0 !== undefined) {
                        startPrice.value = activeShape.shape.y0;
                        console.log('[DEBUG] Set start price from shape.y0 to:', activeShape.shape.y0, 'element value:', startPrice.value);
                    } else {
                        console.log('[DEBUG] No y0 in shape object');
                    }
                }

                if (endPrice && activeShape.shape) {
                    if (activeShape.shape.y1 !== undefined) {
                        endPrice.value = activeShape.shape.y1;
                        console.log('[DEBUG] Set end price from shape.y1 to:', activeShape.shape.y1, 'element value:', endPrice.value);
                    } else {
                        console.log('[DEBUG] No y1 in shape object');
                    }
                }

                // Populate form with existing properties
                if (buyOnCross && properties.buyOnCross !== undefined) {
                    buyOnCross.checked = properties.buyOnCross;
                    console.log('[DEBUG] Set buyOnCross to:', properties.buyOnCross, 'element checked:', buyOnCross.checked);
                } else {
                    console.log('[DEBUG] buyOnCross not set - element found:', !!buyOnCross, 'property exists:', properties.buyOnCross !== undefined);
                }
                if (sellOnCross && properties.sellOnCross !== undefined) {
                    sellOnCross.checked = properties.sellOnCross;
                    console.log('[DEBUG] Set sellOnCross to:', properties.sellOnCross, 'element checked:', sellOnCross.checked);
                } else {
                    console.log('[DEBUG] sellOnCross not set - element found:', !!sellOnCross, 'property exists:', properties.sellOnCross !== undefined);
                }
                if (amount && properties.amount !== undefined) {
                    amount.value = properties.amount;
                    console.log('[DEBUG] Set amount to:', properties.amount, 'element value:', amount.value);
                } else {
                    console.log('[DEBUG] amount not set - element found:', !!amount, 'property exists:', properties.amount !== undefined);
                }
                if (amountPercent && properties.amountPercent !== undefined) {
                    amountPercent.value = properties.amountPercent;
                    console.log('[DEBUG] Set amountPercent to:', properties.amountPercent, 'element value:', amountPercent.value);
                } else {
                    console.log('[DEBUG] amountPercent not set - element found:', !!amountPercent, 'property exists:', properties.amountPercent !== undefined);
                }
                if (amountUsdt && properties.amountUsdt !== undefined) {
                    amountUsdt.value = properties.amountUsdt;
                    console.log('[DEBUG] Set amountUsdt to:', properties.amountUsdt, 'element value:', amountUsdt.value);
                } else {
                    console.log('[DEBUG] amountUsdt not set - element found:', !!amountUsdt, 'property exists:', properties.amountUsdt !== undefined);
                }
                if (sendEmailOnCross && properties.sendEmailOnCross !== undefined) {
                    sendEmailOnCross.checked = properties.sendEmailOnCross;
                    console.log('[DEBUG] Set sendEmailOnCross to:', properties.sendEmailOnCross, 'element checked:', sendEmailOnCross.checked);
                } else {
                    console.log('[DEBUG] sendEmailOnCross not set - element found:', !!sendEmailOnCross, 'property exists:', properties.sendEmailOnCross !== undefined);
                }
                if (emailSent && properties.emailSent !== undefined) {
                    emailSent.checked = properties.emailSent;
                    console.log('[DEBUG] Set emailSent to:', properties.emailSent, 'element checked:', emailSent.checked);
                } else {
                    console.log('[DEBUG] emailSent not set - element found:', !!emailSent, 'property exists:', properties.emailSent !== undefined);
                }
                if (document.getElementById('buy-sent') && properties.buy_sent !== undefined) {
                    document.getElementById('buy-sent').checked = properties.buy_sent;
                    console.log('[DEBUG] Set buy_sent to:', properties.buy_sent, 'element checked:', document.getElementById('buy-sent').checked);
                } else {
                    console.log('[DEBUG] buy_sent not set - element found:', !!document.getElementById('buy-sent'), 'property exists:', properties.buy_sent !== undefined);
                }
                if (document.getElementById('sell-sent') && properties.sell_sent !== undefined) {
                    document.getElementById('sell-sent').checked = properties.sell_sent;
                    console.log('[DEBUG] Set sell_sent to:', properties.sell_sent, 'element checked:', document.getElementById('sell-sent').checked);
                } else {
                    console.log('[DEBUG] sell_sent not set - element found:', !!document.getElementById('sell-sent'), 'property exists:', properties.sell_sent !== undefined);
                }
                if (emailDateDisplay && properties.emailDate) {
                    emailDateDisplay.textContent = new Date(properties.emailDate).toLocaleString();
                    console.log('[DEBUG] Set emailDate to:', properties.emailDate, 'element text:', emailDateDisplay.textContent);
                } else {
                    console.log('[DEBUG] emailDate not set - element found:', !!emailDateDisplay, 'property exists:', !!properties.emailDate);
                }

                // Populate alert actions
                const alertActionsElement = document.getElementById('alert-actions-display');
                if (alertActionsElement && properties.alert_actions) {
                    alertActionsElement.value = Array.isArray(properties.alert_actions) ? properties.alert_actions.join('\n') : properties.alert_actions;
                    console.log('[DEBUG] Set alert_actions to:', properties.alert_actions, 'element value:', alertActionsElement.value);
                } else {
                    console.log('[DEBUG] alert_actions not set - element found:', !!alertActionsElement, 'property exists:', !!properties.alert_actions);
                }

                // Populate resolution from the drawing data (not from properties)
                const resolutionElement = document.getElementById('shape-resolution');
                if (resolutionElement) {
                    // Get resolution from the drawing object itself, not properties
                    const drawingResolution = activeShape.shape ? activeShape.shape.resolution : null;
                    resolutionElement.value = drawingResolution || window.resolutionSelect.value || '1h';
                    console.log('[DEBUG] Set resolution to:', drawingResolution, 'fallback to:', window.resolutionSelect.value, 'element value:', resolutionElement.value);
                } else {
                    console.log('[DEBUG] resolution not set - element found:', !!resolutionElement);
                }

                console.log('[DEBUG] All properties populated successfully');
            } else {
                console.warn('[DEBUG] No properties in result:', result);
            }
        } else {
            console.warn('[DEBUG] WebSocket not connected, cannot fetch shape properties');
        }
    } catch (error) {
        console.error('[DEBUG] Error fetching shape properties:', error);
        // Continue with default values
    }
}

async function saveShapeProperties() {
    // Get values from the dialog
    const startPriceInput = document.getElementById('start-price').value;
    const endPriceInput = document.getElementById('end-price').value;
    const buyOnCross = document.getElementById('buy-on-cross').checked;
    const sellOnCross = document.getElementById('sell-on-cross').checked;
    const amountInput = document.getElementById('amount').value;
    const sendEmailOnCross = document.getElementById('send-email-on-cross').checked;

    // Get symbol and drawing ID
    const symbol = window.symbolSelect ? window.symbolSelect.value : null;
    const drawingId = document.getElementById('shape-id-display').textContent;

    if (!symbol) {
        alert('Please select a symbol first.');
        return;
    }

    if (!drawingId) {
        alert('No shape selected.');
        return;
    }

    // Validate Y values
    let startPrice, endPrice;
    if (startPriceInput.trim() !== '') {
        startPrice = parseFloat(startPriceInput);
        if (isNaN(startPrice)) {
            alert('Please enter a valid start price.');
            return;
        }
    }

    if (endPriceInput.trim() !== '') {
        endPrice = parseFloat(endPriceInput);
        if (isNaN(endPrice)) {
            alert('Please enter a valid end price.');
            return;
        }
    }

    // Prepare properties object
    const properties = {
        buyOnCross,
        sellOnCross,
        sendEmailOnCross
    };

    const buySent = document.getElementById('buy-sent') ? document.getElementById('buy-sent').checked : false;
    const sellSent = document.getElementById('sell-sent') ? document.getElementById('sell-sent').checked : false;
    const emailSent = document.getElementById('email-sent') ? document.getElementById('email-sent').checked : false;

    const amountPercentInput = document.getElementById('amount-percent').value;
    const amountUsdtInput = document.getElementById('amount-usdt').value;

    // Handle amount - convert to number if provided
    if (amountInput.trim() !== '') {
        const amount = parseFloat(amountInput);
        if (isNaN(amount) || amount <= 0) {
            alert('Please enter a valid positive amount.');
            return;
        }
        properties.amount = amount;
    }

    // Handle amount in % - convert to number if provided
    if (amountPercentInput.trim() !== '') {
        const amountPercent = parseFloat(amountPercentInput);
        if (isNaN(amountPercent) || amountPercent <= 0) {
            alert('Please enter a valid positive amount in %.');
            return;
        }
        properties.amountPercent = amountPercent;
    }

    // Handle amount in USDT - convert to number if provided
    if (amountUsdtInput.trim() !== '') {
        const amountUsdt = parseFloat(amountUsdtInput);
        if (isNaN(amountUsdt) || amountUsdt <= 0) {
            alert('Please enter a valid positive amount in USDT.');
            return;
        }
        properties.amountUsdt = amountUsdt;
    }

    // Add the special fields
    properties.buy_sent = buySent;
    properties.sell_sent = sellSent;
    properties.emailSent = emailSent;

    try {
        // Use WebSocket to save shape properties
        if (window.wsAPI && window.wsAPI.connected) {
            const requestId = Date.now().toString();

            // Set up promise to wait for response
            const savePromise = new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for shape properties save confirmation'));
                }, 5000); // 5 second timeout

                const messageHandler = (message) => {
                    if (message.type === 'shape_success' && message.request_id === requestId) {
                        clearTimeout(timeout);
                        window.wsAPI.offMessage('shape_success', messageHandler);
                        window.wsAPI.offMessage('error', messageHandler);
                        resolve(message.data);
                    } else if (message.type === 'error' && message.request_id === requestId) {
                        clearTimeout(timeout);
                        window.wsAPI.offMessage('shape_success', messageHandler);
                        window.wsAPI.offMessage('error', messageHandler);
                        reject(new Error(message.message || 'Failed to save shape properties'));
                    }
                };

                // Listen for both success and error messages
                window.wsAPI.onMessage('shape_success', messageHandler);
                window.wsAPI.onMessage('error', messageHandler);
            });

            // Send save properties message
            const resolutionValue = document.getElementById('shape-resolution').value;
            window.wsAPI.sendMessage({
                type: 'shape',
                action: 'update',
                data: {
                    symbol: symbol,
                    drawing_id: drawingId,
                    properties: properties,
                    resolution: resolutionValue && resolutionValue.trim() !== '' ? resolutionValue : window.resolutionSelect.value
                },
                request_id: requestId
            });

            const result = await savePromise;

            // Close the dialog
            closeShapePropertiesDialog();

            // Refresh the chart to show updated shape
            if (window.combinedWebSocket && window.combinedWebSocket.readyState === WebSocket.OPEN) {
                // Trigger a refresh by sending current config
                const active_indicators = Array.from(document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]:checked')).map(cb => cb.value);
                const resolution = window.resolutionSelect.value;
                const currentTime = new Date().getTime();
                let wsFromTs = new Date(currentTime - 30 * 86400 * 1000).toISOString();
                let wsToTs = new Date(currentTime + 30 * 86400 * 1000).toISOString();

                if (window.currentXAxisRange && Array.isArray(window.currentXAxisRange) && window.currentXAxisRange.length === 2) {
                    wsFromTs = new Date(window.currentXAxisRange[0]).toISOString();
                    wsToTs = new Date(window.currentXAxisRange[1]).toISOString();
                }

                setupCombinedWebSocket(symbol, active_indicators, resolution, wsFromTs, wsToTs);
            }

            // Show success message
            // alert('Shape properties saved successfully!');
        } else {
            throw new Error('WebSocket not connected');
        }
    } catch (error) {
        console.error('Error saving shape properties:', error);
        alert(`Failed to save shape properties: ${error.message}`);
    }
}

async function updateShapeYValues(symbol, drawingId, startPrice, endPrice) {
    try {
        // Get current drawings
        const response = await fetch(`/get_drawings/${symbol}`);
        const result = await response.json();

        if (response.ok && result.status === 'success') {
            const drawings = result.drawings || [];
            const drawingIndex = drawings.findIndex(d => d.id === drawingId);

            if (drawingIndex !== -1) {
                // Update the Y values
                if (startPrice !== undefined) {
                    drawings[drawingIndex].start_price = startPrice;
                }
                if (endPrice !== undefined) {
                    drawings[drawingIndex].end_price = endPrice;
                }

                // Update the drawing via PUT request
                const updateResponse = await fetch(`/update_drawing/${symbol}/${drawingId}`, {
                    method: 'PUT',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(drawings[drawingIndex])
                });

                if (updateResponse.ok) {
                } else {
                    console.error('Failed to update shape Y values');
                }
            }
        }
    } catch (error) {
        console.error('Error updating shape Y values:', error);
    }
}

function closeShapePropertiesDialog() {
    const dialog = document.getElementById('shape-properties-dialog');
    if (dialog) {
        dialog.style.display = 'none';
    }
}

// Initialize log streaming
function initializeLogStream() {
    const logElement = document.getElementById('event-output');

    if (!logElement) {
        console.error("Log element with ID 'event-output' not found in the DOM.");
        return;
    }

    const eventSource = new EventSource('/stream/logs');

    eventSource.onmessage = function(event) {
        try {
            const logLineText = JSON.parse(event.data);

            // Get current content, split into lines, and prepend the new log
            let currentLog = logElement.value;
            let lines = currentLog.split('\n');
            lines.unshift(logLineText);

            // Limit the number of lines to prevent the UI from slowing down
            if (lines.length > 200) {
                lines = lines.slice(0, 200);
            }

            logElement.value = lines.join('\n');

            // Auto-scroll to the top to show the latest message if already there
            /*
            if (logElement.scrollTop < 10) { // A small tolerance
                logElement.scrollTop = 0;
            }
            */

        } catch (e) {
            console.error("Error parsing log data:", e, "Raw data:", event.data);
        }
    };

    eventSource.onerror = function(err) {
        console.error("EventSource for logs failed:", err);
        // The browser will automatically attempt to reconnect.
    };

}

// Custom Toolbar Functionality
document.addEventListener('DOMContentLoaded', function() {
    // Pan button
    const panBtn = document.getElementById('pan-btn');
    if (panBtn) {
        panBtn.addEventListener('click', function() {
            console.log('[TOOLBAR] Pan button clicked');
            if (window.gd) {
                console.log('[TOOLBAR] Current dragmode before pan:', window.gd.layout.dragmode);
                console.log('[TOOLBAR] Setting dragmode to pan');
                Plotly.relayout(window.gd, { dragmode: 'pan' });

                // Update button states
                updateToolbarButtonStates('pan');
                console.log('[TOOLBAR] Pan mode activated');
                console.log('Plotly config:', window.gd.config || window.config || 'config not found');
            } else {
                console.log('[TOOLBAR] Chart not ready (window.gd is null)');
            }
        });
    }

    // Zoom button
    const zoomBtn = document.getElementById('zoom-btn');
    if (zoomBtn) {
        zoomBtn.addEventListener('click', function() {
            console.log('[TOOLBAR] Zoom button clicked');
            if (window.gd) {
                console.log('[TOOLBAR] Current dragmode before zoom:', window.gd.layout.dragmode);
                console.log('[TOOLBAR] Setting dragmode to zoom');

                Plotly.relayout(window.gd, { dragmode: 'zoom' }).then(function() {
                    console.log('[TOOLBAR] After relayout - dragmode is:', window.gd.layout.dragmode);
                    console.log('[TOOLBAR] Chart layout after zoom relayout:', JSON.stringify(window.gd.layout, null, 2));

                    // Update button states
                    updateToolbarButtonStates('zoom');
                    console.log('[TOOLBAR] Zoom mode activated - button states updated');
                    console.log('Plotly config:', window.gd.config || window.config || 'config not found');
                }).catch(function(error) {
                    console.error('[TOOLBAR] Zoom relayout failed:', error);
                });
            } else {
                console.log('[TOOLBAR] Chart not ready (window.gd is null)');
            }
        });
    }

    // Draw Line button
    const drawlineBtn = document.getElementById('drawline-btn');
    if (drawlineBtn) {
        drawlineBtn.addEventListener('click', function() {
            console.log('[TOOLBAR] Draw Line button clicked');
            if (window.gd) {
                console.log('[TOOLBAR] Current dragmode before drawline:', window.gd.layout.dragmode);
                console.log('[TOOLBAR] Setting dragmode to drawline');

                // Use programmatic approach directly (like pan/zoom buttons)
                Plotly.relayout(window.gd, {
                    newshape: {
                        type: 'line',
                        line: { color: DEFAULT_DRAWING_COLOR, width: 2 },
                        layer: 'above'
                    },
                    dragmode: 'drawline'
                }).then(() => {
                    console.log('[TOOLBAR] Newshape and dragmode set for line, final dragmode:', window.gd.layout.dragmode);
                    console.log('[TOOLBAR] Layout after line button click:', JSON.stringify(window.gd.layout, null, 2));

                    // Update button states
                    updateToolbarButtonStates('drawline');
                    console.log('[TOOLBAR] Draw line mode activated');
                    console.log('Plotly config:', window.gd.config || window.config || 'config not found');
                }).catch((error) => {
                    console.error('[TOOLBAR] Failed to set newshape and dragmode for line:', error);
                });
            } else {
                console.log('[TOOLBAR] Chart not ready (window.gd is null)');
            }
        });
    }

    // Draw Rectangle button
    const drawrectBtn = document.getElementById('drawrect-btn');
    if (drawrectBtn) {
        drawrectBtn.addEventListener('click', function() {
            console.log('[TOOLBAR] Draw Rectangle button clicked');
            if (window.gd) {
                console.log('[TOOLBAR] Current dragmode before drawrect:', window.gd.layout.dragmode);
                console.log('[TOOLBAR] Setting dragmode to drawrect');

                // Use programmatic approach directly (like pan/zoom buttons)
                Plotly.relayout(window.gd, {
                    newshape: {
                        type: 'rect',
                        line: { color: DEFAULT_DRAWING_COLOR, width: 2 },
                        fillcolor: 'rgba(0, 0, 255, 0.1)',
                        opacity: 0.5,
                        layer: 'above'
                    },
                    dragmode: 'drawrect'
                }).then(() => {
                    console.log('[TOOLBAR] Newshape and dragmode set for rectangle, final dragmode:', window.gd.layout.dragmode);
                    console.log('[TOOLBAR] Layout after rectangle button click:', JSON.stringify(window.gd.layout, null, 2));

                    // Update button states
                    updateToolbarButtonStates('drawrect');
                    console.log('[TOOLBAR] Draw rectangle mode activated');
                    console.log('Plotly config:', window.gd.config || window.config || 'config not found');
                }).catch((error) => {
                    console.error('[TOOLBAR] Failed to set newshape and dragmode for rectangle:', error);
                });
            } else {
                console.log('[TOOLBAR] Chart not ready (window.gd is null)');
            }
        });
    }

    // Screenshot button
    const screenshotBtn = document.getElementById('screenshot-btn');
    if (screenshotBtn) {
        screenshotBtn.addEventListener('click', function() {
            if (window.gd) {
                try {
                    // Use Plotly's built-in download function
                    Plotly.downloadImage(window.gd, {
                        format: 'png',
                        width: 1200,
                        height: 800,
                        filename: `chart-${window.symbolSelect ? window.symbolSelect.value : 'trading'}-${new Date().toISOString().split('T')[0]}`
                    });
                    console.log('Plotly config:', window.gd.config || window.config || 'config not found');
                } catch (error) {
                    console.error('Screenshot failed:', error);
                    alert('Screenshot failed. Please try again.');
                }
            }
        });
    }

    // Zoom In button
    const zoomInBtn = document.getElementById('zoom-in-btn');
    if (zoomInBtn) {
        zoomInBtn.addEventListener('click', function() {
            if (window.gd) {
                zoomIn(window.gd);
                console.log('Plotly config:', window.gd.config || window.config || 'config not found');
            }
        });
    }

    // Zoom Out button
    const zoomOutBtn = document.getElementById('zoom-out-btn');
    if (zoomOutBtn) {
        zoomOutBtn.addEventListener('click', function() {
            if (window.gd) {
                zoomOut(window.gd);
                console.log('Plotly config:', window.gd.config || window.config || 'config not found');
            }
        });
    }

    // Auto Scale button
    const autoscaleBtn = document.getElementById('autoscale-btn');
    if (autoscaleBtn) {
        autoscaleBtn.addEventListener('click', function() {
            if (window.gd && typeof applyAutoscale === 'function') {
                applyAutoscale(window.gd);
                console.log('Plotly config:', window.gd.config || window.config || 'config not found');
            }
        });
    }

    // Initialize button states
    updateToolbarButtonStates();
});

// Function to update toolbar button states based on current dragmode
function updateToolbarButtonStates(activeMode) {
    const panBtn = document.getElementById('pan-btn');
    const zoomBtn = document.getElementById('zoom-btn');
    const drawlineBtn = document.getElementById('drawline-btn');
    const drawrectBtn = document.getElementById('drawrect-btn');

    if (!panBtn || !zoomBtn || !drawlineBtn || !drawrectBtn) return;

    // Remove active class from all buttons
    panBtn.classList.remove('active');
    zoomBtn.classList.remove('active');
    drawlineBtn.classList.remove('active');
    drawrectBtn.classList.remove('active');

    // Add active class to current mode button
    if (activeMode === 'pan' || (!activeMode && window.gd && window.gd.layout.dragmode === 'pan')) {
        panBtn.classList.add('active');
    } else if (activeMode === 'zoom' || (!activeMode && window.gd && window.gd.layout.dragmode === 'zoom')) {
        zoomBtn.classList.add('active');
    } else if (activeMode === 'drawline' || (!activeMode && window.gd && window.gd.layout.dragmode === 'drawline')) {
        drawlineBtn.classList.add('active');
    } else if (activeMode === 'drawrect' || (!activeMode && window.gd && window.gd.layout.dragmode === 'drawrect')) {
        drawrectBtn.classList.add('active');
    }
}

// Update toolbar states when dragmode changes
document.addEventListener('plotly_relayout', function(event) {
    if (event.detail && event.detail.dragmode) {
        updateToolbarButtonStates(event.detail.dragmode);
    }
});



// Apply URL chart view parameters to restore chart state
function applyUrlChartViewParameters() {
    console.log('[URL_PARAMS] Applying URL chart view parameters...');

    // Check if URL parameters are available
    if (!window.urlStartTs || !window.urlEndTs || !window.urlYMin || !window.urlYMax) {
        console.log('[URL_PARAMS] No URL parameters found, skipping chart view restoration');
        return;
    }

    // Parse the parameters
    const startTs = parseFloat(window.urlStartTs);
    const endTs = parseFloat(window.urlEndTs);
    const yMin = parseFloat(window.urlYMin);
    const yMax = parseFloat(window.urlYMax);

    // Validate parameters
    if (isNaN(startTs) || isNaN(endTs) || isNaN(yMin) || isNaN(yMax)) {
        console.warn('[URL_PARAMS] Invalid URL parameters, skipping chart view restoration');
        return;
    }

    // Set the axis ranges
    window.currentXAxisRange = [startTs, endTs];
    window.currentYAxisRange = [yMin, yMax];

    // Update display elements
    if (window.xAxisMinDisplay && window.xAxisMaxDisplay) {
        window.xAxisMinDisplay.textContent = new Date(startTs).toISOString();
        window.xAxisMaxDisplay.textContent = new Date(endTs).toISOString();
    }
    if (window.yAxisMinDisplay && window.yAxisMaxDisplay) {
        window.yAxisMinDisplay.textContent = yMin.toFixed(2);
        window.yAxisMaxDisplay.textContent = yMax.toFixed(2);
    }

    console.log(`[URL_PARAMS] Applied chart view from URL: x-axis [${new Date(startTs).toISOString()} to ${new Date(endTs).toISOString()}], y-axis [${yMin.toFixed(2)} to ${yMax.toFixed(2)}]`);

    // Apply to chart if it's already initialized
    if (window.gd) {
        Plotly.relayout(window.gd, {
            'xaxis.range': [new Date(startTs), new Date(endTs)],
            'yaxis.range': [yMin, yMax],
            'xaxis.autorange': false,
            'yaxis.autorange': false
        }).then(() => {
            console.log('[URL_PARAMS] Chart view applied successfully');
        }).catch(error => {
            console.error('[URL_PARAMS] Error applying chart view:', error);
        });
    }
}

// Update minimum value slider range based on trade data
function updateMinValueSliderRange() {
    const minValueSlider = document.getElementById('min-value-slider');

    if (!minValueSlider) return;

    // Check if trade history data exists
    if (!window.tradeHistoryData || window.tradeHistoryData.length === 0) {
        // No trade data - use default percentage range
        minValueSlider.min = 0;
        minValueSlider.max = 1; // 1 (100%) default max for percentage mode
        minValueSlider.step = 0.1;
        return;
    }

    // Extract all USD value values from trade history (price * amount)
    const valueValues = window.tradeHistoryData.map(trade => trade.price * trade.amount).filter(value =>
        typeof value === 'number' && !isNaN(value) && value > 0
    );

    if (valueValues.length === 0) {
        // No valid value data - use default percentage range
        minValueSlider.min = 0;
        minValueSlider.max = 1;
        minValueSlider.step = 0.1;
        return;
    }

    // Find maximum value for percentage calculations
    const maxValue = Math.max(...valueValues);

    // Store maximum trade value globally for percentage calculations
    window.maxTradeValue = maxValue;

    // For percentage mode, slider is always 0-1, representing 0%-100% of max value
    minValueSlider.min = 0;
    minValueSlider.max = 1;
    minValueSlider.step = 0.1;

    // Update the max value display
    updateMaxValueDisplay();

    // Ensure current value stays within percentage range
    const currentValue = parseFloat(minValueSlider.value);
    if (currentValue > 1) {
        minValueSlider.value = 1;
        updateMinValueDisplay();
    } else if (currentValue < 0) {
        minValueSlider.value = 0;
        updateMinValueDisplay();
    }

    console.log(`Min Value slider updated: max=${maxValue.toLocaleString()}, percentage range 0-1, trades=${window.tradeHistoryData.length}`);
}

// Update min volume display value
function updateMinValueDisplay() {
    const minValueSlider = document.getElementById('min-value-slider');
    const minVolumeValue = document.getElementById('min-volume-value');

    if (minValueSlider && minVolumeValue) {
        const currentValue = parseFloat(minValueSlider.value);
        // Display percentage (0-100% instead of 0-1)
        const percentageValue = (currentValue * 100).toFixed(1);
        minVolumeValue.textContent = `${percentageValue}%`;
    }
}

// Update max volume display value (shows the current slider maximum range)
function updateMaxValueDisplay() {
    const minValueSlider = document.getElementById('min-value-slider');
    const maxVolumeValue = document.getElementById('max-volume-value');

    if (minValueSlider && maxVolumeValue) {
        const maxValue = parseFloat(minValueSlider.max);
        maxVolumeValue.textContent = maxValue.toLocaleString();
    }
}

// Handle minimum volume slider change
function handleMinValueChange() {
    updateMinValueDisplay();

    // Clear any existing timeout to reset the timer
    if (window.minValueChangeTimeout) {
        clearTimeout(window.minValueChangeTimeout);
    }

    // Set a new timeout to send the message after 2 seconds
    window.minValueChangeTimeout = setTimeout(() => {
        const minValueSlider = document.getElementById('min-value-slider');
        const minPercentage = minValueSlider ? parseFloat(minValueSlider.value) || 0 : 0;
        const symbol = window.symbolSelect ? window.symbolSelect.value : 'BTCUSDT';
        const email = window.userEmail || '';

        // Send trade_history message to update filter
        window.wsAPI.sendMessage({
            type: 'trade_history',
            data: {
                symbol: symbol,
                email: email,
                minValuePercentage: minPercentage,
                from_ts: window.currentXAxisRange ? Math.floor(window.currentXAxisRange[0] / 1000) : Math.floor((Date.now() - 30 * 86400 * 1000) / 1000),
                to_ts: window.currentXAxisRange ? Math.floor(window.currentXAxisRange[1] / 1000) : Math.floor(Date.now() / 1000)
            }
        });

        // Reset the timeout
        window.minValueChangeTimeout = null;

        // Save settings if function exists
        if (typeof saveSettingsInner === 'function') {
            saveSettingsInner();
        }
    }, 2000); // 2 second delay
}


function updateChartWithDataAndShapes(ohlcv, active_indicators, drawings, symbol) {
    if (!window.gd) {
        console.warn('Chart not ready for combined OHLCV, indicators, and shapes update');
        return;
    }

    // Get existing traces to preserve non-OHLCV/indicator traces
    const existingTraces = window.gd.data || [];

    // Build set of trace names that will be replaced
    const tracesToReplace = new Set(['Price']);

    // Indicator configurations for plotting
    const indicatorConfigs = {
        'macd': [
            { key: 'macd', name: 'MACD', color: 'blue', type: 'scatter', mode: 'lines' },
            { key: 'signal', name: 'MACD Signal', color: 'red', type: 'scatter', mode: 'lines' },
            { key: 'histogram', name: 'MACD Histogram', color: 'green', type: 'bar' }
        ],
        'rsi': [
            { key: 'rsi', name: 'RSI', color: 'purple', type: 'scatter', mode: 'lines' }
        ],
        'stochrsi_9_3': [
            { key: 'stoch_k', name: 'StochRSI K (9,3)', color: 'orange', type: 'scatter', mode: 'lines' },
            { key: 'stoch_d', name: 'StochRSI D (9,3)', color: 'brown', type: 'scatter', mode: 'lines' }
        ],
        'stochrsi_14_3': [
            { key: 'stoch_k', name: 'StochRSI K (14,3)', color: 'cyan', type: 'scatter', mode: 'lines' },
            { key: 'stoch_d', name: 'StochRSI D (14,3)', color: 'magenta', type: 'scatter', mode: 'lines' }
        ],
        'stochrsi_40_4': [
            { key: 'stoch_k', name: 'StochRSI K (40,4)', color: 'lime', type: 'scatter', mode: 'lines' },
            { key: 'stoch_d', name: 'StochRSI D (40,4)', color: 'teal', type: 'scatter', mode: 'lines' }
        ],
        'stochrsi_60_10': [
            { key: 'stoch_k', name: 'StochRSI K (60,10)', color: 'pink', type: 'scatter', mode: 'lines' },
            { key: 'stoch_d', name: 'StochRSI D (60,10)', color: 'navy', type: 'scatter', mode: 'lines' }
        ],
        'cto_line': [
            { key: 'cto_upper', name: 'CTO Upper', color: 'darkgreen', type: 'scatter', mode: 'lines' },
            { key: 'cto_lower', name: 'CTO Lower', color: 'darkred', type: 'scatter', mode: 'lines' }
        ]
    };

    // Add indicator trace names to the set of traces to replace
    if (active_indicators && Array.isArray(active_indicators)) {
        active_indicators.forEach(indicatorName => {
            const config = indicatorConfigs[indicatorName];
            if (config) {
                config.forEach(subConfig => {
                    tracesToReplace.add(subConfig.name);
                });
            }
        });
    }

    // Preserve existing traces that are not being replaced
    const preservedTraces = existingTraces.filter(trace => !tracesToReplace.has(trace.name));

    // Create candlestick trace from OHLCV data
    const candlestickTrace = {
        x: ohlcv.map(point => new Date(point.time * 1000)),
        open: ohlcv.map(point => point.ohlc.open),
        high: ohlcv.map(point => point.ohlc.high),
        low: ohlcv.map(point => point.ohlc.low),
        close: ohlcv.map(point => point.ohlc.close),
        volume: ohlcv.map(point => point.ohlc.volume),
        type: 'candlestick',
        name: 'Price',
        xaxis: 'x',
        yaxis: 'y',
        increasing: { line: { color: 'green' } },
        decreasing: { line: { color: 'red' } }
    };

    // Start with preserved traces and add the candlestick trace
    const traces = [...preservedTraces, candlestickTrace];

    // Process active indicators
    if (active_indicators && Array.isArray(active_indicators) && active_indicators.length > 0) {
        // FORCE indicator order to match backend configuration
        const forcedIndicatorOrder = ['macd', 'rsi', 'stochrsi_9_3', 'stochrsi_14_3', 'stochrsi_40_4', 'stochrsi_60_10', 'open_interest', 'jma', 'cto_line'];
        const orderedActiveIndicators = forcedIndicatorOrder.filter(indicatorId => active_indicators.includes(indicatorId));

        orderedActiveIndicators.forEach((indicatorName, indicatorIndex) => {
            const config = indicatorConfigs[indicatorName];
            if (config) {
                const yAxisName = `y${indicatorIndex + 2}`; // y2, y3, y4, etc.

                // DEBUG: Log OI data specifically
                if (indicatorName === 'open_interest') {
                    console.log('[DEBUG OI] Processing Open Interest indicator');
                    console.log('[DEBUG OI] OHLCV data sample:', ohlcv.slice(0, 3).map(p => ({
                        time: p.time,
                        indicators: p.indicators
                    })));
                }

                config.forEach(subConfig => {
                    // Extract data for this sub-indicator
                    const yData = ohlcv.map(point => {
                        const indicatorData = point.indicators && point.indicators[indicatorName];
                        return indicatorData ? indicatorData[subConfig.key] : null;
                    }).filter(val => val !== null); // Filter out nulls, but keep for plotting

                    // DEBUG: Log OI data extraction
                    if (indicatorName === 'open_interest') {
                        console.log(`[DEBUG OI] Extracted ${yData.length} non-null values for ${subConfig.key}`);
                        console.log(`[DEBUG OI] First 5 values:`, yData.slice(0, 5));
                        console.log(`[DEBUG OI] All values are null:`, yData.every(val => val === null));
                    }

                    // Only add trace if we have data
                    if (yData.some(val => val !== null)) {
                        const indicatorTrace = {
                            x: ohlcv.map(point => new Date(point.time * 1000)),
                            y: ohlcv.map(point => {
                                const indicatorData = point.indicators && point.indicators[indicatorName];
                                return indicatorData ? indicatorData[subConfig.key] : null;
                            }),
                            type: subConfig.type,
                            mode: subConfig.mode,
                            name: subConfig.name,
                            ...(subConfig.type !== 'bar' && { line: { color: subConfig.color } }),
                            ...(subConfig.type === 'bar' && { marker: { color: subConfig.color } }),
                            xaxis: 'x',
                            yaxis: yAxisName // Assign to correct y-axis based on indicator order
                        };
                        traces.push(indicatorTrace);

                        // DEBUG: Log OI trace creation
                        if (indicatorName === 'open_interest') {
                            console.log('[DEBUG OI] Created OI trace:', {
                                name: subConfig.name,
                                yAxis: yAxisName,
                                dataPoints: indicatorTrace.y.length,
                                nonNullValues: indicatorTrace.y.filter(v => v !== null).length
                            });
                        }
                    } else {
                        // DEBUG: Log when OI trace is not created
                        if (indicatorName === 'open_interest') {
                            console.log(`[DEBUG OI] Skipping OI trace for ${subConfig.key} - no valid data`);
                        }
                    }
                });
            }
        });
    }

    // Update layout if indicators are present (use existing updateLayoutForIndicators function)
    let updatedLayout = window.gd.layout;
    if (active_indicators && active_indicators.length > 0 && window.updateLayoutForIndicators) {
        // FORCE indicator order to match backend configuration for layout
        const forcedIndicatorOrder = ['macd', 'rsi', 'stochrsi_9_3', 'stochrsi_14_3', 'stochrsi_40_4', 'stochrsi_60_10', 'open_interest', 'jma', 'cto_line'];
        const orderedActiveIndicators = forcedIndicatorOrder.filter(indicatorId => active_indicators.includes(indicatorId));

        // DEBUG: Log layout update for OI
        console.log('[DEBUG OI] Updating layout for indicators:', orderedActiveIndicators);
        console.log('[DEBUG OI] OI in ordered indicators:', orderedActiveIndicators.includes('open_interest'));

        updatedLayout = window.updateLayoutForIndicators(orderedActiveIndicators);
    }

    // Process drawings/shapes
    if (!updatedLayout.shapes) {
        updatedLayout.shapes = [];
    }

    console.log(`🎨 Updating chart with ${ohlcv.length} OHLCV points, ${active_indicators ? active_indicators.length : 0} indicators, and ${drawings.length} drawings for ${symbol}`);

    // DEBUG: Log final traces for OI
    const oiTraces = traces.filter(t => t.name && t.name.toLowerCase().includes('open'));
    console.log('[DEBUG OI] Final OI traces:', oiTraces.map(t => ({ name: t.name, dataPoints: t.y ? t.y.length : 0, nonNull: t.y ? t.y.filter(v => v !== null).length : 0 })));

    // Debug: Check if OHLCV data has indicators
    if (ohlcv.length > 0) {
        const samplePoint = ohlcv[0];
        console.log(`[DEBUG] Sample OHLCV point:`, {
            time: samplePoint.time,
            hasIndicators: !!samplePoint.indicators,
            indicatorsKeys: samplePoint.indicators ? Object.keys(samplePoint.indicators) : [],
            openInterest: samplePoint.indicators?.open_interest?.open_interest
        });
    }

    // Process each drawing
    drawings.forEach((drawing, index) => {
        try {
            // Convert drawing data to Plotly shape format
            const shape = convertDrawingToShape(drawing);

            if (shape) {
                // Check if shape already exists (by id)
                const existingIndex = updatedLayout.shapes.findIndex(s => s.id === drawing.id);

                if (existingIndex !== -1) {
                    // Update existing shape
                    updatedLayout.shapes[existingIndex] = shape;
                } else {
                    // Add new shape
                    updatedLayout.shapes.push(shape);
                }
            } else {
                console.warn(`Combined WebSocket: Could not convert drawing to shape:`, drawing);
            }
        } catch (error) {
            console.error(`Combined WebSocket: Error processing drawing ${index}:`, error, drawing);
        }
    });

    // Update the chart with traces and the updated layout in a single Plotly.react call
    Plotly.react(window.gd, traces, updatedLayout).then(() => {
        console.log(`✅ Chart updated with ${ohlcv.length} OHLCV points, ${active_indicators ? active_indicators.length : 0} active indicators, and ${drawings.length} drawings`);

        // Debug: Check final traces after update
        setTimeout(() => {
            if (window.gd && window.gd.data) {
                const indicatorTraces = window.gd.data.filter(t => t.name && t.name !== 'Price');
                console.log(`[DEBUG] Final chart traces:`, indicatorTraces.map(t => ({
                    name: t.name,
                    yaxis: t.yaxis,
                    dataPoints: t.y ? t.y.filter(v => v !== null && v !== undefined).length : 0,
                    firstValue: t.y ? t.y.find(v => v !== null && v !== undefined) : null
                })));
            }
        }, 100);
    }).catch(error => {
        console.error('❌ Error updating chart with combined data:', error);
    });
}

function updateChartShapes(drawings, symbol) {
    const chartElement = document.getElementById('chart');
    if (!chartElement || !window.gd) {
        console.warn('Combined WebSocket: Chart not ready for drawings update');
        return;
    }

    // Ensure layout.shapes exists
    if (!window.gd.layout.shapes) {
        window.gd.layout.shapes = [];
    }

    console.log(`🎨 Updating chart with ${drawings.length} drawings for ${symbol}`);

    // Process each drawing
    drawings.forEach((drawing, index) => {
        try {
            // Convert drawing data to Plotly shape format
            const shape = convertDrawingToShape(drawing);

            if (shape) {
                // Check if shape already exists (by id)
                const existingIndex = window.gd.layout.shapes.findIndex(s => s.id === drawing.id);

                if (existingIndex !== -1) {
                    // Update existing shape
                    window.gd.layout.shapes[existingIndex] = shape;
                } else {
                    // Add new shape
                    window.gd.layout.shapes.push(shape);
                }
            } else {
                console.warn(`Combined WebSocket: Could not convert drawing to shape:`, drawing);
            }
        } catch (error) {
            console.error(`Combined WebSocket: Error processing drawing ${index}:`, error, drawing);
        }
    });

    // Update the chart with new shapes - ensure shapes are preserved during chart updates
    try {
        // First, ensure the layout has a shapes array
        if (!window.gd.layout.shapes) {
            window.gd.layout.shapes = [];
        }

        // Update the chart with shapes using relayout to preserve existing data
        Plotly.relayout(chartElement, {
            shapes: window.gd.layout.shapes
        });
        console.log(`🎨 Successfully updated chart with shapes

        `);
    } catch (error) {
        console.error('❌ Error updating shapes:', error);
    }
}
/*  */