let isLoadingSettings = false;
let isRestoringIndicators = false;
let isProgrammaticallySettingResolution = false;
let isProgrammaticallySettingRange = false;

// Debounce utility function
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}



async function populateDropdowns() {
    try {
        // Use symbols from init_success message (stored in window.availableSymbols)
        const symbols = window.availableSymbols || [];

        // Populate symbols dropdown
        window.symbolSelect.innerHTML = '';
        symbols.forEach(symbol => {
            const option = document.createElement('option');
            option.value = symbol;
            option.textContent = symbol.replace('USDT', '/USDT');
            window.symbolSelect.appendChild(option);
        });

        // Populate resolutions dropdown with hardcoded values (matching server config)
        window.resolutionSelect.innerHTML = '';
        const resolutions = ["1m", "5m", "1h", "1d", "1w"];
        resolutions.forEach(resolution => {
            const option = document.createElement('option');
            option.value = resolution;
            option.textContent = resolution;
            window.resolutionSelect.appendChild(option);
        });

        // Populate ranges dropdown with hardcoded values (matching server config)
        window.rangeSelect.innerHTML = '';
        const ranges = [
            {"value": "1h", "label": "1h"},
            {"value": "8h", "label": "8h"},
            {"value": "24h", "label": "24h"},
            {"value": "3d", "label": "3d"},
            {"value": "7d", "label": "7d"},
            {"value": "30d", "label": "30d"},
            {"value": "3m", "label": "3M"},
            {"value": "6m", "label": "6M"},
            {"value": "1y", "label": "1Y"},
            {"value": "3y", "label": "3Y"}
        ];
        ranges.forEach(range => {
            const option = document.createElement('option');
            option.value = range.value;
            option.textContent = range.label;
            window.rangeSelect.appendChild(option);
        });

    } catch (error) {
        console.error('Error populating dropdowns:', error);
        // Fallback: populate with known symbols
        if (window.symbolSelect.options.length === 0) {
            const fallbackSymbols = ["BTCUSDT", "XMRUSDT", "ETHUSDT", "SOLUSDT", "SUIUSDT", "PAXGUSDT", "BNBUSDT", "ADAUSDT"];
            window.symbolSelect.innerHTML = '';
            fallbackSymbols.forEach(symbol => {
                const option = document.createElement('option');
                option.value = symbol;
                option.textContent = symbol.replace('USDT', '/USDT');
                window.symbolSelect.appendChild(option);
            });
        }
        if (window.resolutionSelect.options.length === 0) window.resolutionSelect.innerHTML = '<option value="1d">1 Day</option>';
        if (window.rangeSelect.options.length === 0) window.rangeSelect.innerHTML = '<option value="30d">30d</option>';
    }
}

function loadSettings(symbolOverride = null) {
    isLoadingSettings = true; // Prevent saveSettings from running during load

    // Use provided symbol override (e.g., from URL) or fall back to dropdown value
    const currentSymbol = symbolOverride || window.symbolSelect.value;
    if (!currentSymbol) {
        console.error("loadSettings: No symbol available. This should not happen - dropdown should be populated first.");
        isLoadingSettings = false;
        return;
    }

    updateSelectedShapeInfoPanel(null); // Assumes updateSelectedShapeInfoPanel is globally available

    // Send WebSocket message for settings
    window.wsAPI.sendMessage({
        type: 'config',
        action: 'get_settings',
        data: { symbol: currentSymbol, email: window.userEmail || '' }
    });
}

// Handler for settings response
function handleSettingsResponse(message) {
    if (message.action !== 'get_settings') return;

    const settings = message.data;
    isLoadingSettings = false; // Allow saves again

    // Always apply saved settings to UI controls - this is the expected behavior

    // Set resolution programmatically (without triggering change event)
    if (settings.resolution && window.resolutionSelect.options.length > 0) {
        isProgrammaticallySettingResolution = true;
        window.resolutionSelect.value = settings.resolution;
        // Reset flag after a short delay to allow any event processing
        delay(10).then(() => {
            isProgrammaticallySettingResolution = false;
        });
    }

    // Set range programmatically (without triggering change event)
    if (settings.range && window.rangeSelect.options.length > 0) {
        isProgrammaticallySettingRange = true;
        window.rangeSelect.value = settings.range;
        // Reset flag after a short delay to allow any event processing
        delay(10).then(() => {
            isProgrammaticallySettingRange = false;
        });
    }

    if (settings.xAxisMin !== null && settings.xAxisMax !== null && typeof settings.xAxisMin !== 'undefined') {
        // Check if timestamps are in seconds or milliseconds
        let minTimestamp = settings.xAxisMin;
        let maxTimestamp = settings.xAxisMax;

        // Check for old alarm dates (before year 2000)
        const minDate = new Date(settings.xAxisMin < 1e10 ? settings.xAxisMin * 1000 : settings.xAxisMin);
        const maxDate = new Date(settings.xAxisMax < 1e10 ? settings.xAxisMax * 1000 : settings.xAxisMax);

        // Handle timestamp format detection and conversion
        // Current system uses seconds (1.75e9 range), but we need milliseconds for JavaScript Date objects
        if (settings.xAxisMin < 1e10) { // Less than 10 billion (reasonable for seconds since 1970)
            // Convert seconds to milliseconds for JavaScript Date objects
            minTimestamp = settings.xAxisMin * 1000;
            maxTimestamp = settings.xAxisMax * 1000;
        }
        // Handle legacy timestamps that were incorrectly saved as milliseconds but represent old dates
        else if (settings.xAxisMin < 1e12 && new Date(settings.xAxisMin).getFullYear() < 2000) {
            // These are old millisecond timestamps that need to be converted to proper milliseconds
            minTimestamp = Math.floor(settings.xAxisMin / 1000);
            maxTimestamp = Math.floor(settings.xAxisMax / 1000);
        }
        // Handle current millisecond timestamps (1.75e12 range for 2025 dates)
        else {
            // Already in milliseconds, use as-is
            minTimestamp = settings.xAxisMin;
            maxTimestamp = settings.xAxisMax;
        }

        // Only set currentXAxisRange if it's not already set (preserves user's current panning)
        if (!window.currentXAxisRange) {
            const minDate = new Date(minTimestamp);
            const maxDate = new Date(maxTimestamp);
            if (minDate.getFullYear() < 2000 || maxDate.getFullYear() < 2000 || isNaN(minDate.getTime()) || isNaN(maxDate.getTime())) {
                console.warn('🚨 Old or invalid dates detected in settings, resetting x-axis to auto range');
                window.currentXAxisRange = null;
                window.xAxisMinDisplay.textContent = 'Auto';
                window.xAxisMaxDisplay.textContent = 'Auto';
            } else {
                window.currentXAxisRange = [minTimestamp, maxTimestamp];
                window.xAxisMinDisplay.textContent = isNaN(minDate.getTime()) ? 'Invalid Date' : minDate.toISOString();
                window.xAxisMaxDisplay.textContent = isNaN(maxDate.getTime()) ? 'Invalid Date' : maxDate.toISOString();
            }
        } else {
            // If already set, update display
            if (window.currentXAxisRange) {
                const minDateDisplay = new Date(window.currentXAxisRange[0]);
                const maxDateDisplay = new Date(window.currentXAxisRange[1]);
                window.xAxisMinDisplay.textContent = isNaN(minDateDisplay.getTime()) ? 'Invalid Date' : minDateDisplay.toISOString();
                window.xAxisMaxDisplay.textContent = isNaN(maxDateDisplay.getTime()) ? 'Invalid Date' : maxDateDisplay.toISOString();
            } else {
                window.xAxisMinDisplay.textContent = 'Auto';
                window.xAxisMaxDisplay.textContent = 'Auto';
            }
        }

    } else {
        window.currentXAxisRange = null;
        window.xAxisMinDisplay.textContent = 'Auto';
        window.xAxisMaxDisplay.textContent = 'Auto';
    }

   if (settings.yAxisMin !== null && settings.yAxisMax !== null && typeof settings.yAxisMin !== 'undefined') {
         // Check for unusual Y-axis ranges that might indicate old/problematic settings
         const yMin = parseFloat(settings.yAxisMin);
         const yMax = parseFloat(settings.yAxisMax);

         // Only set currentYAxisRange if it's not already set (preserves user's current zoom)
         if (!window.currentYAxisRange) {
             window.currentYAxisRange = [settings.yAxisMin, settings.yAxisMax];
             window.yAxisMinDisplay.textContent = settings.yAxisMin.toFixed(2);
             window.yAxisMaxDisplay.textContent = settings.yAxisMax.toFixed(2);
         } else {
         }
     } else {
         // Only clear if not already set by user
         if (!window.currentYAxisRange) {
             window.currentYAxisRange = null;
             window.yAxisMinDisplay.textContent = 'Auto';
             window.yAxisMaxDisplay.textContent = 'Auto';
         }
     }

    // Restore active indicators - ensure DOM is ready
    const restoreIndicators = () => {
        const allIndicatorCheckboxes = document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]');

        if (allIndicatorCheckboxes.length === 0) {
            console.warn('[DEBUG settingsManager] No indicator checkboxes found, will retry in 100ms');
            setTimeout(restoreIndicators, 100);
            return;
        }

        // Set flag to prevent saves during restoration
        isRestoringIndicators = true;

        // First uncheck all
        allIndicatorCheckboxes.forEach(checkbox => checkbox.checked = false);

        // Then check the active ones
        if (settings.active_indicators && Array.isArray(settings.active_indicators) && settings.active_indicators.length > 0) {
            settings.active_indicators.forEach(indicatorValue => {
                const checkbox = document.querySelector(`#indicator-checkbox-list input[type="checkbox"][value="${indicatorValue}"]`);
                if (checkbox) {
                    checkbox.checked = true;
                } else {
                    console.warn('[DEBUG settingsManager] Indicator checkbox not found for:', indicatorValue);
                }
            });
        }
        isRestoringIndicators = false;
    };

    // Try to restore indicators immediately, or retry if DOM not ready
    restoreIndicators();

    // Live data is always enabled now

    // Load Agent Trades checkbox state
    if (typeof settings.showAgentTrades === 'boolean') {
        document.getElementById('showAgentTradesCheckbox').checked = settings.showAgentTrades;
    } else {
        document.getElementById('showAgentTradesCheckbox').checked = false;
    }

    // Load replay controls
    const replayFromInput = document.getElementById('replay-from');
    const replayToInput = document.getElementById('replay-to');
    const replaySpeedInput = document.getElementById('replay-speed');

    if (settings.replayFrom) replayFromInput.value = settings.replayFrom;
    else replayFromInput.value = '';
    if (settings.replayTo) replayToInput.value = settings.replayTo;
    else replayToInput.value = '';
    if (settings.replaySpeed) replaySpeedInput.value = settings.replaySpeed;
    else replaySpeedInput.value = '1';

    // Load Ollama settings
    if (typeof settings.useLocalOllama === 'boolean') {
        window.useLocalOllamaCheckbox.checked = settings.useLocalOllama;
    } else {
        window.useLocalOllamaCheckbox.checked = false;
    }
    window.useLocalOllamaCheckbox.dispatchEvent(new Event('change')); // Trigger UI update

    if (settings.localOllamaModelName) {
        setTimeout(() => { window.localOllamaModelSelect.value = settings.localOllamaModelName; }, 500); // Delay to allow dropdown to populate
    }

    // Load streamDeltaTime
    if (typeof settings.streamDeltaTime === 'number' && window.streamDeltaSlider && window.streamDeltaValueDisplay) {
        window.streamDeltaSlider.value = settings.streamDeltaTime;
        window.streamDeltaValueDisplay.textContent = settings.streamDeltaTime;
        window.currentStreamDeltaTime = settings.streamDeltaTime; // Update global state
    } else if (window.streamDeltaSlider && window.streamDeltaValueDisplay) { // Default if not in settings
        window.streamDeltaSlider.value = 0; // Default value
        window.streamDeltaValueDisplay.textContent = '0';
        window.currentStreamDeltaTime = 0; // Update global state
    }

    // Load min value filter
    if (typeof settings.minValueFilter === 'number' && window.minValueSlider && window.minValueDisplay) {
        window.minValueSlider.value = settings.minValueFilter;
        updateMinValueDisplay(); // Use percentage display function
    } else if (window.minValueSlider && window.minValueDisplay) { // Default if not in settings
        window.minValueSlider.value = 0; // Default value - show all trades
        updateMinValueDisplay(); // Use percentage display function
    }

    // Load trade visualization settings
    if (typeof settings.enableTradeTrace === 'boolean' && window.enableTradeTraceCheckbox) {
        window.enableTradeTraceCheckbox.checked = settings.enableTradeTrace;
    }
    if (typeof settings.enableMyTrades === 'boolean' && window.enableMyTradesCheckbox) {
        window.enableMyTradesCheckbox.checked = settings.enableMyTrades;
    }
    if (settings.last_selected_symbol) {
        const currentUrlSymbol = window.location.pathname.substring(1).toUpperCase() || null;
        // Only apply the saved symbol if it's different from the current URL symbol
        // This prevents overriding the symbol specified in the URL
        if (currentUrlSymbol && currentUrlSymbol !== settings.last_selected_symbol) {
            // URL symbol takes precedence, so don't override it with saved setting
        } else {
            window.isProgrammaticallySettingSymbol = true;
            window.symbolSelect.value = settings.last_selected_symbol;
            window.isProgrammaticallySettingSymbol = false;
        }
    }

    // Apply loaded ranges to the chart if it exists
    if (window.gd && (window.currentXAxisRange || window.currentYAxisRange)) {
        const layoutUpdate = {};

        if (window.currentXAxisRange) {
            layoutUpdate['xaxis.range'] = [new Date(window.currentXAxisRange[0]), new Date(window.currentXAxisRange[1])];
            layoutUpdate['xaxis.autorange'] = false;
        }

        if (window.currentYAxisRange) {
            layoutUpdate['yaxis.range'] = window.currentYAxisRange;
            layoutUpdate['yaxis.autorange'] = false;
        }

        if (Object.keys(layoutUpdate).length > 0) {
            Plotly.relayout(window.gd, layoutUpdate).then(() => {
            }).catch(error => {
                console.error('[DEBUG settingsManager] Error applying loaded ranges to chart:', error);
            });
        }
    }

    isRestoringIndicators = false; // Ensure indicator restoration flag is also reset

    // WebSocket will be initialized by main.js after all initialization is complete

    // Set up message handler for settings response
    window.wsAPI.onMessage('response', handleSettingsResponse);
}

// Apply debouncing to saveSettings to prevent rapid calls from spamming the API
async function saveSettingsInner() {
    // Prevent saving settings while they're still being loaded
    if (isLoadingSettings) {
        return;
    }

    // Prevent saving settings while indicators are still being restored
    if (isRestoringIndicators) {
        return;
    }

    // Check if WebSocket API is available
    if (!window.wsAPI) {
        console.warn("saveSettings: WebSocket API not available, skipping save");
        return;
    }

    const currentSymbol = window.symbolSelect.value;
    if (!currentSymbol) {
        console.warn("saveSettings: No symbol selected. Cannot save settings.");
        return;
    }
    const settings = {
        symbol: currentSymbol,
        resolution: window.resolutionSelect.value || '1h',
        range: window.rangeSelect.value || '24h',
        // Initialize all expected fields with proper defaults to ensure server receives complete config
        xAxisMin: null,
        xAxisMax: null,
        yAxisMin: null,
        yAxisMax: null,
        replayFrom: '',
        replayTo: '',
        replaySpeed: '1',
        useLocalOllama: false,
        localOllamaModelName: '',
        active_indicators: [],
        liveDataEnabled: true,
        showAgentTrades: false,
        streamDeltaTime: 1,
        minValueFilter: 0,
        enableTradeTrace: true
    };

    // Update X-axis range fields
    if (window.currentXAxisRange) {
        settings.xAxisMin = window.currentXAxisRange[0];
        settings.xAxisMax = window.currentXAxisRange[1];
    }
    
    // Update Y-axis range fields
    if (window.currentYAxisRange) {
        settings.yAxisMin = window.currentYAxisRange[0];
        settings.yAxisMax = window.currentYAxisRange[1];
    }

    // Update replay settings
    settings.replayFrom = document.getElementById('replay-from')?.value || '';
    settings.replayTo = document.getElementById('replay-to')?.value || '';
    settings.replaySpeed = document.getElementById('replay-speed')?.value || '1';
    
    // Update AI settings
    settings.useLocalOllama = document.getElementById('use-local-ollama-checkbox')?.checked || false;
    settings.localOllamaModelName = document.getElementById('local-ollama-model-select')?.value || '';

    // Update active indicators
    const indicatorCheckboxes = document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]');
    const active_indicators = [];
    indicatorCheckboxes.forEach(checkbox => {
        if (checkbox.checked) {
            active_indicators.push(checkbox.value);
        }
    });
    settings.active_indicators = active_indicators;
    
    // These fields are already set with defaults above, just need to update from UI
    settings.liveDataEnabled = true; // Always enabled now
    settings.showAgentTrades = document.getElementById('showAgentTradesCheckbox')?.checked || false;

    // Update stream delta time from slider (already has default above)
    if (window.streamDeltaSlider) {
        settings.streamDeltaTime = parseInt(window.streamDeltaSlider.value, 10) || 1;
    }

    // Include the last selected symbol in settings
    settings.last_selected_symbol = currentSymbol;

    // Update min value filter setting (already has default above)
    if (window.minValueSlider && window.minValueSlider.value !== undefined) {
        settings.minValueFilter = parseFloat(window.minValueSlider.value) || 0;
    }

    // Update trade visualization settings (already have defaults above)
    if (window.enableTradeTraceCheckbox) {
        settings.enableTradeTrace = window.enableTradeTraceCheckbox.checked;
    }
    if (window.enableMyTradesCheckbox) {
        settings.enableMyTrades = window.enableMyTradesCheckbox.checked;
    }

    // Include user email for authentication - use from global state set by init_success
    settings.email = window.userEmail || '';

    // Send config message via WebSocket (assumes WebSocket is already connected)
    try {
        console.log(JSON.stringify(settings, null, "\t"));
        window.wsAPI.sendMessage({
            type: 'config',
            action: 'set_settings',
            data: settings
        });
    } catch (error) {
        console.error("saveSettings: Error sending config message:", error);
    }

}

// Make saveSettingsInner available globally for immediate saves
window.saveSettingsInner = saveSettingsInner;

// Debounced version of saveSettings (500ms delay)
const saveSettings = debounce(() => {
    // Only save settings if user is authenticated
    /*
    if (!window.userAuthenticated) {
        console.warn("saveSettings: User not authenticated, skipping save");
        return;
    }
    */
    saveSettingsInner();
}, 500);
