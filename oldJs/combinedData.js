// Combined data WebSocket handler for historical OHLC + indicators + live data

// WebSocket connection now uses window.wsAPI - no local connection variables needed
let combinedSymbol = '';
let combinedIndicators = [];
let combinedResolution = '1h';
let combinedFromTs = null;
let combinedToTs = null;

// Store CTO Line traces globally for quick restoration when re-enabled
let storedCTOTraces = null;

// Store buy signals by timestamp for enhanced hover info on candlesticks
let buySignalsByTimestamp = {};

// Global traces array for chart management
let globalTraces = [];

function handleVolumeProfileMessage(message) {

    // Validate message format - handle nested volume_profile structure
    if (!message.data || !message.data.volume_profile) {
        console.warn('💹 Combined WebSocket: Invalid volume profile data format - message.data.volume_profile missing');
        return;
    }

    // Extract the volume profile array from the nested structure
    let volumeProfileArray = message.data.volume_profile;

    // If it's an object with a volume_profile property, get the array
    if (typeof volumeProfileArray === 'object' && !Array.isArray(volumeProfileArray) && volumeProfileArray.volume_profile) {
        volumeProfileArray = volumeProfileArray.volume_profile;
    }

    // Now validate that we have an array
    if (!Array.isArray(volumeProfileArray)) {
        console.warn('💹 Combined WebSocket: Invalid volume profile data format - volume_profile is not an array');
        return;
    }

// Make function globally available for main.js
window.handleVolumeProfileMessage = handleVolumeProfileMessage;

    // Process empty volume profile arrays - this might indicate clearing/resetting
    if (volumeProfileArray.length === 0) {
        // Still call the update function with empty array to allow clearing logic in tradeHistory.js
        if (window.updateVolumeProfileFromWebSocket) {
            window.updateVolumeProfileFromWebSocket([], message.data.symbol || 'BTCUSDT', message.data.rectangle_id);
        } else {
            console.warn('💹 Combined WebSocket: updateVolumeProfileFromWebSocket function not available from tradeHistory.js');
        }
        return;
    }

    // Process non-empty volume profile data within rectangle bounds
    renderVolumeProfileWithinRectangle(volumeProfileArray, message.data.rectangle_id);
}

// Render volume profile bars within a rectangle's bounds
function renderVolumeProfileWithinRectangle(volumeProfileData, rectangleId) {

    if (!volumeProfileData || !Array.isArray(volumeProfileData) || volumeProfileData.length === 0) {
        console.warn(`🚑 Combined WebSocket: No volume profile data to render.`);
        return;
    }

    // Check if chart is ready
    const gd = document.getElementById('chart');
    if (!gd || !gd.layout || !gd.layout.shapes) {
        console.warn('🚑 Combined WebSocket: Chart not ready for rectangle volume profile');
        return;
    }

    // Find the rectangle shape by ID
    const targetRectangle = gd.layout.shapes.find(shape =>
        shape.id === rectangleId &&
        (shape.type === 'rect' || shape.type === 'rectangle' || shape.type === 'box')
    );

    if (!targetRectangle) {
        console.warn(`🚑 Combined WebSocket: Rectangle with ID "${rectangleId}" not found in chart shapes. Available shapes:`, gd.layout.shapes.map(s => ({ id: s.id, type: s.type })));
        return;
    }



    // Extract rectangle bounds (time and price ranges)
    const timeRange = [
        (() => {
            if (targetRectangle.x0 instanceof Date) {
                return targetRectangle.x0.getTime() / 1000;
            } else if (typeof targetRectangle.x0 === 'string') {
                // Handle ISO date strings
                return new Date(targetRectangle.x0).getTime() / 1000;
            } else {
                // Assume it's already a timestamp in seconds
                return targetRectangle.x0;
            }
        })(),
        (() => {
            if (targetRectangle.x1 instanceof Date) {
                return targetRectangle.x1.getTime() / 1000;
            } else if (typeof targetRectangle.x1 === 'string') {
                // Handle ISO date strings
                return new Date(targetRectangle.x1).getTime() / 1000;
            } else {
                // Assume it's already a timestamp in seconds
                return targetRectangle.x1;
            }
        })()
    ];

    const priceRange = [
        Math.min(targetRectangle.y0, targetRectangle.y1),
        Math.max(targetRectangle.y0, targetRectangle.y1)
    ];

    // Safely format time range for logging
    const formatTimeForLog = (timestamp) => {
        try {
            // Check if timestamp is valid (not NaN, not null, reasonable range)
            if (typeof timestamp !== 'number' || isNaN(timestamp) || timestamp <= 0 || timestamp > 2147483647) {
                return `INVALID(${timestamp})`;
            }
            const date = new Date(timestamp * 1000);
            // Additional check for invalid Date object
            if (isNaN(date.getTime())) {
                return `INVALID_DATE(${timestamp})`;
            }
            return date.toISOString();
        } catch (e) {
            return `ERROR(${timestamp}, ${e.message})`;
        }
    };


    // Create volume profile bars positioned within the rectangle
    const volumeProfileTraces = createRectangleVolumeProfileBars(volumeProfileData, timeRange, priceRange, rectangleId);

    if (!volumeProfileTraces || volumeProfileTraces.length === 0) {
        console.warn(`🚑 Combined WebSocket: Failed to create volume profile bars for rectangle ${rectangleId}`);
        return;
    }

    // Remove existing volume profile traces for this rectangle
    const filteredData = gd.data.filter(trace =>
        !trace.name || !trace.name.includes(`VP-${rectangleId}`)
    );

    // Add new volume profile traces
    filteredData.push(...volumeProfileTraces);


    // Debug: Check data before Plotly.react

    // Update the chart with new volume profile traces
    Plotly.react(gd, filteredData, gd.layout, window.config).then(() => {

        // Verify traces are in chart after update
        if (window.gd && window.gd.data) {
            const vpTraces = window.gd.data.filter(t => t.name && t.name.includes(`VP-${rectangleId}`));
            vpTraces.forEach((trace, index) => {
            });
        }
    }).catch((error) => {
        console.error(`🚑 Combined WebSocket: Error rendering volume profile:`, error);
    });
}

function handleTradingSessionsMessage(message) {

    if (!message.data || !Array.isArray(message.data)) {
        console.warn('📈 Combined WebSocket: Invalid trading sessions data format');
        return;
    }

    if (message.data.length === 0) {
        return;
    }


    // Process and display trading sessions as visual elements on the chart
    visualizeTradingSessions(message.data, message.symbol);

}

function visualizeTradingSessions(sessions, symbol) {

    const gd = document.getElementById('chart');
    if (!gd || !gd.layout) {
        console.warn('📈 Chart not ready for trading sessions visualization');
        return;
    }

    // Define color schemes for different session types
    const sessionColors = {
        asian: {
            border: 'rgba(255, 165, 0, 0.9)',
            emoji: '🌅',
            description: 'Asian Session (Tokyo/Singapore)'
        },
        european: {
            border: 'rgba(0, 128, 255, 0.9)',
            emoji: '🇪🇺',
            description: 'European Session (London/Frankfurt)'
        },
        american: {
            border: 'rgba(255, 0, 0, 0.9)',
            emoji: '🇺🇸',
            description: 'American Session (New York)'
        },
        weekend: {
            border: 'rgba(128, 128, 128, 0.9)',
            emoji: '🎮',
            description: 'Weekend Session (24/7 Trading)'
        }
    };

            // Collect session labels for X-axis
            const sessionLabels = [];

            // Update X-axis tick format for better time display
            if (!gd.layout.xaxis) {
                gd.layout.xaxis = {};
            }

            // Use a more complete tick format that includes date
            const updatedTickFormat = gd.layout.xaxis.tickformat || '%Y-%m-%d %H:%M';

            sessions.forEach((session, index) => {
        try {
            // Validate session data
            if (!session.start_time || !session.end_time || !session.activity_type) {
                console.warn(`📈 Skipping session ${index} - missing required fields (start_time, end_time, activity_type)`);
                return;
            }

            const startTime = new Date(session.start_time * 1000);
            const endTime = new Date(session.end_time * 1000);
            const sessionType = session.activity_type.toLowerCase();

            // Validate dates
            if (isNaN(startTime.getTime()) || isNaN(endTime.getTime())) {
                console.warn(`📈 Skipping session ${index} - invalid timestamps:`, session.start_time, session.end_time);
                return;
            }

            // Skip sessions that are too short (less than 5 minutes)
            const sessionDurationHours = (endTime - startTime) / (1000 * 60 * 60);
            if (sessionDurationHours < (5/60)) { // Less than 5 minutes
                return;
            }

            // Get color scheme for this session type
            const colorScheme = sessionColors[sessionType] || {
                border: 'rgba(128, 128, 128, 0.9)',
                emoji: '❓',
                description: 'Unknown Session'
            };

            // Add session label to X-axis
            sessionLabels.push({
                time: startTime.getTime(),
                label: `${colorScheme.emoji} ${sessionType.toUpperCase()}`,
                color: colorScheme.border
            });

        } catch (error) {
            console.error(`📈 Error processing trading session ${index}:`, error, session);
        }
    });

    // Update X-axis with session labels
    if (!gd.layout.xaxis) {
        gd.layout.xaxis = {};
    }

    // Custom tick formatting to show session labels
    const currentTickFormat = updatedTickFormat;
    const currentTickMode = gd.layout.xaxis.tickmode || 'auto';

    // Create enhanced X-axis configuration
    const xAxisConfig = {
        ...gd.layout.xaxis,
        tickmode: currentTickMode,
        tickformat: currentTickFormat,
        // Add session labels as annotations positioned on the X-axis
        layer: 'above'
    };

    // Ensure shapes array exists
    if (!gd.layout.shapes) {
        gd.layout.shapes = [];
    }

    // Ensure annotations array exists
    if (!gd.layout.annotations) {
        gd.layout.annotations = [];
    }

    // Remove existing trading session shapes to avoid duplicates
    gd.layout.shapes = gd.layout.shapes.filter(shape =>
        !shape.name || !shape.name.startsWith('trading_session_')
    );

    // Remove existing trading session annotations to avoid duplicates
    gd.layout.annotations = gd.layout.annotations.filter(annotation =>
        !annotation.name || !annotation.name.startsWith('session_label_')
    );

    let asianSessionCount = 0;
    let europeanSessionCount = 0;
    let americanSessionCount = 0;
    let weekendSessionCount = 0;

    sessions.forEach((session, index) => {
        try {
            // Validate session data
            if (!session.start_time || !session.end_time || !session.activity_type) {
                console.warn(`📈 Skipping session ${index} - missing required fields (start_time, end_time, activity_type)`);
                return;
            }

            const startTime = new Date(session.start_time * 1000);
            const endTime = new Date(session.end_time * 1000);
            const sessionType = session.activity_type.toLowerCase();

            // Validate dates
            if (isNaN(startTime.getTime()) || isNaN(endTime.getTime())) {
                console.warn(`📈 Skipping session ${index} - invalid timestamps:`, session.start_time, session.end_time);
                return;
            }

            // Skip sessions that are too short (less than 5 minutes)
            const sessionDurationHours = (endTime - startTime) / (1000 * 60 * 60);
            if (sessionDurationHours < (5/60)) { // Less than 5 minutes
                return;
            }

            // Get color scheme for this session type
            const colorScheme = sessionColors[sessionType] || {
                background: 'rgba(128, 128, 128, 0.12)',
                border: 'rgba(128, 128, 128, 0.5)',
                emoji: '❓',
                description: 'Unknown Session'
            };

            // Position shapes to fill gaps between sessions - CALCULATE CONTINUOUS COVERAGE
            let sessionName = session.session_name || sessionType.toUpperCase();
            let y0, y1, yLabelPos;

            // All session types now use the same height level to fill gaps
            // This ensures no gaps between consecutive sessions
            // Position below the X-axis in the lowest subplot (price chart)
            y0 = -0.08;   // Below the bottom edge of the price chart
            y1 = 0;  // Extend to the bottom edge (X-axis level)
            yLabelPos = -0.04; // Label position in center of the rectangle below the chart

            // Create single session background rectangle - FIXED TO NOT BE MOVABLE
            const sessionShape = {
                name: `trading_session_${sessionType}_${index}`,
                type: 'rect',
                xref: 'x',
                yref: 'paper',
                x0: startTime,
                x1: endTime,
                y0: y0,
                y1: y1,
                fillcolor: colorScheme.background,
                line: {
                    color: colorScheme.border,
                    width: 1,
                    dash: 'solid'
                },
                layer: 'below', // Background layer
                visible: true,
                editable: false, // EXPLICITLY NON-MOVABLE
                hovertemplate: `
                    <b>${colorScheme.emoji} ${sessionName}</b><br>
                    <b>Description:</b> ${colorScheme.description}<br>
                    <b>Symbol:</b> ${symbol}<br>
                    <b>Time (GMT):</b> ${startTime.toLocaleString()} - ${endTime.toLocaleString()}<br>
                    <b>Duration:</b> ${sessionDurationHours.toFixed(2)} hours<br>
                    <b>GMT Range:</b> ${session.gmt_range || 'N/A'}<br>
                    <b>Volatility:</b> ${session.volatility || 'N/A'}<br>
                    <b>Characteristics:</b> ${session.characteristics || 'N/A'}<br>
                    <b>Local Time:</b> ${session.local_description || 'N/A'}<br>
                    <extra></extra>
                `
            };

            gd.layout.shapes.push(sessionShape);

            // Create FIXED POSITION session label annotation with emoji + name
            if (!gd.layout.annotations) {
                gd.layout.annotations = [];
            }

            // Position annotation in center of each session bar with emoji + name
            const annotation = {
                name: `session_label_${sessionType}_${index}`,
                x: startTime.getTime() + ((endTime.getTime() - startTime.getTime()) / 2), // Center of session
                y: yLabelPos, // Position varies by session type, below x-axis
                xref: 'x',
                yref: 'paper',
                text: `${colorScheme.emoji} ${sessionName}`,
                showarrow: false,
                xanchor: 'center',
                yanchor: 'middle',
                font: {
                    family: 'Arial, sans-serif',
                    size: 9, // Smaller for below-chart positioning
                    color: colorScheme.border, // Use session color for visibility
                    weight: 'bold'
                },
                bgcolor: 'rgba(255, 255, 255, 0.95)', // Light background for contrast against dark bars
                bordercolor: colorScheme.border,
                borderwidth: 1,
                borderpad: 2,
                hovertext: `${colorScheme.emoji} ${sessionName}
Time: ${startTime.toLocaleString()} - ${endTime.toLocaleString()}
GMT: ${session.gmt_range || 'N/A'}
Volume: ${session.volatility || 'N/A'}
${session.characteristics || ''}`,
                hoverlabel: {
                    bgcolor: 'rgba(0, 0, 0, 0.9)',
                    bordercolor: colorScheme.border,
                    font: { color: 'white', size: 11 }
                }
            };

            gd.layout.annotations.push(annotation);

            // Count sessions by type for logging
            switch (sessionType) {
                case 'asian':
                    asianSessionCount++;
                    break;
                case 'european':
                    europeanSessionCount++;
                    break;
                case 'american':
                    americanSessionCount++;
                    break;
                case 'weekend':
                    weekendSessionCount++;
                    break;
                default:
            }

        } catch (error) {
            console.error(`📈 Error processing trading session ${index}:`, error, session);
        }
    });

    // Update the chart with session labels on X-axis
    try {
        Plotly.relayout(gd, {
            xaxis: xAxisConfig,
            annotations: gd.layout.annotations
        });

    } catch (error) {
        console.error('📈 Error updating chart with session labels:', error);
    }
}


function handleShapeVolumeProfilesMessage(message) {

    if (!message.data || typeof message.data !== 'object') {
        console.warn('💹 Combined WebSocket: Invalid shape volume profiles data format');
        return;
    }

    const drawingIds = Object.keys(message.data);
    if (drawingIds.length === 0) {
        return;
    }


    // Check if chart is ready
    if (!window.gd || !window.gd.layout) {
        console.warn('💹 Combined WebSocket: Chart not ready for shape volume profiles');
        return;
    }

    // Remove existing shape volume profile traces
    const filteredData = window.gd.data.filter(trace =>
        !trace.name || !trace.name.startsWith('Shape Volume:')
    );

    // Process each shape's volume profile and create visualization
    const shapeVolumeTraces = [];
    const shapeColors = [
        'rgba(255, 0, 0, 0.8)',   // Red
        'rgba(0, 255, 0, 0.8)',   // Green
        'rgba(0, 0, 255, 0.8)',   // Blue
        'rgba(255, 255, 0, 0.8)', // Yellow
        'rgba(255, 0, 255, 0.8)', // Magenta
        'rgba(0, 255, 255, 0.8)', // Cyan
        'rgba(255, 165, 0, 0.8)', // Orange
        'rgba(128, 0, 128, 0.8)'  // Purple
    ];

    drawingIds.forEach((drawingId, index) => {
        const shapeData = message.data[drawingId];


        if (!shapeData.volume_profile || !shapeData.volume_profile.volume_profile ||
            !Array.isArray(shapeData.volume_profile.volume_profile)) {
            console.warn(`   No valid volume profile data found for shape ${drawingId}`);
            return;
        }

        const volumeData = shapeData.volume_profile.volume_profile;
        if (volumeData.length === 0) {
            console.warn(`   Empty volume profile for shape ${drawingId}`);
            return;
        }

        // Create horizontal bars for this shape
        const shapeTraces = createShapeVolumeProfileBars(
            volumeData,
            shapeData.time_range,
            shapeData.price_range,
            drawingId,
            shapeData.shape_type,
            shapeColors[index % shapeColors.length]
        );

        if (shapeTraces && shapeTraces.length > 0) {
            shapeVolumeTraces.push(...shapeTraces);
        } else {
            console.warn(`   Failed to create volume bars for shape ${drawingId}`);
        }
    });

    // Add shape volume profile traces to chart
    if (shapeVolumeTraces.length > 0) {
        filteredData.push(...shapeVolumeTraces);

        Plotly.react(window.gd, filteredData, window.gd.layout, window.config);
    } else {
    }

}

// Create horizontal volume profile bars for a rectangle
function createRectangleVolumeProfileBars(volumeProfileData, timeRange, priceRange, rectangleId) {

    if (!volumeProfileData || !Array.isArray(volumeProfileData) || volumeProfileData.length === 0) {
        console.warn(`🚑 No volume profile data for rectangle ${rectangleId}`);
        return null;
    }

    if (!timeRange || !Array.isArray(timeRange) || timeRange.length !== 2) {
        console.warn(`🚑 Invalid time range for rectangle ${rectangleId}:`, timeRange);
        return null;
    }

    if (!priceRange || !Array.isArray(priceRange) || priceRange.length !== 2) {
        console.warn(`🚑 Invalid price range for rectangle ${rectangleId}:`, priceRange);
        return null;
    }

    const [startTime, endTime] = timeRange;
    const [lowPrice, highPrice] = priceRange;

    // Validate ranges
    if (startTime >= endTime || lowPrice >= highPrice) {
        console.warn(`🚑 Invalid ranges for rectangle ${rectangleId}: time ${startTime}-${endTime}, price ${lowPrice}-${highPrice}`);
        return null;
    }

    const startDate = new Date(startTime * 1000);
    const endDate = new Date(endTime * 1000);
    const timeSpan = endDate - startDate;


    // DEBUG: Log rectangle bounds and volume data before filtering

    if (volumeProfileData.length > 0) {
        // Sample first 5 and last 5 levels
        const sampleLevels = volumeProfileData.slice(0, 5);
        if (volumeProfileData.length > 10) {
            sampleLevels.push('...', `... (${volumeProfileData.length - 10} more levels) ...`);
            sampleLevels.push(...volumeProfileData.slice(-5));
        }

        sampleLevels.forEach((level, index) => {
            if (level !== '...') {
                const price = typeof level.price === 'string' ? parseFloat(level.price) : level.price;
            } else {
            }
        });

        // Get price statistics
        const prices = volumeProfileData.map(level => typeof level.price === 'string' ? parseFloat(level.price) : level.price);
        const validPrices = prices.filter(p => !isNaN(p));
        if (validPrices.length > 0) {
            const minPrice = Math.min(...validPrices);
            const maxPrice = Math.max(...validPrices);
            const avgPrice = validPrices.reduce((a, b) => a + b, 0) / validPrices.length;
        }
    }

    // Filter volume profile data to be within the rectangle's price range
    let filteredCount = 0;
    let keptCount = 0;
    let errorCount = 0;

    console.log(`🔍 Filtering volume profile data for rectangle ${rectangleId}:`);
    console.log(`   Rectangle price range: [${lowPrice.toFixed(2)}, ${highPrice.toFixed(2)}]`);
    console.log(`   Volume profile data points: ${volumeProfileData.length}`);

    const relevantVolumeData = volumeProfileData.filter(level => {
        // Handle invalid level data
        if (!level || typeof level.price === 'undefined' || level.price === null) {
            errorCount++;
            return false;
        }

        const price = typeof level.price === 'string' ? parseFloat(level.price) : level.price;

        // Check for NaN result from parsing
        if (isNaN(price)) {
            errorCount++;
            return false;
        }

        const isInRange = price >= lowPrice && price <= highPrice;

        if (isInRange) {
            keptCount++;
            console.log(`   ✅ KEPT: ${price.toFixed(2)} (in range [${lowPrice.toFixed(2)}, ${highPrice.toFixed(2)}])`);
        } else {
            filteredCount++;
            console.log(`   ❌ FILTERED: ${price.toFixed(2)} (out of range [${lowPrice.toFixed(2)}, ${highPrice.toFixed(2)}])`);
        }

        return isInRange;
    });

    console.log(`🔍 Filtering results for rectangle ${rectangleId}:`);
    console.log(`   Total data points: ${volumeProfileData.length}`);
    console.log(`   Kept: ${keptCount}, Filtered: ${filteredCount}, Errors: ${errorCount}`);
    console.log(`   Relevant data points: ${relevantVolumeData.length}`);


    if (relevantVolumeData.length > 0) {
        const keptPrices = relevantVolumeData.map(level => typeof level.price === 'string' ? parseFloat(level.price) : level.price);
        const minKept = Math.min(...keptPrices);
        const maxKept = Math.max(...keptPrices);
    } else {
    }


    if (relevantVolumeData.length === 0) {
        console.warn(`🚑 No volume data within rectangle ${rectangleId} price range`);
        return null;
    }

    // Find max volume for scaling within this rectangle
    const maxVolumeInRectangle = Math.max(...relevantVolumeData.map(level => Math.max(
        level.totalVolume || 0,
        level.buyVolume || 0,
        level.sellVolume || 0
    )));

    if (maxVolumeInRectangle === 0) {
        console.warn(`🚑 No volume data for rectangle ${rectangleId}`);
        return null;
    }

    // Calculate bar thickness based on bar count and price range to prevent overlapping
    // Sort prices to find gaps between consecutive levels
    const sortedPrices = relevantVolumeData.map(level => typeof level.price === 'string' ? parseFloat(level.price) : level.price).sort((a, b) => a - b);

    // Find the minimum gap between consecutive price levels
    let minGap = Infinity;
    for (let i = 1; i < sortedPrices.length; i++) {
        const gap = sortedPrices[i] - sortedPrices[i - 1];
        if (gap > 0 && gap < minGap) {
            minGap = gap;
        }
    }

    // Fallback if minGap is still Infinity (shouldn't happen with valid data)
    if (!isFinite(minGap) || minGap === 0) {
        const barCount = relevantVolumeData.length;
        const priceRangeSpan = highPrice - lowPrice;
        minGap = priceRangeSpan / Math.max(barCount, 1);
    }

    // Set bar thickness to be less than the minimum gap to prevent overlap
    // Use 80% of the minimum gap to ensure clear separation
    let barThickness = minGap * 0.8;

    // Ensure bar thickness is reasonable (not too thin, not too thick)
    const priceRangeSpan = highPrice - lowPrice;
    const minThickness = priceRangeSpan * 0.001; // Minimum 0.1% of price range
    const maxThickness = priceRangeSpan * 0.05;  // Maximum 5% of price range

    barThickness = Math.max(minThickness, Math.min(maxThickness, barThickness));

    // Create traces for the volume profile bars
    const traces = [];

    // Validate and create timestamp objects with error handling
    let rectangleStartTime = null;
    let timeCenter = null;

    try {
        if (isNaN(startTime) || startTime <= 0) {
            console.warn(`🚑 Invalid startTime: ${startTime}`);
            return null;
        }
        if (isNaN(endTime) || endTime <= 0 || endTime <= startTime) {
            console.warn(`🚑 Invalid endTime: ${endTime}, startTime: ${startTime}`);
            return null;
        }

        rectangleStartTime = new Date(startTime * 1000);
        timeCenter = new Date((startTime + endTime) / 2 * 1000);

        if (isNaN(rectangleStartTime.getTime()) || isNaN(timeCenter.getTime())) {
            console.warn(`🚑 Invalid timestamp conversion - startTime: ${startTime}, endTime: ${endTime}`);
            return null;
        }

    } catch (error) {
        console.error(`🚑 Error creating rectangle timestamps:`, error);
        return null;
    }

        // Sort by price for visual consistency
        relevantVolumeData.sort((a, b) => a.price - b.price);

        relevantVolumeData.forEach((level, index) => {
            const price = typeof level.price === 'string' ? parseFloat(level.price) : level.price;
            const totalVol = level.totalVolume || 0;
            const buyVol = level.buyVolume || 0;
            const sellVol = level.sellVolume || 0;

            // Use the calculated bar thickness based on bar count and price range
            const topPrice = barThickness;

            // Calculate bar lengths based on volume (same as original scaling)
            const maxBarLengthMs = timeSpan; // Full rectangle time span for maximum bar length
            const buyBarLengthMs = buyVol > 0 ? (buyVol / maxVolumeInRectangle) * maxBarLengthMs : 0;
            const sellBarLengthMs = sellVol > 0 ? (sellVol / maxVolumeInRectangle) * maxBarLengthMs : 0;

            // BUY BAR - positioned above price, extends from left edge based on volume
            if (buyVol > 0) {
                const buyBarEndTime = new Date(rectangleStartTime.getTime() + buyBarLengthMs);
            const buyBarTrace = {
                x: [rectangleStartTime, buyBarEndTime, buyBarEndTime, rectangleStartTime, rectangleStartTime], // Create closed rectangle
                y: [price, price, price + topPrice, price + topPrice, price], // Create closed rectangle
                type: 'scatter',
                fill: 'toself', // Fill the closed shape
                    fillcolor: 'rgba(219, 175, 31, 0.3)', // Increased transparency
                mode: 'lines', // Use only lines mode - no markers
                line: {
                    color: 'rgba(219, 175, 31, 0.3)', // Fully opaque
                    width: 1 // Thicker line
                },
                    name: `VP-${rectangleId} Buy: ${price.toFixed(2)}`,
                    hovertemplate:
                        `<b>Rectangle Volume Profile - BUYERS</b><br>` +
                        `Rectangle: ${rectangleId}<br>` +
                        `Price: $${price.toFixed(2)}<br>` +
                        `Buy Volume: ${buyVol.toFixed(4)}<br>` +
                        `Total Volume: ${totalVol.toFixed(4)}<br>` +
                        `Time Range: ${startDate.toLocaleString()} - ${endDate.toLocaleString()}<br>` +
                        `Bar Position: ${rectangleStartTime.toISOString()} to ${buyBarEndTime.toISOString()}<br>` +
                        `<extra></extra>`,
                    xaxis: 'x',
                    yaxis: 'y',
                    showlegend: false,
                    hoverlabel: {
                        bgcolor: 'rgba(0, 0, 0, 0.8)',
                        bordercolor: 'rgba(0, 255, 0, 1.0)',
                        font: { color: 'white', size: 12 }
                    }
                };

                traces.push(buyBarTrace);
            }



        });


    return traces;
}

// Create horizontal volume profile bars for a shape
function createShapeVolumeProfileBars(volumeData, timeRange, priceRange, drawingId, shapeType, shapeColor) {


    if (!volumeData || !Array.isArray(volumeData) || volumeData.length === 0) {
        console.warn(`No volume data for shape ${drawingId}`);
        return null;
    }

    if (!timeRange || !Array.isArray(timeRange) || timeRange.length !== 2) {
        console.warn(`Invalid time range for shape ${drawingId}:`, timeRange);
        return null;
    }

    if (!priceRange || !Array.isArray(priceRange) || priceRange.length !== 2) {
        console.warn(`Invalid price range for shape ${drawingId}:`, priceRange);
        return null;
    }

    const [startTime, endTime] = timeRange;
    const [lowPrice, highPrice] = priceRange;

    // Validate time and price ranges
    if (startTime >= endTime || lowPrice >= highPrice) {
        console.warn(`Invalid ranges for shape ${drawingId}: time ${startTime}-${endTime}, price ${lowPrice}-${highPrice}`);
        return null;
    }

    // Convert times to Date objects if they're not already
    const startDate = new Date(startTime * 1000);
    const endDate = new Date(endTime * 1000);
    const timeSpan = endDate - startDate;


    // Filter volume data to be within the shape's price range
    const relevantVolumeData = volumeData.filter(level => {
        const price = level.price;
        return price >= lowPrice && price <= highPrice;
    });


    if (relevantVolumeData.length === 0) {
        console.warn(`No volume data within shape ${drawingId} price range`);
        return null;
    }

    // Find max volume for scaling within this shape
    const maxVolumeInShape = Math.max(...relevantVolumeData.map(level => Math.max(
        level.totalVolume || 0,
        level.buyVolume || 0,
        level.sellVolume || 0
    )));

    if (maxVolumeInShape === 0) {
        console.warn(`No volume data for shape ${drawingId}`);
        return null;
    }


    // Create traces for the volume profile bars
    const traces = [];
    const timeCenter = new Date((startTime + endTime) / 2 * 1000);

    // Sort by price for visual consistency
    relevantVolumeData.sort((a, b) => a.price - b.price);

    relevantVolumeData.forEach((level, index) => {
        const price = level.price;
        const totalVol = level.totalVolume || 0;
        const buyVol = level.buyVolume || 0;
        const sellVol = level.sellVolume || 0;

        // Offset vertically by index to avoid overlapping on the same price level
        const verticalOffset = (index - relevantVolumeData.length / 2) * 0.001; // Small offset per bar
        const actualPrice = price + verticalOffset;

        // Calculate maximum bar length proportional to time span of shape
        const maxBarLengthHours = Math.min(1.0, timeSpan / (1000 * 60 * 60 * 4)); // Max 1 hour bars or 25% of shape width
        const maxBarLengthMs = maxBarLengthHours * 60 * 60 * 1000;

        // Create separate bars for buyers (right side) and sellers (left side)

        // BUY BAR - positioned to the RIGHT of center
        if (buyVol > 0) {
            const buyBarLengthMs = (buyVol / maxVolumeInShape) * maxBarLengthMs;
            const buyBarStartTime = timeCenter;
            const buyBarEndTime = new Date(timeCenter.getTime() + buyBarLengthMs);

            const buyBarTrace = {
                x: [buyBarStartTime, buyBarEndTime],
                y: [actualPrice, actualPrice],
                type: 'scatter',
                mode: 'lines',
                name: `Shape Buy: ${drawingId} @ ${price.toFixed(2)}`,
                line: {
                    color: 'rgba(0, 255, 0, 0.8)', // Green for buyers
                    width: Math.max(2, Math.min(8, (buyVol / maxVolumeInShape) * 6)) // Same thickness calculation
                },
                hovertemplate:
                    `<b>Shape Volume Profile - BUYERS</b><br>` +
                    `Shape: ${drawingId} (${shapeType})<br>` +
                    `Price: $${price.toFixed(2)}<br>` +
                    `Buy Volume: ${buyVol.toFixed(4)}<br>` +
                    `Total Volume: ${totalVol.toFixed(4)}<br>` +
                    `Time Range: ${startDate.toLocaleString()} - ${endDate.toLocaleString()}<br>` +
                    `<extra></extra>`,
                xaxis: 'x', // Main x-axis (time)
                yaxis: 'y', // Main price chart
                showlegend: false,
                hoverlabel: {
                    bgcolor: 'rgba(0, 0, 0, 0.8)',
                    bordercolor: 'rgba(0, 255, 0, 0.8)',
                    font: { color: 'white', size: 12 }
                }
            };

            traces.push(buyBarTrace);
        }

        // SELL BAR - positioned to the LEFT of center
        if (sellVol > 0) {
            const sellBarLengthMs = (sellVol / maxVolumeInShape) * maxBarLengthMs;
            const sellBarStartTime = new Date(timeCenter.getTime() - sellBarLengthMs);
            const sellBarEndTime = timeCenter;

            const sellBarTrace = {
                x: [sellBarStartTime, sellBarEndTime],
                y: [actualPrice, actualPrice],
                type: 'scatter',
                mode: 'lines',
                name: `Shape Sell: ${drawingId} @ ${price.toFixed(2)}`,
                line: {
                    color: 'rgba(255, 0, 0, 0.8)', // Red for sellers
                    width: Math.max(2, Math.min(8, (sellVol / maxVolumeInShape) * 6)) // Same thickness calculation
                },
                hovertemplate:
                    `<b>Shape Volume Profile - SELLERS</b><br>` +
                    `Shape: ${drawingId} (${shapeType})<br>` +
                    `Price: $${price.toFixed(2)}<br>` +
                    `Sell Volume: ${sellVol.toFixed(4)}<br>` +
                    `Total Volume: ${totalVol.toFixed(4)}<br>` +
                    `Time Range: ${startDate.toLocaleString()} - ${endDate.toLocaleString()}<br>` +
                    `<extra></extra>`,
                xaxis: 'x', // Main x-axis (time)
                yaxis: 'y', // Main price chart
                showlegend: false,
                hoverlabel: {
                    bgcolor: 'rgba(0, 0, 0, 0.8)',
                    bordercolor: 'rgba(255, 0, 0, 0.8)',
                    font: { color: 'white', size: 12 }
                }
            };

            traces.push(sellBarTrace);
        }

    });


    // Shape labels removed as requested

    return traces;
}


// Initialize timestamps with default values (30 days ago to now)
function initializeDefaultTimestamps() {
    if (combinedFromTs === null || combinedToTs === null) {
        const currentTime = new Date().getTime();
        combinedFromTs = Math.floor((currentTime - 30 * 86400 * 1000) / 1000); // 30 days ago in seconds
        combinedToTs = Math.floor(currentTime / 1000); // Now in seconds
    }
}

// Message queue and synchronization
let messageQueue = [];
let isProcessingMessage = false;
let chartUpdateLock = false;
let chartUpdateDebounceTimer = null;
const CHART_UPDATE_DEBOUNCE_DELAY = 100; // ms

// Live data queuing for performance
let queuedLiveUpdate = null;
let liveUpdateTimeout = null;

// WebSocket connection management
let websocketSetupDebounceTimer = null;
const WEBSOCKET_SETUP_DEBOUNCE_DELAY = 500; // ms
let isWebSocketConnecting = false;
let lastProcessedTimestampRange = null;
let processedMessageIds = new Set();
let websocketConnectionId = 0;

// WebSocket lifecycle logging
const websocketLogs = [];
const MAX_WEBSOCKET_LOGS = 50;

function logWebSocketEvent(event, details = {}) {
    const logEntry = {
        timestamp: new Date().toISOString(),
        event,
        connectionId: websocketConnectionId,
        details
    };
    websocketLogs.push(logEntry);
    if (websocketLogs.length > MAX_WEBSOCKET_LOGS) {
        websocketLogs.shift();
    }
}

// Historical data processing - direct handling without accumulation

// Real-time price line management functions
function updateOrAddRealtimePriceLine(gd, price, candleStartTimeMs, candleEndTimeMs, doRelayout = false) {
    // Make function globally available
    window.updateOrAddRealtimePriceLine = updateOrAddRealtimePriceLine;
    if (!gd || !gd.layout) {
        // console.warn("[PriceLine] Chart or layout not ready.");
        return;
    }

    // Set flag to prevent relayout event processing during price line updates
    if (!window.ignoreRelayoutEvents) {
        window.ignoreRelayoutEvents = false;
    }
    window.ignoreRelayoutEvents = true;

    // Ensure layout exists
    if (!gd.layout) {
        gd.layout = {};
    }

    let shapes = gd.layout.shapes || [];
    let annotations = gd.layout.annotations || [];
    // Ensure annotations is always an array
    if (!Array.isArray(annotations)) {
        annotations = [];
        gd.layout.annotations = annotations;
    }

    shapes = shapes.filter(shape => shape.name !== CROSSHAIR_VLINE_NAME); // Assumes CROSSHAIR_VLINE_NAME is global

    let lineIndex = shapes.findIndex(shape => shape.name === REALTIME_PRICE_LINE_NAME); // Assumes REALTIME_PRICE_LINE_NAME is global
    try {
        // Determine the correct y-axis reference for the price chart
        // When indicators are present, price chart is on yaxis1, otherwise on y
        const hasIndicators = window.gd && window.gd.data && window.gd.data.some(trace => trace.type !== 'candlestick');
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
                layer: 'below',
                editable: false // Explicitly make this system shape not editable by Plotly
        };

        if (typeof price !== 'number' || isNaN(price)) {
            // console.error("[PriceLine] Invalid price for annotation:", price, "(type:", typeof price, ")");
            // Optionally remove any existing price annotation if the new price is invalid
            gd.layout.annotations = gd.layout.annotations.filter(ann => ann.name !== REALTIME_PRICE_TEXT_ANNOTATION_NAME);
            if (doRelayout) Plotly.relayout(gd, { shapes: gd.layout.shapes, annotations: gd.layout.annotations });
            return;
        }

        // Calculate a small offset for positioning the label above the line
        // Use 10% of the current y-axis span for consistent positioning
        let labelOffset = 0;
        if (window.gd && window.gd.layout && window.gd.layout.yaxis && window.gd.layout.yaxis.range) {
            const yRange = window.gd.layout.yaxis.range;
            const ySpan = yRange[1] - yRange[0];
            labelOffset = 0.1 * ySpan; // 10% of y-axis span
        } else {
            // Fallback if y-axis range is not available
            labelOffset = 0.005 * price;
        }

        const annotationDefinition = {
            name: REALTIME_PRICE_TEXT_ANNOTATION_NAME, // From config.js
            text: price.toFixed(2), // Format the price, adjust precision as needed
            xref: 'paper',  // Relative to the entire plotting area
            yref: yref + labelOffset,     // Use the same y-axis reference as the line
            x: 1,           // Position more inside the chart area (95% from left)
            y: price,       // Position above the price line
            showarrow: false,
            xanchor: 'right', // Anchor the text from its right side
            yanchor: 'bottom', // Anchor the text from its bottom (places it above the line)
            font: {
                family: 'Arial, sans-serif',
                size: 10, // Reduced font size for smaller display
                color: 'black' // Solid black for better contrast
            },
            bgcolor: 'white', // Solid white background for SVG
            bordercolor: 'black', // Black border for visibility
            borderwidth: 2,  // Thicker border
            borderpad: 1     // More padding
        };

        if (lineIndex !== -1) {
            shapes[lineIndex] = lineDefinition;
        } else {
            shapes.push(lineDefinition);
        }

       // Remove old annotation before adding new one to prevent duplicates
       const existingAnnotationIndex = annotations.findIndex(ann => ann.name === REALTIME_PRICE_TEXT_ANNOTATION_NAME);
       if (existingAnnotationIndex !== -1) {
           annotations.splice(existingAnnotationIndex, 1);
       }
       annotations.push(annotationDefinition);
       // Update the layout with the modified annotations
       gd.layout.annotations = annotations;


        gd.layout.shapes = shapes;
        // gd.layout.annotations is already updated by reference


    } catch (e) {
        // console.error("[PriceLine] Error during shape modification:", e);
    }

    if (doRelayout) {
        Plotly.relayout(gd, { shapes: gd.layout.shapes, annotations: gd.layout.annotations }).then(() => {
            // Clear the flag after Plotly operation completes
            window.ignoreRelayoutEvents = false;
        }).catch(() => {
            // Clear the flag even if operation fails
            window.ignoreRelayoutEvents = false;
        });
    } else {
        // For live price updates, use Plotly.update to refresh shapes/annotations without triggering relayout events
        Plotly.update(gd, {}, { shapes: gd.layout.shapes, annotations: gd.layout.annotations }).then(() => {
            // Clear the flag after Plotly operation completes
            window.ignoreRelayoutEvents = false;
        }).catch(() => {
            // Clear the flag even if operation fails
            window.ignoreRelayoutEvents = false;
        });
    }
}

function removeRealtimePriceLine(gd, doRelayout = false) {
    // Make function globally available
    window.removeRealtimePriceLine = removeRealtimePriceLine;

    // Ensure layout exists
    if (!gd) {
        // console.warn("[PriceLine] removeRealtimePriceLine: Chart not available.");
        return false;
    }
    if (!gd.layout) {
        gd.layout = {};
    }
    if (!gd.layout.shapes) {
        gd.layout.shapes = [];
    }

    const initialLength = gd.layout.shapes.length;
    let annotationsChanged = false;

    // Ensure annotations is an array
    if (!gd.layout.annotations) {
        gd.layout.annotations = [];
    } else if (!Array.isArray(gd.layout.annotations)) {
        gd.layout.annotations = [];
    }

    if (gd.layout.annotations.length > 0) {
        const initialAnnotationLength = gd.layout.annotations.length;
        gd.layout.annotations = gd.layout.annotations.filter(ann => ann.name !== REALTIME_PRICE_TEXT_ANNOTATION_NAME);
        annotationsChanged = gd.layout.annotations.length < initialAnnotationLength;
    }

    gd.layout.shapes = gd.layout.shapes.filter(shape => shape.name !== REALTIME_PRICE_LINE_NAME && shape.name !== CROSSHAIR_VLINE_NAME);
    const removed = gd.layout.shapes.length < initialLength;

    if ((removed || annotationsChanged) && doRelayout) {
        Plotly.relayout(gd, { shapes: gd.layout.shapes, annotations: gd.layout.annotations });
    }
    return removed;
}

// Message queue processing functions
function enqueueMessage(message) {
    // Generate message ID for deduplication with better uniqueness
    let messageId;

    if (message.type === 'live_price') {
        // For live_price messages, include price and timestamp for uniqueness
        const price = message.price || (message.data && message.data.price) || 'unknown';
        const timestamp = message.timestamp || (message.data && message.data.timestamp) || Date.now();
        messageId = `${message.type}_${message.symbol || 'unknown'}_${price}_${timestamp}`;
    } else if (message.type === 'live') {
        // For live data messages, include timestamp and price
        const timestamp = message.data && message.data.time ? message.data.time : Date.now();
        const price = message.data && message.data.ohlc ? message.data.ohlc.close : 'unknown';
        messageId = `${message.type}_${message.symbol || 'unknown'}_${timestamp}_${price}`;
    } else if (message.type === 'historical') {
        // For historical data, include symbol and time range
        const symbol = message.symbol || 'unknown';
        const fromTs = message.data && message.data[0] ? message.data[0].time : 'unknown';
        const toTs = message.data && message.data.length > 0 ? message.data[message.data.length - 1].time : 'unknown';
        messageId = `${message.type}_${symbol}_${fromTs}_${toTs}`;
    } else if (message.type === 'volume_profile') {
        // For volume profile messages, include rectangle ID
        messageId = `${message.type}_${message.symbol || 'unknown'}_${message.rectangle_id || 'unknown'}`;
    } else if (message.type === 'trading_sessions') {
        // For trading sessions, include date and session count
        const sessionCount = message.data && Array.isArray(message.data) ? message.data.length : 'unknown';
        const date = message.data && message.data[0] && message.data[0].start_time ?
            new Date(message.data[0].start_time * 1000).toDateString() : 'unknown';
        messageId = `${message.type}_${message.symbol || 'unknown'}_${date}_${sessionCount}`;
    } else if (message.type === 'positions_update') {
        // For position updates, include timestamp and position count
        const timestamp = message.timestamp || Date.now();
        const positionCount = message.positions && Array.isArray(message.positions) ? message.positions.length : 'unknown';
        messageId = `${message.type}_${message.symbol || 'unknown'}_${timestamp}_${positionCount}`;
    } else if (message.type === 'drawings') {
        // For drawings data, include drawing count
        const drawingCount = message.data && Array.isArray(message.data) ? message.data.length :
                            message.drawings && message.drawings.length || 'unknown';
        messageId = `${message.type}_${message.symbol || 'unknown'}_${drawingCount}`;
    } else if (message.type === 'buy_signals') {
        // For buy signals, include signal count and timestamp range
        const signalCount = message.data && Array.isArray(message.data) ? message.data.length : 'unknown';
        const firstSignal = message.data && message.data[0] ? message.data[0].timestamp : 'unknown';
        messageId = `${message.type}_${message.symbol || 'unknown'}_${signalCount}_${firstSignal}`;
    } else if (message.type === 'history_update') {
        // For history updates, include data point count
        const pointCount = message.data && Array.isArray(message.data) ? message.data.length : 'unknown';
        messageId = `${message.type}_${message.symbol || 'unknown'}_${pointCount}`;
    } else if (message.type === 'youtube_videos') {
        // For YouTube videos, include video count
        const videoCount = message.data && Array.isArray(message.data) ? message.data.length : 'unknown';
        messageId = `${message.type}_${message.symbol || 'unknown'}_${videoCount}`;
    } else if (message.type === 'trade_history') {
        // For trade history, include trade count and timestamp range
        const tradeCount = message.data && Array.isArray(message.data) ? message.data.length : 'unknown';
        const latestTrade = message.data && message.data.length > 0 ?
            message.data[message.data.length - 1].timestamp : 'unknown';
        messageId = `${message.type}_${message.symbol || 'unknown'}_${tradeCount}_${latestTrade}`;
    } else {
        // For other message types, use the original method
        messageId = `${message.type}_${message.symbol || 'unknown'}_${JSON.stringify(message.data || {}).slice(0, 100)}`;
    }

    // Check for duplicate messages
    if (processedMessageIds.has(messageId)) {
        return;
    }

    // Add timestamp range validation for historical data with safe data handling
    if (message.type === 'historical' && lastProcessedTimestampRange) {
        if (!message.data || !Array.isArray(message.data) || message.data.length === 0) {
            console.warn('⚠️ Invalid or empty historical data in duplicate range check - skipping');
        } else {
            // Find first valid timestamp
            let messageFromTs = null;
            for (const point of message.data) {
                if (point && point.time && !isNaN(point.time) && point.time > 0) {
                    messageFromTs = point.time;
                    break;
                }
            }

            // Find last valid timestamp
            let messageToTs = null;
            for (let i = message.data.length - 1; i >= 0; i--) {
                const point = message.data[i];
                if (point && point.time && !isNaN(point.time) && point.time > 0) {
                    messageToTs = point.time;
                    break;
                }
            }

            if (messageFromTs !== null && messageToTs !== null) {
                const isDuplicateRange = (
                    Math.abs(messageFromTs - lastProcessedTimestampRange.fromTs) < 60 && // Within 1 minute
                    Math.abs(messageToTs - lastProcessedTimestampRange.toTs) < 60
                );

                if (isDuplicateRange) {
                    return;
                }

                // Update last processed range
                lastProcessedTimestampRange = { fromTs: messageFromTs, toTs: messageToTs };
            } else {
                console.warn('⚠️ No valid timestamps found in historical data for range validation');
            }
        }
    }

    // Mark message as processed
    processedMessageIds.add(messageId);

    // Keep only last 1000 processed message IDs to prevent memory leaks
    if (processedMessageIds.size > 1000) {
        const idsArray = Array.from(processedMessageIds);
        processedMessageIds.clear();
        idsArray.slice(-500).forEach(id => processedMessageIds.add(id)); // Keep last 500
    }

    messageQueue.push(message);
    processMessageQueue();
}

function processMessageQueue() {
    if (isProcessingMessage || messageQueue.length === 0) {
        return;
    }

    isProcessingMessage = true;
    const message = messageQueue.shift();


    try {
        // Validate message type exists
        if (!message || typeof message !== 'object') {
            console.warn('⚠️ Invalid message format:', message);
            isProcessingMessage = false;
            return;
        }

        if (!message.type) {
            // Try to infer message type from message structure
            if (message.info && message.price && message.amount) {
                // Looks like a single trade message - wrap in array for trade_history handler
                message.type = 'trade_history';
                message.data = [message];
            } else {
                console.info(JSON.stringify(message));
                console.warn('⚠️ Message missing type and cannot infer:', message);
                isProcessingMessage = false;
                return;
            }
        }

        // Log message processing with safe timestamp handling
        let safeLogMessage = 'Processing message';
        if (message.type === 'historical' && !lastProcessedTimestampRange && message.data && Array.isArray(message.data) && message.data.length > 0) {
            safeLogMessage += ` (${message.data.length} historical points)`;
        } else if (message.type === 'historical' && lastProcessedTimestampRange && message.data && Array.isArray(message.data) && message.data.length > 0) {
            // Safe timestamp logging
            const msgFromTs = message.data[0]?.time;
            const msgToTs = message.data[message.data.length - 1]?.time;
            if (typeof msgFromTs === 'number' && !isNaN(msgFromTs) && typeof msgToTs === 'number' && !isNaN(msgToTs)) {
                safeLogMessage += ` (${new Date(msgFromTs * 1000).toISOString()} to ${new Date(msgToTs * 1000).toISOString()})`;
            } else {
                safeLogMessage += ` (${message.data.length} points, timestamp conversion skipped)`;
            }
        }

        // Process message based on type
        function handleYouTubeVideos(message) {

            if (!message.data || !Array.isArray(message.data)) {
                console.warn('🎥 Combined WebSocket: Invalid YouTube videos data format');
                return;
            }

            if (message.data.length === 0) {
                return;
            }

            // Process and display YouTube videos as markers on the chart
            addYouTubeVideosToChart(message.data, message.symbol);

        }

        switch (message.type) {
            case 'historical':
                handleHistoricalData(message);
                break;
            case 'live':
                handle_live_message(message);
                //handleLiveData(message);
                break;
            case 'live_price':
                handleLivePriceUpdate(message);
                break;
            case 'positions_update':
                handlePositionsUpdate(message);
                break;
            case 'drawings':
                handleDrawingsData(message);
                break;
            case 'buy_signals':
                handleBuySignals(message);
                break;
            case 'history_update':
                handleHistoryUpdate(message);
                break;
            case 'youtube_videos':
                handleYouTubeVideos(message);
                break;
            case 'volume_profile':
                handleVolumeProfileMessage(message);
                break;
            case 'trade_history':
                handleTradeHistoryMessage(message);
                break;                
            case 'trading_sessions':
                handleTradingSessionsMessage(message);
                break;
            case 'trade_update':
                handleTradeHistoryMessage(message);
                break;
            case 'trading_sessions':
                handleTradingSessionsMessage(message);
                break;
            case 'ready':
                break;
            default:
                console.warn('⚠️ Unknown message type:', message.type);
        }

    } catch (e) {
        console.error('❌ Combined WebSocket: Error processing queued message:', e.message);
        console.error('❌ Stack trace:', e.stack);
        console.error('❌ Raw message data:', JSON.stringify(message, null, 2));
    } finally {
        isProcessingMessage = false;
        // Process next message if available
        if (messageQueue.length > 0) {
            setTimeout(processMessageQueue, 10); // Small delay to prevent blocking
        }
    }
}

// Chart update lock functions
function acquireChartUpdateLock() {
    console.info("acquireChartUpdateLock");
    if (chartUpdateLock) {
        console.warn('🔒 Chart update lock already held, waiting...');
        return false;
    }
    chartUpdateLock = true;
    return true;
}

function releaseChartUpdateLock() {
    chartUpdateLock = false;
}

function isChartUpdateLocked() {
    return chartUpdateLock;
}

// Debounced chart update function
function debouncedChartUpdate(updateFunction, ...args) {
    clearTimeout(chartUpdateDebounceTimer);
    chartUpdateDebounceTimer = setTimeout(() => {
        if (!acquireChartUpdateLock()) {
            console.warn('⚠️ Skipping debounced chart update - lock held');
            return;
        }

        try {
            updateFunction(...args);
        } finally {
            releaseChartUpdateLock();
        }
    }, CHART_UPDATE_DEBOUNCE_DELAY);
}

function handleRealtimeKlineForCombined(dataPoint) {

    if (!dataPoint) {
        console.warn('🔴 Combined WebSocket: No data point provided to handleRealtimeKlineForCombined');
        return;
    }

    // Check if currently dragging a shape - skip live price updates during dragging
    // console.log('🔄 LIVE UPDATE CHECK: isDraggingShape =', window.isDraggingShape, 'typeof:', typeof window.isDraggingShape);
    if (window.isDraggingShape) {
        console.log('🔄 LIVE UPDATE BLOCKED: Skipping live price update during shape dragging');
        console.warn('🚫 LIVE PRICE UPDATE PREVENTED DUE TO DRAGGING');
        return;
    }

    // Check if chart is ready
    const gd = document.getElementById('chart');
    if (!gd || !gd.layout) {
        console.warn('🔴 Combined WebSocket: Chart not ready for live price line');
        return;
    }

    // Extract price data with comprehensive fallbacks
    let livePrice = null;

    // First try OHLC close price
    if (dataPoint.ohlc && typeof dataPoint.ohlc.close === 'number' && !isNaN(dataPoint.ohlc.close)) {
        livePrice = dataPoint.ohlc.close;
    }
    // Then try direct close property
    else if (typeof dataPoint.close === 'number' && !isNaN(dataPoint.close)) {
        livePrice = dataPoint.close;
    }
    // Then try direct price property
    else if (typeof dataPoint.price === 'number' && !isNaN(dataPoint.price)) {
        livePrice = dataPoint.price;
    }
    // Backend sometimes sends "live_price" property
    else if (typeof dataPoint.live_price === 'number' && !isNaN(dataPoint.live_price)) {
        livePrice = dataPoint.live_price;
    }

    // Final validation
    if (livePrice === null || typeof livePrice !== 'number' || isNaN(livePrice)) {
        console.warn('🔴 Combined WebSocket: Invalid live price detected', {
            livePrice: livePrice,
            livePriceType: typeof livePrice,
            dataPointKeys: dataPoint ? Object.keys(dataPoint) : 'dataPoint is null/undefined',
            ohlcExists: !!(dataPoint && dataPoint.ohlc),
            ohlcKeys: dataPoint && dataPoint.ohlc ? Object.keys(dataPoint.ohlc) : null,
            hasClose: !!(dataPoint && dataPoint.close),
            closeType: dataPoint && dataPoint.close ? typeof dataPoint.close : null,
            hasPrice: !!(dataPoint && dataPoint.price),
            priceType: dataPoint && dataPoint.price ? typeof dataPoint.price : null
        });
        return;
    }

    // Get candle timing information with validation
    let candleStartTimeMs;
    if (dataPoint.time && !isNaN(dataPoint.time) && dataPoint.time > 0) {
        candleStartTimeMs = dataPoint.time * 1000;
        // Validate the result
        if (!isFinite(candleStartTimeMs)) {
            console.warn('⚠️ Invalid candle start time calculated:', candleStartTimeMs);
            return;
        }
    } else {
        console.warn('⚠️ Invalid data point time for candle timing:', dataPoint.time);
        return;
    }

    const timeframeSeconds = getTimeframeSecondsJS(combinedResolution);
    const candleEndTimeMs = candleStartTimeMs + (timeframeSeconds * 1000);

    // Validate candle end time
    if (!isFinite(candleEndTimeMs)) {
        console.warn('⚠️ Invalid candle end time calculated:', candleEndTimeMs);
        return;
    }



    // Draw the live price line
    // console.log('📈 DRAWING LIVE PRICE LINE: price =', livePrice, 'at time =', new Date(candleStartTimeMs).toISOString());
    updateOrAddRealtimePriceLine(gd, livePrice, candleStartTimeMs, candleEndTimeMs, true);
}

function setupCombinedWebSocket(symbol, indicators = [], resolution = '1h', fromTs = null, toTs = null) {
    // Initialize default timestamps if not provided
    if (fromTs === null || toTs === null) {
        initializeDefaultTimestamps();
        // Use the initialized timestamps if none were provided
        if (fromTs === null) fromTs = combinedFromTs;
        if (toTs === null) toTs = combinedToTs;
    }

    // Increment connection ID for logging
    websocketConnectionId++;

    logWebSocketEvent('setup_called', {
        symbol,
        indicatorsCount: indicators.length,
        resolution,
        fromTs,
        toTs,
        wsAPIConnected: window.wsAPI ? window.wsAPI.connected : false
    });

    // Check if this is a duplicate call (same parameters)
    const isDuplicateCall = (
        combinedSymbol === symbol &&
        combinedResolution === resolution &&
        JSON.stringify(combinedIndicators) === JSON.stringify(indicators) &&
        combinedFromTs === fromTs &&
        combinedToTs === toTs
    );

    if (isDuplicateCall && window.wsAPI && window.wsAPI.connected) {
        logWebSocketEvent('duplicate_call_skipped', {
            reason: 'Same parameters, wsAPI already connected'
        });
        return;
    }

    // Clear any pending debounced calls
    if (websocketSetupDebounceTimer) {
        clearTimeout(websocketSetupDebounceTimer);
        websocketSetupDebounceTimer = null;
    }

    // Debounce the WebSocket setup - just set flags, message handlers will be set up when wsAPI is fully connected
    websocketSetupDebounceTimer = setTimeout(() => {
        logWebSocketEvent('websocket_setup_debounced', {
            symbol,
            indicatorsCount: indicators.length,
            resolution,
            fromTs,
            toTs
        });
    }, WEBSOCKET_SETUP_DEBOUNCE_DELAY);
}

function setupWebSocketMessageHandler() {

    if (!window.wsAPI) {
        console.warn('Combined WebSocket: Cannot setup message handler - wsAPI not available');
        return;
    }

    if (!window.wsAPI.connected) {
        console.warn('Combined WebSocket: Cannot setup message handler - wsAPI not connected');
        return;
    }

    // Register message handlers for all message types we expect
    const messageTypes = [
        'historical', 'live', 'live_price', 'positions_update', 'drawings',
        'buy_signals', 'history_update', 'youtube_videos', 'volume_profile',
        'trade_history', 'trading_sessions', 'ready'
    ];

    messageTypes.forEach(messageType => {
        window.wsAPI.onMessage(messageType, (message) => {
            try {
                // console.log('Received message:', message);

                // Enqueue message for sequential processing
                enqueueMessage(message);
            } catch (e) {
                console.error('❌ Combined WebSocket: Error processing message:', e.message);
                console.error('❌ Raw message data:', message);
            }
        });
    });

}

function sendCombinedConfig(fromMs = null, toMs = null) {
    const symbol = window.symbolSelect ? window.symbolSelect.value : null;
    const resolution = window.resolutionSelect ? window.resolutionSelect.value : '1h';

    // Use provided parameters or fall back to currentXAxisRange
    const finalFromMs = fromMs !== null ? fromMs : (window.currentXAxisRange ? window.currentXAxisRange[0] : null);
    const finalToMs = toMs !== null ? toMs : (window.currentXAxisRange ? window.currentXAxisRange[1] : null);

    const minValueSlider = document.getElementById('min-value-slider');
    let minPercentage = minValueSlider ? parseFloat(minValueSlider.value) || 0 : 0;    

    if (!window.wsAPI || !window.wsAPI.connected) {
        console.warn('Combined WebSocket: Cannot send config - wsAPI not connected');
        return;
    }

    const config = {
        type: 'config',
        symbol: symbol,  // Include symbol for redundancy and clarity
        active_indicators: window.combinedIndicators,
        resolution: resolution,
        xAxisMin: finalFromMs,  // Now ISO timestamp string
        xAxisMax: finalToMs
    };

    // Detailed timestamp logging for server comparison
    const fromDate = new Date(finalFromMs);
    const toDate = new Date(finalToMs);
    const rangeMs = toDate.getTime() - fromDate.getTime();
    const rangeHours = rangeMs / (1000 * 60 * 60);

    // Store server range for comparison with client range
    window.lastServerRange = {
        symbol: symbol,
        fromTs: finalFromMs,
        toTs: finalToMs,
        fromDate: fromDate.toISOString(),
        toDate: toDate.toISOString(),
        rangeMs,
        rangeHours: rangeHours.toFixed(1),
        timestamp: new Date().toISOString()
    };

    try {
        // Send message through wsAPI instead of direct WebSocket
        const success = window.wsAPI.sendMessage({ type: "config", data: config });

        // Send trade_history message
        const tradeHistoryMessage = {
            type: "trade_history",
            data: {
                symbol: symbol,
                email: window.userEmail || '',
                from_ts: finalFromMs,
                to_ts: finalToMs,
                minValuePercentage: minPercentage
            }
        };

        window.wsAPI.sendMessage(tradeHistoryMessage);

        // Send get_trading_sessions message if time range is small enough
        const timeRangeSeconds = (finalToMs - finalFromMs) / 1000;
        const threeDaysSeconds = 3 * 24 * 60 * 60;

        if (timeRangeSeconds < threeDaysSeconds) {
            const tradingSessionsMessage = {
                type: "get_trading_sessions",
                data: {
                    symbol: symbol,
                    from_ts: finalFromMs,
                    to_ts: finalToMs
                }
            };

            window.wsAPI.sendMessage(tradingSessionsMessage);
        }

    } catch (error) {
        console.error('Combined WebSocket: Error sending config:', error);
    }
}

function validateIndicatorData(data, indicatorName) {
    /**
     * Validates indicator data for null values.
     * - Null values are allowed during indicator warmup periods (earlier timestamps)
     * - No null values allowed in the user's requested time range (combinedFromTs to combinedToTs)
     * Returns true if validation passes, false only for data issues in requested time range.
     */
    if (!data || !Array.isArray(data)) {
        console.error(`❌ VALIDATION FAILED: ${indicatorName} - Invalid data format`);
        return false;
    }

    if (!combinedFromTs || !combinedToTs) {
        console.warn(`⚠️ VALIDATION WARNING: ${indicatorName} - No time range defined, allowing any data`);
        return true;
    }

    // Validate that time range values are valid (numbers, Date objects, or ISO strings)
    const fromTsValid = isValidTimestamp(combinedFromTs);
    const toTsValid = isValidTimestamp(combinedToTs);

    // Helper function to validate timestamps in any supported format
    function isValidTimestamp(ts) {
        if (ts === null || ts === undefined) return false;
        if (typeof ts === 'string') {
            // Check if it's a valid ISO string
            try {
                const date = new Date(ts);
                return !isNaN(date.getTime()) && date.getTime() > 0;
            } catch(e) {
                return false;
            }
        }
        if (typeof ts === 'number') {
            return !isNaN(ts) && ts > 0;
        }
        if (ts instanceof Date) {
            return !isNaN(ts.getTime()) && ts.getTime() > 0;
        }
        return false;
    }

    let lookbackNulls = 0;
    let lookbackPoints = 0;
    let userRangeNulls = 0;
    let userRangePoints = 0;

    // Check each data point for null values in indicator fields
    // Count nulls SEPARATELY for user-requested range vs lookback period
    data.forEach((point, index) => {
        if (point.indicators) {
            // Determine if this point is within user's requested time range
            const isInUserRange = fromTsValid && toTsValid && combinedFromTs <= point.time && point.time <= combinedToTs;

            // Determine if this point is outside user range (before or after)
            const isBeforeUserRange = point.time < combinedFromTs;
            const isAfterUserRange = point.time > combinedToTs;
            const isInLookbackRange = !isInUserRange && (isBeforeUserRange || isAfterUserRange);

            Object.keys(point.indicators).forEach(indicatorId => {
                const indicatorData = point.indicators[indicatorId];
                if (indicatorData) {
                    Object.keys(indicatorData).forEach(key => {
                        const value = indicatorData[key];
                        const isNull = value === null || value === undefined;

                        if (isNull) {
                            if (isInUserRange) {
                                userRangeNulls++;
                            } else if (isInLookbackRange) {
                                lookbackNulls++;
                            }
                        }

                        if (isInUserRange) {
                            userRangePoints++;
                        } else if (isInLookbackRange) {
                            lookbackPoints++;
                        }
                    });
                }
            });
        }
    });

    // LOGGING: Show data range breakdown with safe date formatting

    if (!fromTsValid || !toTsValid) {
        console.warn(`⚠️ VALIDATION WARNING: ${indicatorName} - Invalid time range values (fromTs: ${combinedFromTs}, toTs: ${combinedToTs}), using fallback range determination`);
    }

    let lookbackDateStr = 'invalid';
    if (fromTsValid) {
        try {
            lookbackDateStr = new Date((combinedFromTs - 86400) * 1000).toISOString(); // 1 day before
        } catch(e) {
            lookbackDateStr = 'date_conversion_error';
        }
    }

    let userRangeStartStr = 'invalid';
    let userRangeEndStr = 'invalid';
    if (fromTsValid) {
        try {
            userRangeStartStr = new Date(combinedFromTs * 1000).toISOString();
        } catch(e) {
            userRangeStartStr = 'date_conversion_error';
        }
    }
    if (toTsValid) {
        try {
            userRangeEndStr = new Date(combinedToTs * 1000).toISOString();
        } catch(e) {
            userRangeEndStr = 'date_conversion_error';
        }
    }

    // CRITICAL FIX: Accept nulls from indicator warmup periods at the start of data series
    // Only reject if there are nulls in the middle of the completed data (real gaps)
    if (userRangeNulls > 0) {
        // Find the pattern of nulls in user range
        // Look for consecutive nulls early in the series (warmup period)
        // vs isolated nulls later in the series (actual data gaps)

        let firstValidPointIndex = -1;
        let lastNullPointIndex = -1;
        let totalPointsInRange = 0;

        data.forEach((point, index) => {
            const isInUserRange = fromTsValid && toTsValid && point.time >= combinedFromTs && point.time <= combinedToTs;

            if (!isInUserRange) return; // Skip points outside user range

            totalPointsInRange++;
            let hasValidIndicators = false;

            if (point.indicators) {
                for (const indicatorId in point.indicators) {
                    for (const key in point.indicators[indicatorId]) {
                        const value = point.indicators[indicatorId][key];
                        if (value !== null && value !== undefined && !isNaN(value)) {
                            hasValidIndicators = true;
                            break;
                        }
                    }
                    if (hasValidIndicators) break;
                }
            }

            if (hasValidIndicators && firstValidPointIndex === -1) {
                firstValidPointIndex = index;
            }

            if (!hasValidIndicators) {
                lastNullPointIndex = index;
            }
        });

        // Check if nulls are clustered at the beginning (warmup periods)
        // vs nulls appearing later in the series (actual data gaps)
        const nullsAreWarmupPeriod = (firstValidPointIndex > 0 && (lastNullPointIndex < firstValidPointIndex));

        // Calculate nulls before first valid value vs nulls after first valid value
        let nullsInWarmupPeriod = 0;
        let nullsAfterWarmupPeriod = 0;
        let pointsProcessed = 0;

        data.forEach((point, index) => {
            const isInUserRange = fromTsValid && toTsValid && point.time >= combinedFromTs && point.time <= combinedToTs;

            if (!isInUserRange) return;

            pointsProcessed++;
            let hasValidIndicators = false;

            if (point.indicators) {
                for (const indicatorId in point.indicators) {
                    for (const key in point.indicators[indicatorId]) {
                        const value = point.indicators[indicatorId][key];
                        if (value !== null && value !== undefined && !isNaN(value)) {
                            hasValidIndicators = true;
                            break;
                        }
                    }
                    if (hasValidIndicators) break;
                }
            }

            // Count nulls:
            // - Before first valid point: expected warmup period nulls
            // - After first valid point: unexpected data gaps
            if (!hasValidIndicators) {
                if (pointsProcessed <= (firstValidPointIndex - point.index + 1) || firstValidPointIndex === -1) {
                    nullsInWarmupPeriod++;
                } else {
                    nullsAfterWarmupPeriod++;
                }
            }
        });

        // Only reject if there are nulls AFTER the warmup period (real data gaps)
        if (nullsAfterWarmupPeriod > 0) {
            const rangeStr = fromTsValid && toTsValid ? `(${userRangeStartStr} to ${userRangeEndStr})` : '(invalid time range)';
            console.error(`❌ VALIDATION FAILED: ${indicatorName} - Found ${nullsAfterWarmupPeriod} null values in COMPLETED data (after warmup period) within requested time range ${rangeStr}. This indicates data gaps that are not acceptable.`);
            return false;
        }

        // Accept nulls if they're concentrated in warmup period (normal behavior)
    }

    // If time range is completely corrupted, we have no way to know what's the "last bar"
    // Return false to reject this data and wait for a proper time range
    if (!fromTsValid || !toTsValid) {
        console.error(`❌ VALIDATION FAILED: ${indicatorName} - Time range is completely corrupted (fromTs: ${combinedFromTs}, toTs: ${combinedToTs}). Cannot determine user range.`);
        return false;
    }

    // WARNING: Log lookback (warmup) nulls (this is expected and acceptable)
    if (lookbackNulls > 0) {
    } else {
    }

    return true;
}

function handleHistoricalData(message) {

    if (!message.data || !Array.isArray(message.data)) {
        console.warn(JSON.stringify(message, null, 2));
        console.warn('Combined WebSocket: Invalid or empty historical data');
        return;
    }

    // Check if historical data contains drawings and process them
    if (message.data.drawings && Array.isArray(message.data.drawings)) {
        console.log('Combined WebSocket: Found drawings in historical data, processing...', message.data.drawings.length);
        const drawingsMessage = {
            drawings: message.data.drawings,
            symbol: message.symbol
        };
        handleDrawingsData(drawingsMessage);
    }

    if (!Array.isArray(message.data) || message.data.length === 0) {
        console.warn('Combined WebSocket: No valid OHLC data in historical message');
        return;
    }

    // Validate indicator data with enhanced diagnostics
    if (!validateIndicatorData(message.data, `historical_${message.symbol}`)) {
        console.error('🚨 CRITICAL: Historical data validation failed - rejecting data to prevent chart issues');
        console.error('📊 VALIDATION FAILURE DETAILS:');
        console.error('  Symbol:', message.symbol);
        console.error('  Data points:', message.data.length);
        console.error('  First point time:', message.data[0]?.time);
        console.error('  Last point time:', message.data[message.data.length - 1]?.time);
        return;
    }

    // Check if chart is ready
    const chartElement = document.getElementById('chart');
    if (!chartElement || !window.gd || !window.gd._fullLayout) {
        return;
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
            } else if (trace.name && typeof trace.name === 'string' && trace.name.startsWith('Stoch K')) {
                // Extract variant from trace name (e.g., 'Stoch K (14,3)' -> 'stochrsi_14_3')
                const variantMatch = trace.name.match(/Stoch K \((\d+),(\d+)\)/);
                if (variantMatch) {
                    indicatorId = `stochrsi_${variantMatch[1]}_${variantMatch[2]}`;
                } else {
                    // Fallback to first stochrsi variant if no variant found
                    indicatorId = combinedIndicators.find(id => id.startsWith('stochrsi'));
                }
                valueKey = 'stoch_k';
            } else if (trace.name && typeof trace.name === 'string' && trace.name.startsWith('Stoch D')) {
                // Extract variant from trace name (e.g., 'Stoch D (14,3)' -> 'stochrsi_14_3')
                const variantMatch = trace.name.match(/Stoch D \((\d+),(\d+)\)/);
                if (variantMatch) {
                    indicatorId = `stochrsi_${variantMatch[1]}_${variantMatch[2]}`;
                } else {
                    // Fallback to first stochrsi variant if no variant found
                    indicatorId = combinedIndicators.find(id => id.startsWith('stochrsi'));
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

    // Merge new data with existing data, preserving indicators where possible
    const mergedData = mergeDataPointsWithIndicators(existingData, message.data);


    // Update chart with merged data
    updateChartWithHistoricalData(mergedData, message.symbol);



}

function mergeDataPoints(existingData, newData) {

    if (!existingData || existingData.length === 0) {
        return newData.sort((a, b) => a.time - b.time);
    }

    if (!newData || newData.length === 0) {
        return existingData;
    }

    // Log timestamp ranges for debugging
    const existingTimestamps = existingData.map(p => p.time).sort((a, b) => a - b);
    const newTimestamps = newData.map(p => p.time).sort((a, b) => a - b);


    // Combine all data points
    const combinedData = [...existingData, ...newData];

    // Sort by timestamp to ensure proper ordering
    const sortedData = combinedData.sort((a, b) => a.time - b.time);

    // Remove duplicates by timestamp, keeping the most recent data
    const mergedData = [];
    const seenTimestamps = new Set();
    let duplicatesFound = 0;
    let overlapsFound = 0;

    for (const point of sortedData) {
        if (!seenTimestamps.has(point.time)) {
            seenTimestamps.add(point.time);
            mergedData.push(point);
        } else {
            // If we have a duplicate timestamp, replace the existing one with the new one
            // This ensures we keep the most recent data for the same timestamp
            const existingIndex = mergedData.findIndex(p => p.time === point.time);
            if (existingIndex !== -1) {
                // Safe timestamp logging
                let timestampStr = 'invalid';
                if (point.time && !isNaN(point.time) && point.time > 0) {
                    try {
                        timestampStr = new Date(point.time * 1000).toISOString();
                    } catch(e) {
                        timestampStr = 'conversion_error';
                    }
                }
                mergedData[existingIndex] = point;
                duplicatesFound++;
            }
            overlapsFound++;
        }
    }

    const duplicatesRemoved = combinedData.length - mergedData.length;

    if (duplicatesRemoved > 0) {
    }

    return mergedData;
}

function mergeDataPointsWithIndicators(existingData, newData) {

    if (!existingData || existingData.length === 0) {
        return newData.sort((a, b) => a.time - b.time);
    }

    if (!newData || newData.length === 0) {
        return existingData;
    }

    // Analyze indicator presence
    const existingWithIndicators = existingData.filter(p => Object.keys(p.indicators || {}).length > 0).length;
    const newWithIndicators = newData.filter(p => Object.keys(p.indicators || {}).length > 0).length;


    // Create a map of existing data by timestamp for quick lookup
    const existingDataMap = new Map();
    existingData.forEach(point => {
        existingDataMap.set(point.time, point);
    });

    // Process new data and merge with existing
    const mergedData = [...existingData]; // Start with existing data
    let replacedPoints = 0;
    let addedPoints = 0;
    let mergedIndicators = 0;

    newData.forEach(newPoint => {
        const existingPoint = existingDataMap.get(newPoint.time);

        if (existingPoint) {
            // Timestamp exists - merge the data, preferring data with indicators
            const hasExistingIndicators = Object.keys(existingPoint.indicators || {}).length > 0;
            const hasNewIndicators = Object.keys(newPoint.indicators || {}).length > 0;

            // Safe timestamp logging
            let timestampStr = 'invalid';
            if (newPoint.time && !isNaN(newPoint.time) && newPoint.time > 0) {
                try {
                    timestampStr = new Date(newPoint.time * 1000).toISOString();
                } catch(e) {
                    timestampStr = 'conversion_error';
                }
            }

            if (hasNewIndicators && !hasExistingIndicators) {
                // New data has indicators, existing doesn't - use new data
                const index = mergedData.findIndex(p => p.time === newPoint.time);
                if (index !== -1) {
                    mergedData[index] = { ...newPoint };
                    replacedPoints++;
                }
            } else if (hasNewIndicators && hasExistingIndicators) {
                // Both have indicators - merge them
                const index = mergedData.findIndex(p => p.time === newPoint.time);
                if (index !== -1) {
                    const existingIndicatorKeys = Object.keys(existingPoint.indicators);
                    const newIndicatorKeys = Object.keys(newPoint.indicators);

                    mergedData[index] = {
                        ...newPoint,
                        indicators: { ...existingPoint.indicators, ...newPoint.indicators }
                    };
                    mergedIndicators++;
                }
            } else {
            }
            // If existing has indicators but new doesn't, keep existing
        } else {
            // New timestamp - add it
            mergedData.push(newPoint);
            addedPoints++;
        }
    });

    // Sort by timestamp to ensure proper ordering
    const sortedData = mergedData.sort((a, b) => a.time - b.time);


    return sortedData;
}

// Helper function to merge existing indicator traces with new data
function mergeIndicatorData(existingTrace, newTrace) {

    if (!existingTrace || !existingTrace.x || !newTrace || !newTrace.x) {
        return { x: newTrace.x || [], y: newTrace.y || [] };
    }

    // Create maps for quick lookup by timestamp
    const existingMap = new Map();
    existingTrace.x.forEach((timestamp, index) => {
        if (timestamp && existingTrace.y[index] !== null && existingTrace.y[index] !== undefined) {
            existingMap.set(timestamp.getTime(), existingTrace.y[index]);
        }
    });

    const newMap = new Map();
    newTrace.x.forEach((timestamp, index) => {
        if (timestamp && newTrace.y[index] !== null && newTrace.y[index] !== undefined) {
            newMap.set(timestamp.getTime(), newTrace.y[index]);
        }
    });

    // Get all unique timestamps
    const allTimestamps = new Set([...existingMap.keys(), ...newMap.keys()]);
    const sortedTimestamps = Array.from(allTimestamps).sort((a, b) => a - b);


    // Merge data, preferring new values over existing
    const mergedX = [];
    const mergedY = [];

    sortedTimestamps.forEach(timestamp => {
        // New data takes precedence for the same timestamp
        const yValue = newMap.has(timestamp) ? newMap.get(timestamp) : existingMap.get(timestamp);

        if (yValue !== null && yValue !== undefined && !isNaN(yValue)) {
            mergedX.push(new Date(timestamp));
            mergedY.push(yValue);
        }
    });

    return { x: mergedX, y: mergedY };
}

function handleLiveData(message) {

    if (!message.data) {
        console.warn('🔴 Combined WebSocket: Invalid live data format - no data field');
        return;
    }

    // Check if chart is ready
    const gd = document.getElementById('chart');

    // Process live data and update chart
    updateChartWithLiveData(message.data, message.symbol);
}

function handleLivePriceUpdate(message) {

    if (!message.price || typeof message.price !== 'number') {
        console.warn('💰 Combined WebSocket: Invalid live price format');
        return;
    }

    // Check if currently dragging a shape - skip live price updates during dragging
    if (window.isDraggingShape) {
        return;
    }

    // Check if chart is ready
    const gd = document.getElementById('chart');
    if (!gd || !gd.layout) {
        console.warn('💰 Combined WebSocket: Chart not ready for live price update');
        return;
    }

    // Get candle timing information for the live price line
    const currentTime = message.timestamp || Math.floor(Date.now() / 1000);
    const candleStartTimeMs = currentTime * 1000;
    const candleEndTimeMs = candleStartTimeMs + (getTimeframeSecondsJS(combinedResolution) * 1000);



    // Draw the live price line without triggering relayout to prevent unwanted plotly_relayout events
    // updateOrAddRealtimePriceLine(gd, message.price, candleStartTimeMs, candleEndTimeMs, false);
}

function handleBuySignals(message) {

    if (!message.data || !Array.isArray(message.data)) {
        console.warn('Combined WebSocket: Invalid buy signals data format');
        return;
    }

    if (message.data.length === 0) {
        return;
    }

    // Store buy signals by timestamp for enhanced candlestick hover info
    message.data.forEach(signal => {
        const timestampKey = signal.timestamp.toString();
        buySignalsByTimestamp[timestampKey] = {
            rsi: signal.rsi,
            rsi_sma14: signal.rsi_sma14,
            deviation: signal.deviation,
            sma_trend_up: signal.sma_trend_up
        };
    });

    // Process and add buy signals to the chart
    addBuySignalsToChart(message.data, message.symbol);
}

function handleDrawingsData(message) {

    // Handle both old and new message formats for compatibility
    let drawingsData = message.drawings || message.data;

    // If message.data is an object with drawings array, use that
    if (!drawingsData && message.data && Array.isArray(message.data.drawings)) {
        drawingsData = message.data.drawings;
    }

    // If drawingsData is still not found, check if the entire message.data is the drawings
    if (!drawingsData || !Array.isArray(drawingsData)) {
        console.warn('Combined WebSocket: Invalid drawings data format - no drawings array found');
        console.warn('Expected message structure:', { symbol: 'SYMBOL', drawings: '[array of drawings]' });
        console.warn('Received message structure:', JSON.stringify(message, null, 2));
        return;
    }

    if (drawingsData.length === 0) {
        return;
    }


    // Process and add drawings to the chart
    addDrawingsToChart(drawingsData, message.symbol || 'unknown');
}

function addPositionsToChart(positions, symbol) {

    const chartElement = document.getElementById('chart');
    if (!chartElement || !window.gd) {
        console.warn('Combined WebSocket: Chart not ready for positions');
        return;
    }

    // Ensure layout.shapes exists
    if (!window.gd.layout.shapes) {
        window.gd.layout.shapes = [];
    }

    // Remove existing position-related shapes (avoid overlapping)
    window.gd.layout.shapes = window.gd.layout.shapes.filter(shape =>
        !shape.name || (!shape.name.includes('position_') && shape.name !== 'buy_signal_')
    );

    positions.forEach((position, index) => {
        try {
            const entryPrice = parseFloat(position.entryPrice);
            const liquidationPrice = position.liquidation_price ? parseFloat(position.liquidation_price) : null;
            const side = position.side;
            const size = parseFloat(position.size || 0);
            const unrealizedPnL = position.unrealized_pnl ? parseFloat(position.unrealized_pnl) : 0;

            if (!entryPrice || size <= 0) {
                console.warn(`Skipping position ${index}: invalid entry price or size`);
                return;
            }

            // Create a visual indicator for the position entry price
            const positionShape = {
                name: `position_${symbol}_${side}_${index}_${entryPrice}`,
                type: 'line',
                xref: 'paper',
                yref: 'y',
                x0: 0,
                y0: entryPrice,
                x1: 1,
                y1: entryPrice,
                line: {
                    color: side === 'LONG' ? 'green' : 'red',
                    width: size > 0.1 ? 3 : 2, // Thicker line for larger positions
                    dash: 'dash'
                },
                hoverinfo: 'skip' // Use annotations for hover instead
            };

            window.gd.layout.shapes.push(positionShape);

            // Add liquidation price line if available
            if (liquidationPrice && liquidationPrice !== entryPrice) {
                const liquidationShape = {
                    name: `liquidation_${symbol}_${side}_${index}_${liquidationPrice}`,
                    type: 'line',
                    xref: 'paper',
                    yref: 'y',
                    x0: 0,
                    y0: liquidationPrice,
                    x1: 1,
                    y1: liquidationPrice,
                    line: {
                        color: 'orange',
                        width: 2,
                        dash: 'dot'
                    },
                    hoverinfo: 'skip'
                };

                window.gd.layout.shapes.push(liquidationShape);
            }

            // Add annotation with position details
            if (!window.gd.layout.annotations) {
                window.gd.layout.annotations = [];
            }

            const positionLabel = `${side} ${size.toFixed(4)}@${entryPrice.toFixed(2)} P&L:$${unrealizedPnL.toFixed(2)}`;
            const annotation = {
                name: `position_label_${symbol}_${index}`,
                x: 0.98, // Near right edge
                y: entryPrice,
                xref: 'paper',
                yref: 'y',
                text: positionLabel,
                showarrow: false,
                xanchor: 'right',
                yanchor: 'middle',
                font: {
                    size: 10,
                    color: side === 'LONG' ? 'green' : 'red',
                    family: 'Arial, sans-serif'
                },
                bgcolor: 'rgba(255, 255, 255, 0.9)',
                bordercolor: side === 'LONG' ? 'green' : 'red',
                borderwidth: 1,
                borderpad: 2
            };

            // Remove existing annotation for this position
            window.gd.layout.annotations = window.gd.layout.annotations.filter(
                ann => !ann.name || !ann.name.includes(`position_label_${symbol}_${index}`) || ann.name !== annotation.name
            );

            window.gd.layout.annotations.push(annotation);

        } catch (error) {
            console.error(`💼 Error processing position ${index}:`, error);
        }
    });

    // Update the chart with positions
    try {
        Plotly.relayout(chartElement, {
            shapes: window.gd.layout.shapes,
            annotations: window.gd.layout.annotations
        });
    } catch (error) {
        console.error('💼 Error updating chart with positions:', error);
    }
}

function handlePositionsUpdate(message) {

    if (!message.positions || !Array.isArray(message.positions)) {
        console.warn('💼 Combined WebSocket: Invalid positions data format');
        return;
    }

    if (message.positions.length === 0) {
        return;
    }

    // Log position details
    message.positions.forEach((position, index) => {
        const entryPrice = parseFloat(position.entryPrice);
        const liquidationPrice = position.liquidation_price ? parseFloat(position.liquidation_price) : null;
        const side = position.side || 'UNKNOWN';
        const symbol = position.symbol || message.symbol;
        const size = parseFloat(position.size || 0);
        const unrealizedPnL = position.unrealized_pnl ? parseFloat(position.unrealized_pnl) : 0;

    });

    // Process and display positions as visual elements on the chart
    addPositionsToChart(message.positions, message.symbol);

}

function handleHistoryUpdate(message) {
    console.log('📈 HISTORY_UPDATE: Received message', {
        symbol: message.data ? message.data.symbol : message.symbol,
        dataLength: message.data && message.data.ohlcv ? message.data.ohlcv.length : 'no data',
        firstPoint: message.data && message.data.ohlcv && message.data.ohlcv[0] ? {
            time: message.data.ohlcv[0].time,
            hasOHLC: !!(message.data.ohlcv[0].open !== undefined),
            hasIndicators: false, // History updates don't include indicators
            ohlcKeys: message.data.ohlcv[0] ? Object.keys(message.data.ohlcv[0]) : 'no ohlc'
        } : 'no first point'
    });

    // Handle nested data structure - validate required fields exist
    if (!message.data || !message.data.ohlcv || !Array.isArray(message.data.ohlcv)) {
        console.warn('📈 Combined WebSocket: Invalid history update data format');
        console.info(JSON.stringify(message, null, 2));
        return;
    }

    // Extract data from nested structure
    const dataArray = message.data.ohlcv;
    const symbol = message.data.symbol || message.symbol;

    if (!dataArray || dataArray.length === 0) {
        console.warn('📈 HISTORY_UPDATE: Empty data array, skipping');
        return;
    }

    if (message.data.length === 0) {
        console.warn('📈 HISTORY_UPDATE: Empty data array, skipping');
        return;
    }

    // Check if chart is ready
    const chartElement = document.getElementById('chart');
    if (!chartElement || !window.gd || !window.gd.layout) {
        console.warn('📈 Combined WebSocket: Chart not ready for history update');
        return;
    }

    console.log('📈 HISTORY_UPDATE: Chart is ready, processing data points');

    // Process the new data points and update the chart
    // Use the same logic as handleHistoricalData but for smaller batches
    const newDataPoints = dataArray;

    // Get existing chart data to merge with
    const existingTrace = window.gd.data.find(trace => trace.type === 'candlestick');
    let existingData = [];

    if (existingTrace && existingTrace.x && existingTrace.x.length > 0) {
        // Convert existing chart data back to our format
        existingData = existingTrace.x.map((timestamp, index) => ({
            time: timestamp.getTime() / 1000, // Convert back to seconds
            ohlc: {
                open: existingTrace.open && existingTrace.open[index] !== undefined ? existingTrace.open[index] : (existingTrace.close ? existingTrace.close[index] : 0),
                high: existingTrace.high && existingTrace.high[index] !== undefined ? existingTrace.high[index] : (existingTrace.close ? existingTrace.close[index] : 0),
                low: existingTrace.low && existingTrace.low[index] !== undefined ? existingTrace.low[index] : (existingTrace.close ? existingTrace.close[index] : 0),
                close: existingTrace.close && existingTrace.close[index] !== undefined ? existingTrace.close[index] : 0,
                volume: existingTrace.volume && existingTrace.volume[index] !== undefined ? existingTrace.volume[index] : 0
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
            if (!trace.name || typeof trace.name !== 'string') return; // Skip traces without valid string names

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
                // Extract variant from trace name
                const variantMatch = trace.name.match(/Stoch K \((\d+),(\d+)\)/);
                if (variantMatch) {
                    indicatorId = `stochrsi_${variantMatch[1]}_${variantMatch[2]}`;
                } else {
                    indicatorId = combinedIndicators.find(id => id.startsWith('stochrsi'));
                }
                valueKey = 'stoch_k';
            } else if (trace.name.startsWith('Stoch D')) {
                // Extract variant from trace name
                const variantMatch = trace.name.match(/Stoch D \((\d+),(\d+)\)/);
                if (variantMatch) {
                    indicatorId = `stochrsi_${variantMatch[1]}_${variantMatch[2]}`;
                } else {
                    indicatorId = combinedIndicators.find(id => id.startsWith('stochrsi'));
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

    // Check if new data has indicators (history updates typically don't have indicators)
    const newDataHasIndicators = newDataPoints.some(p => p.indicators && Object.keys(p.indicators).length > 0);
    const existingDataHasIndicators = existingData.some(p => p.indicators && Object.keys(p.indicators).length > 0);

    console.log('📈 HISTORY_UPDATE: Data analysis', {
        existingDataLength: existingData.length,
        newDataLength: newDataPoints.length,
        existingHasIndicators: existingDataHasIndicators,
        newHasIndicators: newDataHasIndicators,
        last5Points: newDataPoints.slice(-5).map((p, i) => ({
            index: newDataPoints.length - 5 + i,
            time: p.time,
            isoTime: new Date(p.time * 1000).toISOString(),
            hasOHLC: !!(p.open !== undefined || p.ohlc), // Check for flat OHLC or nested
            hasIndicators: !!(p.indicators),
            indicatorKeys: p.indicators ? Object.keys(p.indicators) : []
        }))
    });

    // If new data doesn't have indicators but existing data does, we need to preserve indicators
    let dataToProcess = newDataPoints;
    if (!newDataHasIndicators && existingDataHasIndicators) {
        console.log('📈 HISTORY_UPDATE: New data lacks indicators, preserving existing indicators');

        // Create a map of existing indicators by timestamp for quick lookup
        const existingIndicatorsByTime = new Map();
        existingData.forEach(point => {
            if (point.indicators && Object.keys(point.indicators).length > 0) {
                existingIndicatorsByTime.set(point.time, point.indicators);
            }
        });

        // Add indicators to new data points where they exist
        dataToProcess = newDataPoints.map(newPoint => {
            const existingIndicators = existingIndicatorsByTime.get(newPoint.time);
            if (existingIndicators) {
                return {
                    ...newPoint,
                    indicators: existingIndicators
                };
            }
            return newPoint;
        });

        console.log('📈 HISTORY_UPDATE: Preserved indicators for', dataToProcess.filter(p => p.indicators).length, 'points');
    }

    const mergedData = mergeDataPointsWithIndicators(existingData, dataToProcess);

    console.log('📈 HISTORY_UPDATE: Merged data result', {
        mergedLength: mergedData.length,
        mergedHasIndicators: mergedData.some(p => p.indicators && Object.keys(p.indicators).length > 0),
        indicatorCount: mergedData.filter(p => p.indicators && Object.keys(p.indicators).length > 0).length
    });

    // Update chart with merged data
    console.log('📈 HISTORY_UPDATE: Calling updateChartWithHistoricalData');
    updateChartWithHistoricalData(mergedData, symbol);

}

function addBuySignalsToChart(buySignals, symbol) {

    const chartElement = document.getElementById('chart');
    if (!chartElement || !window.gd) {
        console.warn('Combined WebSocket: Chart not ready for buy signals');
        return;
    }

    // Ensure layout.shapes exists
    if (!window.gd.layout.shapes) {
        window.gd.layout.shapes = [];
    }

    // Remove existing buy signal shapes
    window.gd.layout.shapes = window.gd.layout.shapes.filter(shape => !shape.name || !shape.name.startsWith('buy_signal_'));

    // Process each buy signal
    buySignals.forEach((signal, index) => {
        try {

            // Convert buy signal to Plotly shape format
            const shape = convertBuySignalToShape(signal, index);

            if (shape) {
                window.gd.layout.shapes.push(shape);
            } else {
                console.warn(`💰 Combined WebSocket: Could not convert buy signal to shape:`, signal);
            }
        } catch (error) {
            console.error(`💰 Combined WebSocket: Error processing buy signal ${index}:`, error, signal);
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
    } catch (error) {
        console.error('💰 Combined WebSocket: Error updating chart with buy signals:', error);
    }
}

function convertBuySignalToShape(signal, index) {
    try {

        // Format timestamp for display
        const signalTime = new Date(signal.timestamp * 1000);
        const timeDisplay = signalTime.toLocaleString();

        // Create detailed hover text with all signal information
        const hoverText = [
            `📈 BUY SIGNAL`,
            `Time: ${timeDisplay}`,
            `Price: ${signal.price?.toFixed(2) || 'N/A'}`,
            `RSI: ${signal.rsi?.toFixed(2) || 'N/A'}`,
            `RSI SMA14: ${signal.rsi_sma14?.toFixed(2) || 'N/A'}`,
            `Deviation: ${signal.deviation?.toFixed(2) || 'N/A'}`,
            `SMA Trend Up: ${signal.sma_trend_up === 1 || signal.sma_trend_up === true ? 'Yes' : 'No'}`
        ].join('<br>');

        // Basic shape properties for buy signal marker - made much larger and more visible
        const shape = {
            name: `buy_signal_${signal.timestamp}_${index}`,
            type: 'line',
            xref: 'x',
            yref: 'y',  // Always on main price chart
            x0: new Date(signal.timestamp * 1000),
            x1: new Date(signal.timestamp * 1000),
            y0: signal.price,
            y1: signal.price,
            line: {
                color: 'lime',  // Bright green for better visibility
                width: 8,       // Much thicker line
                dash: 'solid'
            },
            hovertext: hoverText,
            hoverinfo: 'text',  // Show only the custom hover text
            hoverlabel: {
                bgcolor: 'rgba(0, 0, 0, 0.8)',
                bordercolor: 'lime',
                font: { color: 'white', size: 12 }
            },
            layer: 'above',
            editable: false,
            isSystemShape: true,  // Mark as system shape to prevent saving
            systemType: 'buy_signal',  // Additional identification
            // Store signal data for click handler
            signalData: {
                timestamp: signal.timestamp,
                price: signal.price,
                rsi: signal.rsi,
                rsi_sma14: signal.rsi_sma14,
                deviation: signal.deviation,
                sma_trend_up: signal.sma_trend_up,
                timeDisplay: timeDisplay
            }
        };

        // Make a longer horizontal line with time span for better visibility
        shape.x0 = new Date((signal.timestamp - 7200) * 1000); // 2 hours before
        shape.x1 = new Date((signal.timestamp + 7200) * 1000); // 2 hours after (4 hour span total)
        shape.y0 = signal.price;
        shape.y1 = signal.price;

        // Add click handler for buy signal shape
        shape.onclick = function() {
            displayBuySignalDetails(shape.signalData);
        };

        return shape;
    } catch (error) {
        console.error('💰 BUY SIGNALS: Error converting buy signal to shape:', error, signal);
        return null;
    }
}

function addDrawingsToChart(drawings, symbol) {

    const chartElement = document.getElementById('chart');
    if (!chartElement || !window.gd) {
        console.warn('Combined WebSocket: Chart not ready for drawings');
        return;
    }

    // Ensure layout.shapes exists
    if (!window.gd.layout.shapes) {
        window.gd.layout.shapes = [];
    }


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
                // console.warn(`Combined WebSocket: Could not convert drawing to shape:`, drawing);
            }
        } catch (error) {
            // console.error(`Combined WebSocket: Error processing drawing ${index}:`, error, drawing);
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
    } catch (error) {
        console.error('Combined WebSocket: Error updating chart with drawings:', error);
    }
}

function addYouTubeVideosAsMarkers(videos, symbol) {

    const chartElement = document.getElementById('chart');
    if (!chartElement || !window.gd) {
        console.warn('🎥 Combined WebSocket: Chart not ready for YouTube videos');
        return;
    }

    // Prepare data arrays for the scatter trace
    const x = [];
    const y = [];
    const text = [];
    const hovertext = [];
    const video_ids = [];
    const customdata = [];

    // Process each YouTube video
    videos.forEach((video, index) => {
        try {

            // Parse the published timestamp
            let publishedTime;
            if (video.published_at) {
                publishedTime = new Date(video.published_at);
                if (isNaN(publishedTime.getTime())) {
                    console.warn(`🎥 Combined WebSocket: Invalid published_at timestamp: ${video.published_at}`);
                    publishedTime = new Date(); // Fallback
                }
            } else {
                console.warn('🎥 Combined WebSocket: No published_at timestamp provided');
                publishedTime = new Date(); // Fallback
            }

            // Get current chart data to determine Y position (like original youtubeMarkers.js)
            // We want to place YouTube videos near the price data
            let chartHeight = null;
            if (window.gd.layout && window.gd.layout.yaxis && window.gd.layout.yaxis.range) {
                const yMax = window.gd.layout.yaxis.range[1];
                if (yMax !== undefined && yMax !== null && !isNaN(yMax)) {
                    // Position at 85% of the Y range (near the top but not too high)
                    chartHeight = yMax * 0.85;
                }
            }

            if (chartHeight === null || isNaN(chartHeight)) {
                // Fallback: use a reasonable default
                chartHeight = 100;
            }

            // Add data point for this video
            x.push(publishedTime);
            y.push(chartHeight);

            // Create text label for hover/display
            const labelText = `${video.title.substring(0, 50)}${video.title.length > 50 ? '...' : ''}\nby ${video.channel_title}`;
            text.push(labelText);

            // Create detailed hover text
            const publishDateStr = publishedTime.toLocaleString();
            const hoverText = `<b>${video.title}</b><br><br>Channel: ${video.channel_title}<br>Published: ${publishDateStr}<br><br><i>Click to watch on YouTube</i>`;
            hovertext.push(hoverText);

            // Store video metadata
            video_ids.push(video.id);
            customdata.push({
                video_id: video.id,
                title: video.title,
                channel: video.channel_title,
                published_at: video.published_at,
                url: `https://www.youtube.com/watch?v=${video.id}`
            });


        } catch (error) {
            console.error(`🎥 Combined WebSocket: Error processing YouTube video ${index}:`, error, video);
        }
    });

    // Create the YouTube videos scatter trace with diamond markers
    const youtubeTrace = {
        x: x,
        y: y,
        text: text,
        mode: 'markers',
        type: 'scatter',
        name: 'YouTube Videos',
        marker: {
            symbol: 'diamond',
            size: 12,
            color: 'red',
            line: {
                color: 'white',
                width: 2
            }
        },
        hoverinfo: 'text',
        customdata: customdata,
        hovertext: hovertext,
        video_ids: video_ids,
        showlegend: true,
        hoverlabel: {
            bgcolor: 'white',
            bordercolor: 'red',
            font: { color: 'black', size: 12 },
            align: 'left'
        },
        xaxis: 'x',
        yaxis: 'y'  // Place on main price chart
    };


    // Add click handler for YouTube videos (similar to youtubeMarkers.js)
    youtubeTrace.onclick = function(data) {
        if (data && data.points && data.points.length > 0) {
            const point = data.points[0];
            if (point.customdata) {
                const url = point.customdata.url;
                if (url) {
                    window.open(url, '_blank');
                }
            }
        }
    };

    // Add the trace to the chart
    try {
        // Find existing YouTube trace and replace it, or add new one
        const existingTraceIndex = window.gd.data.findIndex(trace => trace.name === 'YouTube Videos');

        if (existingTraceIndex !== -1) {
            // Replace existing trace
            window.gd.data[existingTraceIndex] = youtubeTrace;
        } else {
            // Add new trace
            window.gd.data.push(youtubeTrace);
        }

        // Update the chart
        Plotly.react(chartElement, window.gd.data, window.gd.layout);

    } catch (error) {
        console.error('🎥 Combined WebSocket: Error adding YouTube marker trace:', error);
    }
}

function addYouTubeVideosToChart(videos, symbol) {
    // Legacy function - now just calls the new marker-based function
    addYouTubeVideosAsMarkers(videos, symbol);
}

function getYrefForSubplot(subplotName) {
    // Map subplot names to correct yref values
    // Format: "SYMBOL" for main chart, "SYMBOL-INDICATOR" for subplots

    if (!subplotName) {
        console.warn('🎨 DRAWINGS: No subplot_name provided, defaulting to main chart');
        return 'y';
    }

    // Extract symbol and indicator from subplot_name
    const parts = subplotName.split('-');
    const symbol = parts[0];
    const indicator = parts[1];

    if (!indicator) {
        // Main chart
        return 'y';
    }

    // Get user-selected indicators in their configured display order
    const selectedIndicators = getSelectedIndicators() || [];

    // If this indicator is not selected, default to main chart
    if (!selectedIndicators.includes(indicator)) {
        console.warn(`🎨 DRAWINGS: Indicator ${indicator} not found in selected indicators (${selectedIndicators.join(', ')}), defaulting to main chart`);
        return 'y';
    }

    // Find the index of this indicator in the selected indicators list
    const indicatorIndex = selectedIndicators.indexOf(indicator);

    if (indicatorIndex === -1) {
        console.warn(`🎨 DRAWINGS: Error finding index for indicator ${indicator}, defaulting to main chart`);
        return 'y';
    }

    // Return the correct yref (y2, y3, y4, etc.)
    const yref = `y${indicatorIndex + 2}`;
    return yref;
}

function getSelectedIndicators() {
    // Get user-selected indicators in their display order
    // First try combinedIndicators (set when WebSocket is connected)
    if (combinedIndicators && combinedIndicators.length > 0) {
        return combinedIndicators;
    }

    // Fallback: get currently active indicators from chart traces
    return getCurrentActiveIndicators();
}

function convertDrawingToShape(drawing) {
    try {

        // Determine the correct yref based on subplot_name
        const yref = getYrefForSubplot(drawing.subplot_name);

        // Basic shape properties
        const shape = {
            id: drawing.id,
            type: drawing.type || 'line',
            name: `drawing_${drawing.id}`,
            editable: true,
            layer: 'above',
            xref: 'x',  // Always use main x-axis
            yref: yref  // Use the correct y-axis based on subplot
        };

        // Convert coordinates based on drawing type
        if (drawing.type === 'line' || drawing.type === 'trendline') {
            shape.x0 = new Date(drawing.start_time * 1000);
            shape.y0 = drawing.start_price;
            shape.x1 = new Date(drawing.end_time * 1000);
            shape.y1 = drawing.end_price;
            shape.line = {
                color: drawing.properties?.color || 'blue',
                width: drawing.properties?.width || 2,
                dash: drawing.properties?.dash || 'solid'
            };

        } else if (drawing.type === 'rect' || drawing.type === 'rectangle' || drawing.type === 'box') {
            // For rectangles, we need to determine min/max coordinates
            const x0 = new Date(Math.min(drawing.start_time, drawing.end_time) * 1000);
            const x1 = new Date(Math.max(drawing.start_time, drawing.end_time) * 1000);
            const y0 = Math.min(drawing.start_price, drawing.end_price);
            const y1 = Math.max(drawing.start_price, drawing.end_price);

            shape.type = 'rect';
            shape.x0 = x0;
            shape.y0 = y0;
            shape.x1 = x1;
            shape.y1 = y1;
            shape.line = {
                color: drawing.properties?.color || 'red',
                width: drawing.properties?.width || 2
            };
            shape.fillcolor = drawing.properties?.fillcolor || 'rgba(255, 0, 0, 0.1)';
        } else if (drawing.type === 'horizontal_line' || drawing.type === 'hline') {
            shape.type = 'line';
            shape.x0 = 'x.min'; // Span entire x-axis
            shape.x1 = 'x.max';
            shape.y0 = drawing.start_price;
            shape.y1 = drawing.start_price;
            shape.xref = 'paper';
            shape.line = {
                color: drawing.properties?.color || 'green',
                width: drawing.properties?.width || 2,
                dash: drawing.properties?.dash || 'dash'
            };
            /*
                ...shape,
                yref: shape.yref
            });
            */
        } else {
            console.warn(`Combined WebSocket: Unsupported drawing type: ${drawing.type} for drawing id: ${drawing.id}`);
            return null;
        }

        return shape;
    } catch (error) {
        console.error('Combined WebSocket: Error converting drawing to shape:', error, drawing);
        return null;
    }
}

function convertYouTubeVideoToAnnotation(video, index) {
    try {


        // Parse the published_at timestamp (assuming it's ISO string or similar)
        let publishedTime;
        if (video.published_at) {
            publishedTime = new Date(video.published_at);
            if (isNaN(publishedTime.getTime())) {
                console.warn(`🎥 Combined WebSocket: Invalid published_at timestamp: ${video.published_at}`);
                publishedTime = new Date(); // Fallback to current time
            }
        } else {
            console.warn('🎥 Combined WebSocket: No published_at timestamp provided');
            publishedTime = new Date(); // Fallback
        }

        // Get current chart data to determine Y position
        // We want to place YouTube videos at the top of the price chart
        let chartHeight = null;
        if (window.gd && window.gd.layout && window.gd.layout.yaxis) {
            // For main price chart (yaxis), show near the top
            const yMax = window.gd.layout.yaxis.range ? window.gd.layout.yaxis.range[1] : null;

            if (yMax) {
                // Position at 95% of the Y range (near the top)
                chartHeight = yMax * 0.95;
            }
        }

        if (chartHeight === null) {
            // Fallback: use a reasonable default if chart height can't be determined
            chartHeight = 100; // Will be adjusted when chart data is available
        }

        // Create the annotation
        const annotation = {
            name: `youtube_${video.id}`,
            x: publishedTime,
            y: chartHeight,
            xref: 'x',
            yref: 'y',  // Main price chart Y-axis
            text: `<b>${video.title}</b><br>${video.channel_title}`,
            showarrow: true,
            arrowhead: 2,
            arrowcolor: '#ff0000',  // Red arrow for YouTube branding
            arrowsize: 1.5,
            arrowside: 'end+start',
            arrowwidth: 2,
            ax: 30 + (index * 10),  // Spread horizontally (30px + offset based on index)
            ay: -40 - (index * 15),  // Spread vertically upward (-40px - offset)
            font: {
                family: 'Arial, sans-serif',
                size: 12,
                color: '#000000'
            },
            bgcolor: '#ffffff',
            bordercolor: '#ff0000',  // Red border for YouTube
            borderwidth: 2,
            borderpad: 6,
            opacity: 0.9,
            align: 'left',
            width: 200,  // Fixed width for better readability
            height: 60   // Fixed height
        };



        return annotation;
    } catch (error) {
        console.error('🎥 Combined WebSocket: Error converting YouTube video to annotation:', error, video);
        return null;
    }
}


function updateChartWithHistoricalData(dataPoints, symbol) {
    console.log('📊 UPDATE_CHART: Called with', {
        dataPointsLength: dataPoints ? dataPoints.length : 'null/undefined',
        symbol: symbol,
        firstPoint: dataPoints && dataPoints[0] ? {
            time: dataPoints[0].time,
            hasOHLC: !!(dataPoints[0].ohlc),
            hasIndicators: !!(dataPoints[0].indicators)
        } : 'no first point'
    });

    // Define chartElement globally for the function
    const chartElement = document.getElementById('chart');

    if (!dataPoints || dataPoints.length === 0) {
        console.warn('⚠️ Combined WebSocket: No historical data points to process');
        return;
    }

    // Check if chart update is locked
    if (isChartUpdateLocked()) {
        console.warn('🔒 Chart update locked, queuing historical data update');
        // Queue the update for later
        setTimeout(() => updateChartWithHistoricalData(dataPoints, symbol), CHART_UPDATE_DEBOUNCE_DELAY);
        return;
    }

    // Validate data points before processing - handle both nested ohlc and flat formats
    const validDataPoints = dataPoints.filter(point => {
        if (!point.time || isNaN(point.time) || point.time <= 0) {
            console.warn(`⚠️ Invalid data point - missing or invalid time:`, {
                time: point.time,
                dataKeys: Object.keys(point)
            });
            return false;
        }

        // Check for OHLC data in either nested or flat format
        const hasNestedOHLC = point.ohlc && typeof point.ohlc === 'object';
        const hasFlatOHLC = point.open !== undefined && point.high !== undefined &&
                           point.low !== undefined && point.close !== undefined;

        if (!hasNestedOHLC && !hasFlatOHLC) {
            console.warn(`⚠️ Invalid data point - missing OHLC data:`, {
                time: point.time,
                hasNestedOHLC,
                hasFlatOHLC,
                dataKeys: Object.keys(point)
            });
            return false;
        }

        return true;
    });

    if (validDataPoints.length === 0) {
        console.error(`❌ No valid data points with timestamps and OHLC data found in ${dataPoints.length} total points`);
        return;
    }

    if (validDataPoints.length !== dataPoints.length) {
        console.warn(`⚠️ Filtered out ${dataPoints.length - validDataPoints.length} invalid data points`);
        dataPoints = validDataPoints; // Update the working dataset
    }

    // Extract OHLC data - handle both nested ohlc format and flat format
    const timestamps = dataPoints.map(point => new Date(point.time * 1000));
    const open = dataPoints.map(point => point.ohlc?.open ?? point.open ?? 0);
    const high = dataPoints.map(point => point.ohlc?.high ?? point.high ?? 0);
    const low = dataPoints.map(point => point.ohlc?.low ?? point.low ?? 0);
    const close = dataPoints.map(point => point.ohlc?.close ?? point.close ?? 0);
    const volume = dataPoints.map(point => point.ohlc?.volume ?? point.vol ?? 0);


    // Check for NaN values in OHLC data
    const ohlcNaNCount = [open, high, low, close].reduce((count, arr) => count + arr.filter(v => isNaN(v)).length, 0);
    if (ohlcNaNCount > 0) {
        console.warn(`🚨 WARNING: Found ${ohlcNaNCount} NaN values in OHLC data!`);
        return; // Don't proceed with invalid data
    }

    // Create custom data for enhanced hover info (buy signals)
    const customdata = timestamps.map((timestamp, index) => {
        const timestampKey = timestamp.getTime() / 1000; // Convert back to seconds for lookup
        const signalData = buySignalsByTimestamp[timestampKey.toString()];

        return signalData ? {
            hasBuySignal: true,
            rsi: signalData.rsi,
            rsi_sma14: signalData.rsi_sma14,
            deviation: signalData.deviation,
            sma_trend_up: signalData.sma_trend_up,
            dataIndex: index
        } : {
            hasBuySignal: false,
            dataIndex: index
        };
    });

    // Validate that we have the required OHLC data
    if (!timestamps.length || !open.length || !high.length || !low.length || !close.length) {
        console.error('❌ Missing required OHLC data arrays');
        return;
    }

    // Ensure all arrays have the same length
    const expectedLength = timestamps.length;
    if (open.length !== expectedLength || high.length !== expectedLength ||
        low.length !== expectedLength || close.length !== expectedLength) {
        console.error(`❌ OHLC array length mismatch: timestamps=${timestamps.length}, open=${open.length}, high=${high.length}, low=${low.length}, close=${close.length}`);
        return;
    }

    // Create main price trace with enhanced hover info
    const priceTrace = {
        x: timestamps,
        open: open,
        high: high,
        low: low,
        close: close,
        volume: volume,
        type: 'candlestick',
        xaxis: 'x',
        yaxis: 'y',
        name: symbol,
        increasing: { line: { color: 'green' } },
        decreasing: { line: { color: 'red' } },
        hoverinfo: isMobileDevice() ? 'skip' : 'all',
        customdata: customdata,
        hovertemplate: isMobileDevice() ? undefined : '%{customdata.hasBuySignal ? "<b>📈 BUY SIGNAL DETECTED</b><br>" : ""}' +
                      'Time: %{x}<br>' +
                      'Open: %{open:.6f}<br>' +
                      'High: %{high:.6f}<br>' +
                      'Low: %{low:.6f}<br>' +
                      'Close: %{close:.6f}<br>' +
                      '%{customdata.hasBuySignal ? "RSI: " + (customdata.rsi ? customdata.rsi.toFixed(2) : "N/A") + "<br>" : ""}' +
                      '%{customdata.hasBuySignal ? "RSI SMA14: " + (customdata.rsi_sma14 ? customdata.rsi_sma14.toFixed(2) : "N/A") + "<br>" : ""}' +
                      '%{customdata.hasBuySignal ? "Deviation: " + (customdata.deviation ? customdata.deviation.toFixed(2) : "N/A") + "<br>" : ""}' +
                      '%{customdata.hasBuySignal ? "SMA Trend Up: " + (customdata.sma_trend_up == 1 || customdata.sma_trend_up === true ? "Yes" : "No") + "<br>" : ""}' +
                      '<extra></extra>'
    };

    // Validate price trace before proceeding
    if (!priceTrace.x || priceTrace.x.length === 0) {
        console.error('❌ Price trace has no timestamps');
        return;
    }
    if (!priceTrace.open || !priceTrace.high || !priceTrace.low || !priceTrace.close) {
        console.error('❌ Price trace missing OHLC arrays');
        return;
    }

    // Create indicator traces
    const indicatorTraces = [];
    const indicatorsData = {};


    // Log the total indicator types found across all data points
    const allIndicatorKeys = new Set();
    dataPoints.forEach(point => {
        if (point.indicators) {
            Object.keys(point.indicators).forEach(key => allIndicatorKeys.add(key));
        }
    });

    // Find the earliest point that has ALL indicators available
    let firstCompletePointIndex = -1;
    let lastCompletePointIndex = -1;

    dataPoints.forEach((point, index) => {
        if (point.indicators && combinedIndicators.every(indicatorId => point.indicators[indicatorId])) {
            if (firstCompletePointIndex === -1) {
                firstCompletePointIndex = index;
            }
            lastCompletePointIndex = index;
        }
    });


    // Only process points that have indicators
    // FIX: Don't filter out points with missing indicators early in the dataset
    // This is normal for indicators during warmup periods
    const processedDataPoints = dataPoints.filter((point, index) =>
        // Only require OHLC data, indicators can be null/undefined
        point.ohlc && point.ohlc.open !== undefined && point.ohlc.close !== undefined
    );


    processedDataPoints.forEach((point, pointIndex) => {
        // Safe timestamp conversion with validation
        let validTimestamp = null;
        if (point.time && !isNaN(point.time) && point.time > 0) {
            try {
                validTimestamp = new Date(point.time * 1000);
                // Verify the date is valid
                if (isNaN(validTimestamp.getTime())) {
                    validTimestamp = null;
                }
            } catch(e) {
                validTimestamp = null;
            }
        }

        if (!validTimestamp) {
            console.warn(`⚠️ Invalid timestamp in dataPoint ${pointIndex}:`, point.time);
            return; // Skip this data point
        }

        if (point.indicators) {
            Object.keys(point.indicators).forEach(indicatorId => {
                if (!indicatorsData[indicatorId]) {
                    indicatorsData[indicatorId] = {
                        timestamps: [],
                        values: {}
                    };
                }

                indicatorsData[indicatorId].timestamps.push(validTimestamp);

                // Store all indicator values for this point
                Object.keys(point.indicators[indicatorId]).forEach(key => {
                    const value = point.indicators[indicatorId][key];

                    if (!indicatorsData[indicatorId].values[key]) {
                        indicatorsData[indicatorId].values[key] = [];
                    }
                    indicatorsData[indicatorId].values[key].push(value);
                });
            });
        }
    });



    // Detailed debug for CTO Line specifically
    if (indicatorsData.cto_line) {
        if (indicatorsData.cto_line.values) {
            Object.keys(indicatorsData.cto_line.values).forEach(key => {
                const valArray = indicatorsData.cto_line.values[key];
            });
        }
    } else {
    }

    // Create traces for each indicator with separate subplots
    // FORCE indicator order to match backend configuration
    const forcedIndicatorOrder = ['macd', 'rsi', 'stochrsi_9_3', 'stochrsi_14_3', 'stochrsi_40_4', 'stochrsi_60_10', 'open_interest', 'jma', 'cto_line'];
    const indicatorTypes = forcedIndicatorOrder.filter(indicatorId => combinedIndicators.includes(indicatorId));

    const subplotCount = indicatorTypes.length;






    indicatorTypes.forEach((indicatorId, index) => {
        const indicatorData = indicatorsData[indicatorId];
        if (!indicatorData) return; // Skip if no data for this indicator
        const yAxisName = `y${index + 2}`; // y2, y3, y4, etc.

        /*
        */

        if (indicatorId === 'macd' && indicatorData.values.macd && indicatorData.values.signal && indicatorData.values.histogram) {

            // Ensure we have valid data before processing
            if (indicatorData.values.macd.length === 0) {
                console.warn('⚠️ MACD: No MACD data points to process');
                return;
            }

            // MACD with signal and histogram - use data directly from Python backend

            // DEBUG: Log last 5 MACD data points being sent to Plotly
            const macdLast5 = indicatorData.values.macd.slice(-5);
            const signalLast5 = indicatorData.values.signal.slice(-5);
            const histogramLast5 = indicatorData.values.histogram.slice(-5);
            const macdTimestampsLast5 = indicatorData.timestamps.slice(-5);
            macdLast5.forEach((val, idx) => {
            });

            // Use the backend's timestamps for this indicator to maintain proper alignment
            indicatorTraces.push({
                x: indicatorData.timestamps,  // Use backend timestamps for proper alignment
                y: indicatorData.values.macd,
                type: 'scatter',
                mode: 'lines',
                name: 'MACD',
                line: { color: 'blue' },
                yaxis: yAxisName,
                hoverinfo: isMobileDevice() ? 'skip' : 'all',
                connectgaps: false  // Don't connect gaps to show natural indicator behavior
            });

            indicatorTraces.push({
                x: indicatorData.timestamps,  // Use backend timestamps for proper alignment
                y: indicatorData.values.signal,
                type: 'scatter',
                mode: 'lines',
                name: 'MACD Signal',
                line: { color: 'orange' },
                yaxis: yAxisName,
                hoverinfo: isMobileDevice() ? 'skip' : 'all',
                connectgaps: false  // Don't connect gaps to show natural indicator behavior
            });

            indicatorTraces.push({
                x: indicatorData.timestamps,  // Use backend timestamps for proper alignment
                y: indicatorData.values.histogram,
                type: 'bar',
                name: 'MACD Histogram',
                marker: {
                    color: indicatorData.values.histogram.map(v => v !== null && v >= 0 ? 'green' : 'red')
                },
                yaxis: yAxisName,
                hoverinfo: isMobileDevice() ? 'skip' : 'all',
                connectgaps: false  // Don't connect gaps to show natural indicator behavior
            });
        } else if (indicatorId === 'rsi' && indicatorData.values.rsi) {

            // RSI - use data directly from Python backend

            // DEBUG: Log last 5 RSI data points being sent to Plotly
            const rsiLast5 = indicatorData.values.rsi.slice(-5);
            const rsiTimestampsLast5 = indicatorData.timestamps.slice(-5);
            rsiLast5.forEach((val, idx) => {
            });

            // Use the backend's timestamps for this indicator to maintain proper alignment
            const rsiValues = indicatorData.values.rsi;

            // Check for null/undefined values that might cause display issues
            const rsiNullCount = rsiValues.filter(v => v === null || v === undefined).length;

            indicatorTraces.push({
                x: indicatorData.timestamps,  // Use backend timestamps for proper alignment
                y: rsiValues,
                type: 'scatter',
                mode: 'lines',
                name: 'RSI',
                line: { color: 'purple' },
                yaxis: yAxisName,
                hoverinfo: isMobileDevice() ? 'skip' : 'all',
                connectgaps: false  // Don't connect gaps - show natural indicator behavior
            });

            // Check for RSI_SMA14 and add it if available
            if (indicatorData.values.rsi_sma14) {

                const rsiSma14Values = indicatorData.values.rsi_sma14;

                indicatorTraces.push({
                    x: indicatorData.timestamps,  // Use backend timestamps for proper alignment
                    y: rsiSma14Values,
                    type: 'scatter',
                    mode: 'lines',
                    name: 'RSI_SMA14',
                    line: { color: 'dodgerblue' },
                    yaxis: yAxisName,
                    hoverinfo: isMobileDevice() ? 'skip' : 'all',
                    connectgaps: false  // Don't connect gaps - show natural indicator behavior
                });
            }
            } else if (indicatorId.startsWith('stochrsi') && indicatorData.values.stoch_k && indicatorData.values.stoch_d) {

                // Stochastic RSI - use data directly from Python backend

                // DEBUG: Log last 5 StochRSI data points being sent to Plotly
                const stochKLast5 = indicatorData.values.stoch_k.slice(-5);
                const stochDLast5 = indicatorData.values.stoch_d.slice(-5);
                const stochTimestampsLast5 = indicatorData.timestamps.slice(-5);
                stochKLast5.forEach((val, idx) => {
                });
                stochDLast5.forEach((val, idx) => {
                });

                // Extract variant parameters from indicatorId (e.g., 'stochrsi_14_3' -> '14,3')
                const variantMatch = indicatorId.match(/stochrsi_(\d+)_(\d+)/);
                const variantLabel = variantMatch ? `(${variantMatch[1]},${variantMatch[2]})` : '';

                // Use the backend's timestamps for this indicator to maintain proper alignment
                const kValues = indicatorData.values.stoch_k;
                const dValues = indicatorData.values.stoch_d;

                // Check for null/undefined values that might cause display issues
                const kNullCount = kValues.filter(v => v === null || v === undefined).length;
                const dNullCount = dValues.filter(v => v === null || v === undefined).length;


                indicatorTraces.push({
                    x: indicatorData.timestamps,  // Use backend timestamps for proper alignment
                    y: kValues,
                    type: 'scatter',
                    mode: 'lines',
                    name: `Stoch K ${variantLabel}`,
                    line: { color: 'blue' },
                    yaxis: yAxisName,
                    hoverinfo: isMobileDevice() ? 'skip' : 'all',
                    connectgaps: false  // Don't connect gaps - show natural indicator behavior
                });

                indicatorTraces.push({
                    x: indicatorData.timestamps,  // Use backend timestamps for proper alignment
                    y: dValues,
                    type: 'scatter',
                    mode: 'lines',
                    name: `Stoch D ${variantLabel}`,
                    line: { color: 'orange' },
                    yaxis: yAxisName,
                    hoverinfo: isMobileDevice() ? 'skip' : 'all',
                    connectgaps: false  // Don't connect gaps - show natural indicator behavior
                });
            } else if (indicatorId === 'cto_line') {

                // Check if we have the required data
                if (!indicatorData.values.cto_upper || !indicatorData.values.cto_lower) {
                    return;
                }


                // CTO Line (Larsson Line) - two SMMA lines with optional trend coloring

                // DEBUG: Log last 5 CTO Line data points being sent to Plotly
                const upperLast5 = indicatorData.values.cto_upper.slice(-5);
                const lowerLast5 = indicatorData.values.cto_lower.slice(-5);
                const trendLast5 = indicatorData.values.cto_trend && indicatorData.values.cto_trend.length > 0 ? indicatorData.values.cto_trend.slice(-5) : [];
                const ctoTimestampsLast5 = indicatorData.timestamps.slice(-5);
                upperLast5.forEach((val, idx) => {
                    const trend = trendLast5.length > idx ? trendLast5[idx] : 'N/A';
                });
                lowerLast5.forEach((val, idx) => {
                });

                // Use the backend's timestamps for this indicator to maintain proper alignment
                const upperValues = indicatorData.values.cto_upper;
                const lowerValues = indicatorData.values.cto_lower;

                // Check for null/undefined values that might cause display issues
                const upperNullCount = upperValues.filter(v => v === null || v === undefined).length;
                const lowerNullCount = lowerValues.filter(v => v === null || v === undefined).length;


                // Only create traces if we have at least some valid data points
                const upperValidCount = upperValues.filter(v => v !== null && v !== undefined && !isNaN(v)).length;
                const lowerValidCount = lowerValues.filter(v => v !== null && v !== undefined && !isNaN(v)).length;


                if (upperValidCount > 0 && lowerValidCount > 0) {
                    // Create CTO Upper line (fast SMMA) - same style as RSI
                    const upperTrace = {
                        x: indicatorData.timestamps,  // Use backend timestamps for proper alignment
                        y: upperValues,
                        type: 'scatter',
                        mode: 'lines',
                        name: 'CTO Upper',
                        line: { color: 'rgba(0, 255, 0, 0.8)', width: 2 }, // Green upper line (same as RSI style)
                        yaxis: yAxisName,
                        hoverinfo: isMobileDevice() ? 'skip' : 'all',
                        connectgaps: false  // Don't connect gaps - show natural indicator behavior (same as RSI)
                    };

                    // Create CTO Lower line (slow SMMA) - same style as RSI
                    const lowerTrace = {
                        x: indicatorData.timestamps,  // Use backend timestamps for proper alignment
                        y: lowerValues,
                        type: 'scatter',
                        mode: 'lines',
                        name: 'CTO Lower',
                        line: { color: 'rgba(255, 0, 0, 0.8)', width: 2 }, // Red lower line (same as RSI style)
                        yaxis: yAxisName,
                        hoverinfo: isMobileDevice() ? 'skip' : 'all',
                        connectgaps: false  // Don't connect gaps - show natural indicator behavior (same as RSI)
                    };

                    // For CTO line, we need to handle incremental updates differently
                    // unlike RSI which comes in complete batches, CTO might come in partial batches
                    // so we need to MERGE rather than REPLACE existing CTO traces

                    // Check if we already have CTO traces in the chart that need to be merged
                    const existingTraces = window.gd?.data || [];
                    const existingCTOUpperIndex = existingTraces.findIndex(t => t.name === 'CTO Upper');
                    const existingCTOLowerIndex = existingTraces.findIndex(t => t.name === 'CTO Lower');

                    if (existingCTOUpperIndex !== -1 && existingCTOLowerIndex !== -1) {
                        // CTO traces already exist - we need to merge the data

                        const existingUpper = existingTraces[existingCTOUpperIndex];
                        const existingLower = existingTraces[existingCTOLowerIndex];

                        // Merge x (timestamps) and y (values) data
                        const mergedUpper = mergeIndicatorData(existingUpper, upperTrace);
                        const mergedLower = mergeIndicatorData(existingLower, lowerTrace);

                        // Update the traces
                        upperTrace.x = mergedUpper.x;
                        upperTrace.y = mergedUpper.y;
                        lowerTrace.x = mergedLower.x;
                        lowerTrace.y = mergedLower.y;

                    }

                    indicatorTraces.push(upperTrace, lowerTrace);

                    // Store CTO traces globally for quick restoration when re-enabled
                    storedCTOTraces = [upperTrace, lowerTrace];

                } else {
                }

                // Optional: Add trend-based background coloring (bullish/bearish regions)
                // This would require creating fill areas, but for simplicity just show the two lines
            } else {
                // console.warn(`Combined WebSocket: Unknown or incomplete indicator data for ${indicatorId}`);
            }
    });

    // REPLACE PLACEHOLDER TRACES: Before WebSocket data arrives, remove any placeholder traces
    // These are marked with isplaceholder: true and will be replaced with real data
    const existingTracesForPlaceholders = window.gd?.data || [];
    const updatedTraces = existingTracesForPlaceholders.filter(trace => !trace.isplaceholder);

    // Preserve traces that are not price or indicators (trade markers, volume profiles, etc.)
    const preservedTraces = window.gd.data.filter(trace =>
        trace.type !== 'candlestick' &&
        !indicatorTraces.some(indicatorTrace => indicatorTrace.name === trace.name)
    );

    // Create new traces array with preserved traces + new price and indicator traces
    window.gd.data = [...preservedTraces, priceTrace, ...indicatorTraces];

    // The preserved traces (trade markers, volume profiles, etc.) remain in window.gd.data

    // Set the global traces array to the current data for consistent chart management
    globalTraces = [...window.gd.data];

    // DEBUG: Export data as CSV for analysis
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
                const filename = `plotly_data_${combinedSymbol}_${combinedResolution}_${new Date().getTime()}.csv`;
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


    // Check if we need to create a new layout or can reuse existing one
    let layout;
    if (window.gd && window.gd.layout && window.gd.layout.grid) {
        // Reuse existing layout and just update necessary properties
        layout = { ...window.gd.layout };

        // Update axis ranges if needed

        // Preserve user's saved X-axis range if it exists
        if (window.currentXAxisRange && window.currentXAxisRange.length === 2) {
            let xMinMs = window.currentXAxisRange[0];
            let xMaxMs = window.currentXAxisRange[1];

            if (xMinMs < 2e9) {
                xMinMs = xMinMs * 1000;
                xMaxMs = xMaxMs * 1000;
            }

            const xMinDate = new Date(xMinMs);
            const xMaxDate = new Date(xMaxMs);
            layout.xaxis = {
                ...layout.xaxis,
                rangeslider: { visible: false },
                type: 'date',
                autorange: false,
                range: [xMinDate, xMaxDate]
            };
        }

    } else {
        // Create new layout only if we don't have one
        layout = updateLayoutForIndicators(indicatorTypes, Object.keys(indicatorsData));
    }

    // Layout configuration is now handled above in the reuse/create logic

    // Call Plotly.react immediately after layout creation
    if (!chartElement) {
        console.error('Combined WebSocket: Chart element not found');
        return;
    }

    console.log('📊 UPDATE_CHART: About to call Plotly.react with', {
        tracesCount: window.gd.data.length,
        priceTraceIndex: window.gd.data.findIndex(t => t.type === 'candlestick'),
        indicatorTracesCount: window.gd.data.filter(t => t.type !== 'candlestick').length,
        hasLayout: !!layout
    });

    // Preserve existing shapes when updating chart with historical data
    if (window.gd && window.gd.layout && window.gd.layout.shapes) {
        layout.shapes = window.gd.layout.shapes;
    }

    // Set the global traces array to the current data for consistent chart management
    globalTraces = [...window.gd.data];


    // Acquire chart update lock
    if (!acquireChartUpdateLock()) {
        console.warn('🔒 Could not acquire chart update lock for historical data');
        return;
    }

    // Final validation before chart update
    if (!window.gd.data || window.gd.data.length === 0) {
        console.error('❌ No traces to display');
        releaseChartUpdateLock();
        return;
    }

    if (!layout) {
        console.error('❌ No layout configuration');
        releaseChartUpdateLock();
        return;
    }

    console.log('📊 UPDATE_CHART: Calling Plotly.react...');
    Plotly.react(chartElement, window.gd.data, layout).then(() => {
        console.log(`✅ Chart updated successfully with ${window.gd.data.length} traces`);

        // Re-add trade history markers after chart update if trade history data exists
        if (window.tradeHistoryData && window.tradeHistoryData.length > 0 && window.updateTradeHistoryVisualizations) {
            console.log('🔄 Re-adding trade history markers after historical data update');
            window.updateTradeHistoryVisualizations();
        }
    }).catch((error) => {
        console.error('❌ Error during Plotly.react:', error);
    }).finally(() => {
        // Always release the lock
        releaseChartUpdateLock();
        console.log('📊 UPDATE_CHART: Chart update completed');
    });


    // Set up WebSocket message handler after subplots are initialized
    try {
        setupWebSocketMessageHandler();
    } catch (error) {
        console.error('❌ Combined WebSocket: Failed to setup message handler in updateChartWithHistoricalData:', error);
    }

    // DEBUG: Check data range vs axis range
    if (timestamps.length > 0) {
        const dataMinTime = Math.min(...timestamps.map(t => t.getTime()));
        const dataMaxTime = Math.max(...timestamps.map(t => t.getTime()));
        const dataMinPrice = Math.min(...close);
        const dataMaxPrice = Math.max(...close);


        // Check if data is outside axis range - this could cause invisibility

        if (!layout.xaxis.autorange && layout.xaxis.range) {
            const axisMinTime = layout.xaxis.range[0].getTime();
            const axisMaxTime = layout.xaxis.range[1].getTime();


            const dataOutsideAxis = dataMinTime < axisMinTime || dataMaxTime > axisMaxTime;


            if (dataOutsideAxis) {
                // console.warn('🚨 WARNING: Data range extends beyond axis range!');
                // console.warn('  Data min:', new Date(dataMinTime).toISOString(), 'vs Axis min:', layout.xaxis.range[0].toISOString());
                // console.warn('  Data max:', new Date(dataMaxTime).toISOString(), 'vs Axis max:', layout.xaxis.range[1].toISOString());

                // Honor user's zoom/pan settings - do not auto-adjust axis range
            } else {
            }
        } else {
        }
    }



    // Update price trace to use y (not yaxis)
    priceTrace.yaxis = 'y';

    // Force Y-axis autoscale if no specific range is set
    if (!window.currentYAxisRange) {
        window.isApplyingAutoscale = true;
        Plotly.relayout(chartElement, { 'yaxis.autorange': true }).then(() => {
            window.isApplyingAutoscale = false;
        }).catch(() => {
            window.isApplyingAutoscale = false;
        });
    }

    // Verify event handlers are still attached after Plotly.react
    // CRITICAL FIX: Ensure scrollZoom is re-enabled after chart updates
    // This fixes the issue where mouse scrolling stops working after using pinch gestures or chart updates
    delay(100).then(() => {
        if (window.gd && window.gd._ev) {
            const relayoutHandlers = window.gd._ev._events?.plotly_relayout;
            if (!relayoutHandlers || relayoutHandlers.length === 0) {
                // console.warn('Combined WebSocket: Event handlers lost after Plotly.react - re-attaching...');
                if (typeof initializePlotlyEventHandlers === 'function') {
                    initializePlotlyEventHandlers(window.gd);
                }
            }
        }

        // Fix: Ensure scrollZoom is re-enabled after chart updates
        ensureScrollZoomEnabled();
    });
}

function updateChartWithLiveData(dataPoint, symbol) {
    if (!dataPoint) {
        console.warn('Combined WebSocket: No live data point to process');
        return;
    }

    // Safely extract live price for validation with comprehensive fallbacks
    let livePrice = null;

    // First try OHLC close price
    if (dataPoint.ohlc && typeof dataPoint.ohlc.close === 'number' && !isNaN(dataPoint.ohlc.close)) {
        livePrice = dataPoint.ohlc.close;
    }
    // Then try direct close property
    else if (typeof dataPoint.close === 'number' && !isNaN(dataPoint.close)) {
        livePrice = dataPoint.close;
    }
    // Then try direct price property
    else if (typeof dataPoint.price === 'number' && !isNaN(dataPoint.price)) {
        livePrice = dataPoint.price;
    }
    // Backend sometimes sends "live_price" property
    else if (typeof dataPoint.live_price === 'number' && !isNaN(dataPoint.live_price)) {
        livePrice = dataPoint.live_price;
    }

    if (livePrice === null || typeof livePrice !== 'number' || isNaN(livePrice)) {
        console.warn('🔴 Combined WebSocket: Invalid live price:', livePrice, {
            dataPointKeys: dataPoint ? Object.keys(dataPoint) : 'dataPoint is null/undefined',
            ohlcExists: !!(dataPoint && dataPoint.ohlc),
            hasClose: !!(dataPoint && dataPoint.close),
            hasPrice: !!(dataPoint && dataPoint.price),
            hasLivePrice: !!(dataPoint && dataPoint.live_price)
        });
        return;
    }

    const gd = document.getElementById('chart');
    if (!gd || !gd.data) {
        // console.warn('Combined WebSocket: Chart not ready for live update');
        return;
    }

    // Check if chart update is locked
    if (isChartUpdateLocked()) {
        console.warn('🔒 Chart update locked, queuing live data update');
        // Queue the latest live update, replacing any previous queued live update
        queuedLiveUpdate = dataPoint;
        if (!liveUpdateTimeout) {
            liveUpdateTimeout = setTimeout(() => {
                liveUpdateTimeout = null;
                const latestData = queuedLiveUpdate;
                queuedLiveUpdate = null;
                if (latestData) {
                    updateChartWithLiveData(latestData, symbol);
                }
            }, CHART_UPDATE_DEBOUNCE_DELAY);
        }
        return;
    }

    // Safe timestamp conversion with validation
    let timestamp;
    if (dataPoint.time && !isNaN(dataPoint.time) && dataPoint.time > 0) {
        try {
            timestamp = new Date(dataPoint.time * 1000);
            if (isNaN(timestamp.getTime())) {
                console.warn(`⚠️ Invalid timestamp in live data:`, dataPoint.time);
                return;
            }
        } catch(e) {
            console.warn(`⚠️ Error converting timestamp in live data:`, dataPoint.time);
            return;
        }
    } else {
        console.warn(`⚠️ Invalid or missing timestamp in live data:`, dataPoint.time);
        return;
    }

    // Safely extract price with fallback for different data structures
    const price = (dataPoint.ohlc && dataPoint.ohlc.close) ? dataPoint.ohlc.close :
                  (dataPoint.close !== undefined ? dataPoint.close :
                  (dataPoint.price !== undefined ? dataPoint.price :
                  (dataPoint.live_price !== undefined ? dataPoint.live_price : null)));

    // Find the candlestick trace
    const priceTraceIndex = gd.data.findIndex(trace => trace.type === 'candlestick');
    if (priceTraceIndex === -1) {
        // console.warn('Combined WebSocket: Could not find candlestick trace');
        return;
    }

    const trace = gd.data[priceTraceIndex];

    // Check if this is a new candle or update to existing
    const lastTimestamp = trace.x[trace.x.length - 1];
    const isNewCandle = timestamp.getTime() !== lastTimestamp.getTime();

    // Acquire chart update lock
    if (!acquireChartUpdateLock()) {
        console.warn('🔒 Could not acquire chart update lock for live data');
        return;
    }

    // DEBUG: Log trace update details
    /*
    console.log('[DEBUG] Live update for', {
        symbol: symbol || 'unknown',
        timestamp: timestamp.toISOString(),
        livePrice,
        isNewCandle,
        existingTraceType: trace.type,
        existingTracePoints: trace.x ? trace.x.length : 0,
        lastClose: trace.close ? trace.close[trace.close.length - 1] : 'N/A'
    });
    */

    try {
        if (isNewCandle) {
            // Add new candle
            const ohlc = dataPoint.ohlc || {};
            const newData = {
                x: [[timestamp]],
                open: [[ohlc.open || livePrice]],
                high: [[ohlc.high || livePrice]],
                low: [[ohlc.low || livePrice]],
                close: [[ohlc.close || livePrice]]
            };

            console.log('[DEBUG] Extending trace with new candle', newData);
            Plotly.extendTraces(gd, newData, [priceTraceIndex]);
        } else {
            // Update existing candle
            const ohlc = dataPoint.ohlc || {};

            // Store original values for comparison
            const originalHigh = trace.high[trace.high.length - 1];
            const originalLow = trace.low[trace.low.length - 1];
            const originalClose = trace.close[trace.close.length - 1];

            // Update high with consideration for live price
            let newHigh = originalHigh;
            if (ohlc.high !== undefined) {
                newHigh = Math.max(newHigh, ohlc.high);
            }
            // Always consider live price for high update
            newHigh = Math.max(newHigh, livePrice);
            trace.high[trace.high.length - 1] = newHigh;

            // Update low with consideration for live price
            let newLow = originalLow;
            if (ohlc.low !== undefined) {
                newLow = Math.min(newLow, ohlc.low);
            }
            // Always consider live price for low update
            newLow = Math.min(newLow, livePrice);
            trace.low[trace.low.length - 1] = newLow;

            // Update close - either use OHLC close if provided, or live price
            const newClose = ohlc.close !== undefined ? ohlc.close : livePrice;
            trace.close[trace.close.length - 1] = newClose;


            // Update the trace data arrays (this should directly affect the chart)
            trace.high[trace.high.length - 1] = newHigh;
            trace.low[trace.low.length - 1] = newLow;
            trace.close[trace.close.length - 1] = newClose;

            // Force a chart redraw by triggering a data update
            try {
                // Use Plotly.restyle to update the candlestick data
                const updateData = {
                    high: [trace.high],
                    low: [trace.low],
                    close: [trace.close]
                };

                Plotly.restyle(gd, updateData, [priceTraceIndex]).then(() => {
                }).catch((error) => {
                    console.warn('⚠️ Restyle failed, trying react fallback:', error);
                    // Fallback to Plotly.react
                    const layoutWithShapes = { ...gd.layout };
                    if (gd.layout && gd.layout.shapes) {
                        layoutWithShapes.shapes = gd.layout.shapes;
                    }
                    Plotly.react(gd, gd.data, layoutWithShapes);
                });
            } catch (error) {
                console.error('❌ Error during candlestick restyle:', error);
                // Final fallback
                const layoutWithShapes = { ...gd.layout };
                if (gd.layout && gd.layout.shapes) {
                    layoutWithShapes.shapes = gd.layout.shapes;
                }
                Plotly.react(gd, gd.data, layoutWithShapes);
            }

        }
    } finally {
        // Always release the lock
        releaseChartUpdateLock();
    }

    // Update price line if it exists
    // updateOrAddRealtimePriceLine(gd, price, timestamp.getTime(), timestamp.getTime() + (getTimeframeSecondsJS(combinedResolution) * 1000));

}

function closeCombinedWebSocket(reason = "Closing WebSocket") {
    logWebSocketEvent('close_requested', { reason });

    if (combinedWebSocket) {
        const wasConnected = combinedWebSocket.readyState === WebSocket.OPEN;
        const connectionId = websocketConnectionId;

        // Remove all event handlers to prevent any further processing
        combinedWebSocket.onopen = null;
        combinedWebSocket.onmessage = null;
        combinedWebSocket.onerror = null;
        combinedWebSocket.onclose = null;

        // Close the connection
        if (combinedWebSocket.readyState === WebSocket.OPEN || combinedWebSocket.readyState === WebSocket.CONNECTING) {
            combinedWebSocket.close(1000, reason);
        }

        logWebSocketEvent('connection_closed', {
            wasConnected,
            finalState: combinedWebSocket.readyState,
            connectionId
        });

        combinedWebSocket = window.combinedWebSocket = null;
    }

    // Reset all state variables
    combinedSymbol = '';
    combinedIndicators = [];
    combinedResolution = '1h';
    combinedFromTs = null;
    combinedToTs = null;

    // Clear message queue and processing state
    messageQueue = [];
    isProcessingMessage = false;

    // Clear processed message tracking
    processedMessageIds.clear();
    lastProcessedTimestampRange = null;

    // Reset connection flags
    isWebSocketConnecting = false;
    if (websocketSetupDebounceTimer) {
        clearTimeout(websocketSetupDebounceTimer);
        websocketSetupDebounceTimer = null;
    }

    logWebSocketEvent('state_reset', { reason });
}

function updateCombinedIndicators(newIndicators) {

    const oldIndicators = [...combinedIndicators];
    combinedIndicators = newIndicators;

    // Determine which indicators are new
    const newIndicatorIds = newIndicators.filter(ind => !oldIndicators.includes(ind));

    // Send new config via wsAPI
    if (window.wsAPI && window.wsAPI.connected) {
        sendCombinedConfig();
    }

    // Calculate new indicators client-side for immediate display
    if (newIndicatorIds.length > 0) {
        calculateIndicatorsClientSide(newIndicatorIds);
    }

    // Update chart display for removed indicators
    updateChartIndicatorsDisplay(oldIndicators, newIndicators);
}

function calculateIndicatorsClientSide(newIndicatorIds) {
    // Calculate indicators client-side using existing chart data for immediate display
    const chartElement = document.getElementById('chart');
    if (!chartElement || !window.gd || !window.gd.data) {
        return;
    }

    // Get existing OHLC data from the chart
    const priceTrace = window.gd.data.find(trace => trace.type === 'candlestick');
    if (!priceTrace) {
        return;
    }


    // Extract OHLC data
    const closes = priceTrace.close || [];
    const highs = priceTrace.high || [];
    const lows = priceTrace.low || [];
    const opens = priceTrace.open || [];
    const timestamps = priceTrace.x || [];

    if (closes.length === 0) {
        return;
    }

    // Calculate each new indicator
    const calculatedTraces = [];

    newIndicatorIds.forEach((indicatorId, index) => {
        try {
            // Calculate the correct y-axis for this indicator (correct order)
            const yAxisName = `y${index + 2}`; // y2, y3, y4, etc. in correct order

            const traces = calculateIndicator(indicatorId, timestamps, opens, highs, lows, closes, yAxisName);
            calculatedTraces.push(...traces);
        } catch (error) {
            // console.error(`Combined WebSocket: Error calculating ${indicatorId}:`, error);
        }
    });

    if (calculatedTraces.length > 0) {
        // Add calculated traces to the chart
        const updatedData = [...window.gd.data, ...calculatedTraces];
        const updatedLayout = updateLayoutForIndicators([...new Set([...getCurrentActiveIndicators(), ...newIndicatorIds])], getCurrentActiveIndicators());

        Plotly.react(chartElement, updatedData, updatedLayout);
    }
}

function getCurrentActiveIndicators() {
    // Get indicators that currently have traces in the chart
    if (!window.gd || !window.gd.data) return [];

    const indicators = [];
    window.gd.data.forEach(trace => {
        if (!trace.name || typeof trace.name !== 'string') return; // Skip traces without valid string names

        if (trace.name === 'MACD' || trace.name === 'MACD Signal' || trace.name === 'MACD Histogram') {
            if (!indicators.includes('macd')) indicators.push('macd');
        } else if (trace.name === 'RSI' || trace.name === 'RSI_SMA14') {
            if (!indicators.includes('rsi')) indicators.push('rsi');
        } else if (trace.name.startsWith('Stoch K') || trace.name.startsWith('Stoch D')) {
            // Extract variant from trace name (e.g., 'Stoch K (14,3)' -> 'stochrsi_14_3')
            const variantMatch = trace.name.match(/Stoch [KD] \((\d+),(\d+)\)/);
            if (variantMatch) {
                const stochVariant = `stochrsi_${variantMatch[1]}_${variantMatch[2]}`;
                if (!indicators.includes(stochVariant)) indicators.push(stochVariant);
            } else {
                // Fallback to first stochrsi variant if no variant found
                const stochVariant = window.combinedIndicators?.find(id => id.startsWith('stochrsi'));
                if (stochVariant && !indicators.includes(stochVariant)) indicators.push(stochVariant);
            }
        } else if (trace.name === 'CTO Upper' || trace.name === 'CTO Lower') {
            if (!indicators.includes('cto_line')) indicators.push('cto_line');
        } else if (trace.name === 'JMA') {
            if (!indicators.includes('jma')) indicators.push('jma');
        } else if (trace.name === 'Open Interest') {
            if (!indicators.includes('open_interest')) indicators.push('open_interest');
        }
    });

    return indicators;
}

function calculateIndicator(indicatorId, timestamps, opens, highs, lows, closes, yAxisName) {
    const traces = [];

    switch (indicatorId) {
        case 'rsi':
            const rsiValues = calculateRSI(closes, 14);
            if (rsiValues.length > 0) {
                traces.push({
                    x: timestamps.slice(-rsiValues.length),
                    y: rsiValues,
                    type: 'scatter',
                    mode: 'lines',
                    name: 'RSI',
                    line: { color: 'purple' },
                    xaxis: 'x',
                    yaxis: yAxisName,
                    hoverinfo: 'all'
                });
            }
            break;

        case 'macd':
            const macdResult = calculateMACD(closes);
            if (macdResult.macd.length > 0) {
                traces.push({
                    x: timestamps.slice(-macdResult.macd.length),
                    y: macdResult.macd,
                    type: 'scatter',
                    mode: 'lines',
                    name: 'MACD',
                    line: { color: 'blue' },
                    xaxis: 'x',
                    yaxis: yAxisName,
                    hoverinfo: 'all'
                });

                if (macdResult.signal.length > 0) {
                    traces.push({
                        x: timestamps.slice(-macdResult.signal.length),
                        y: macdResult.signal,
                        type: 'scatter',
                        mode: 'lines',
                        name: 'MACD Signal',
                        line: { color: 'orange' },
                        xaxis: 'x',
                        yaxis: yAxisName,
                        hoverinfo: 'all'
                    });
                }

                if (macdResult.histogram.length > 0) {
                    traces.push({
                        x: timestamps.slice(-macdResult.histogram.length),
                        y: macdResult.histogram,
                        type: 'bar',
                        name: 'MACD Histogram',
                        marker: {
                            color: macdResult.histogram.map(v => v >= 0 ? 'green' : 'red')
                        },
                        xaxis: 'x',
                        yaxis: yAxisName,
                        hoverinfo: 'all'
                    });
                }
            }
            break;

        default:
    }

    return traces;
}

function calculateRSI(closes, period = 14) {
    if (closes.length < period + 1) return [];

    const gains = [];
    const losses = [];

    for (let i = 1; i < closes.length; i++) {
        const change = closes[i] - closes[i - 1];
        gains.push(change > 0 ? change : 0);
        losses.push(change < 0 ? Math.abs(change) : 0);
    }

    const ema = [];
    const multiplier = 2 / (period + 1);

    // First EMA value is SMA
    let sum = 0;
    for (let i = 0; i < period; i++) {
        sum += values[i];
    }
    ema.push(sum / period);

    // Calculate remaining EMA values
    for (let i = period; i < values.length; i++) {
        const currentEMA = (values[i] - ema[ema.length - 1]) * multiplier + ema[ema.length - 1];
        ema.push(currentEMA);
    }

    return ema;
}

function calculateMACD(closes, fastPeriod = 12, slowPeriod = 26, signalPeriod = 9) {
    const fastEMA = calculateEMA(closes, fastPeriod);
    const slowEMA = calculateEMA(closes, slowPeriod);

    const macd = [];
    const minLength = Math.min(fastEMA.length, slowEMA.length);

    for (let i = 0; i < minLength; i++) {
        macd.push(fastEMA[i] - slowEMA[i]);
    }

    const signal = calculateEMA(macd, signalPeriod);
    const histogram = [];

    const histMinLength = Math.min(macd.length, signal.length);
    for (let i = 0; i < histMinLength; i++) {
        histogram.push(macd[i] - signal[i]);
    }

    return { macd, signal, histogram };
}

function calculateEMA(values, period) {
    if (values.length < period) return [];

    const rsi = [];
    let avgGain = gains.slice(0, period).reduce((sum, gain) => sum + gain, 0) / period;
    let avgLoss = losses.slice(0, period).reduce((sum, loss) => sum + loss, 0) / period;

    if (avgLoss === 0) {
        rsi.push(100);
    } else {
        const rs = avgGain / avgLoss;
        rsi.push(100 - (100 / (1 + rs)));
    }

    for (let i = period; i < gains.length; i++) {
        avgGain = (avgGain * (period - 1) + gains[i]) / period;
        avgLoss = (avgLoss * (period - 1) + losses[i]) / period;

        if (avgLoss === 0) {
            rsi.push(100);
        } else {
            const rs = avgGain / avgLoss;
            rsi.push(100 - (100 / (1 + rs)));
        }
    }

    return rsi;
}

function calculateMACD(closes, fastPeriod = 12, slowPeriod = 26, signalPeriod = 9) {
    const fastEMA = calculateEMA(closes, fastPeriod);
    const slowEMA = calculateEMA(closes, slowPeriod);

    const macd = [];
    const minLength = Math.min(fastEMA.length, slowEMA.length);

    for (let i = 0; i < minLength; i++) {
        macd.push(fastEMA[i] - slowEMA[i]);
    }

    const signal = calculateEMA(macd, signalPeriod);
    const histogram = [];

    const histMinLength = Math.min(macd.length, signal.length);
    for (let i = 0; i < histMinLength; i++) {
        histogram.push(macd[i] - signal[i]);
    }

    return { macd, signal, histogram };
}

function calculateEMA(values, period) {
    if (values.length < period) return [];

    const ema = [];
    const multiplier = 2 / (period + 1);

    // First EMA value is SMA
    let sum = 0;
    for (let i = 0; i < period; i++) {
        sum += values[i];
    }
    ema.push(sum / period);

    // Calculate remaining EMA values
    for (let i = period; i < values.length; i++) {
        const currentEMA = (values[i] - ema[ema.length - 1]) * multiplier + ema[ema.length - 1];
        ema.push(currentEMA);
    }

    return ema;
}

function updateChartIndicatorsDisplay(oldIndicators, newIndicators) {
    const chartElement = document.getElementById('chart');
    if (!chartElement || !window.gd || !window.gd.data) {
        return;
    }

    const currentData = window.gd.data;
    if (!currentData || currentData.length === 0) {
        return;
    }


    // Find the price trace (candlestick)
    const priceTraceIndex = currentData.findIndex(trace => trace.type === 'candlestick');
    if (priceTraceIndex === -1) {
        console.warn('Combined WebSocket: Could not find price trace for indicator update');
        return;
    }

    const priceTrace = currentData[priceTraceIndex];
    const currentIndicatorTraces = currentData.slice(1); // All traces except the first (price)

    // Determine which indicators to add/remove
    const indicatorsToAdd = newIndicators.filter(ind => !oldIndicators.includes(ind));
    const indicatorsToRemove = oldIndicators.filter(ind => !newIndicators.includes(ind));


    // Start with price trace
    let updatedTraces = [priceTrace];

    // Group traces by indicator type and assign y-axes
    const tracesByIndicator = {};

    // DEBUG: Log which indicators are being added vs kept

    // First pass: group traces by indicator
    currentIndicatorTraces.forEach(trace => {
        let traceIndicatorId = null;
        if (trace.name === 'MACD' || trace.name === 'MACD Signal' || trace.name === 'MACD Histogram') {
            traceIndicatorId = 'macd';
        } else if (trace.name === 'RSI' || trace.name === 'RSI_SMA14') {
            traceIndicatorId = 'rsi';
        } else if (trace.name.startsWith('Stoch K') || trace.name.startsWith('Stoch D')) {
            // Extract variant from trace name (e.g., 'Stoch K (14,3)' -> 'stochrsi_14_3')
            const variantMatch = trace.name.match(/Stoch [KD] \((\d+),(\d+)\)/);
            if (variantMatch) {
                traceIndicatorId = `stochrsi_${variantMatch[1]}_${variantMatch[2]}`;
            } else {
                // Fallback to first stochrsi variant if no variant found
                traceIndicatorId = newIndicators.find(id => id.startsWith('stochrsi'));
            }
        } else if (trace.name === 'CTO Upper' || trace.name === 'CTO Lower') {
            traceIndicatorId = 'cto_line';
        } else if (trace.name === 'JMA') {
            traceIndicatorId = 'jma';
        } else if (trace.name === 'Open Interest') {
            traceIndicatorId = 'open_interest';
        }

        if (traceIndicatorId && newIndicators.includes(traceIndicatorId)) {
            if (!tracesByIndicator[traceIndicatorId]) {
                tracesByIndicator[traceIndicatorId] = [];
            }
            tracesByIndicator[traceIndicatorId].push(trace);
        } else {
        }
    });

    // FORCE indicator order to match backend configuration (CTO Line last)
    const forcedIndicatorOrder = ['macd', 'rsi', 'stochrsi_9_3', 'stochrsi_14_3', 'stochrsi_40_4', 'stochrsi_60_10', 'open_interest', 'jma', 'cto_line'];
    const activeIndicatorIds = forcedIndicatorOrder.filter(indicatorId => newIndicators.includes(indicatorId));


    // Second pass: assign y-axes based on active indicators order
    activeIndicatorIds.forEach((indicatorId, index) => {
        const yAxisName = `y${index + 2}`; // y2, y3, y4, etc. in correct order
        const indicatorTraces = tracesByIndicator[indicatorId] || [];


        if (indicatorTraces.length > 0) {
            // Assign existing traces to their y-axis
            indicatorTraces.forEach(trace => {
                trace.yaxis = yAxisName;
                updatedTraces.push(trace);
            });
        } else {
            // SPECIAL HANDLING: For CTO line when being re-enabled, restore stored traces if available
            if (indicatorId === 'cto_line' && indicatorsToAdd.includes('cto_line')) {
                if (storedCTOTraces && storedCTOTraces.length === 2) {

                    // Restore the stored CTO traces and assign correct y-axis
                    const restoredUpper = { ...storedCTOTraces[0], yaxis: yAxisName };
                    const restoredLower = { ...storedCTOTraces[1], yaxis: yAxisName };

                    updatedTraces.push(restoredUpper, restoredLower);
                } else {

                    // Create minimal placeholder traces for CTO line with empty data
                    // These will be replaced when actual WebSocket data arrives
                    const placeholderCTOUpper = {
                        x: [], // Empty array initially
                        y: [], // Empty array initially
                        type: 'scatter',
                        mode: 'lines',
                        name: 'CTO Upper',
                        line: { color: 'rgba(0, 255, 0, 0.8)', width: 2 },
                        yaxis: yAxisName,
                        hoverinfo: 'skip',
                        connectgaps: false,
                        isplaceholder: true // Mark as placeholder
                    };

                    const placeholderCTOLower = {
                        x: [], // Empty array initially
                        y: [], // Empty array initially
                        type: 'scatter',
                        mode: 'lines',
                        name: 'CTO Lower',
                        line: { color: 'rgba(255, 0, 0, 0.8)', width: 2 },
                        yaxis: yAxisName,
                        hoverinfo: 'skip',
                        connectgaps: false,
                        isplaceholder: true // Mark as placeholder
                    };

                    updatedTraces.push(placeholderCTOUpper);
                    updatedTraces.push(placeholderCTOLower);
                }
            } else {
            }
        }
    });


    // Create layout for updated traces with correct domain ordering
    const layout = updateLayoutForIndicators(activeIndicatorIds, Object.keys(tracesByIndicator));

    // Preserve existing shapes when updating indicators
    if (window.gd && window.gd.layout && window.gd.layout.shapes) {
        layout.shapes = window.gd.layout.shapes;
    }

    // Update the chart layout immediately
    // This call creates new layout domains for the updated active indicators
    Plotly.react(chartElement, updatedTraces, layout);

    // Re-add trade history markers after indicator update
    if (window.tradeHistoryData && window.tradeHistoryData.length > 0 && window.updateTradeHistoryVisualizations) {
        window.updateTradeHistoryVisualizations();
    }

    // NOTE: updateCombinedIndicators already called this function, avoid recursive call

    // CRITICAL: Immediately notify backend and wait for indicator data response
    if (window.wsAPI && window.wsAPI.connected) {
        sendCombinedConfig();
    } else {
        console.warn('🎯 INDICATOR DISPLAY UPDATE: WebSocket not available - reloading chart');
        // Fallback: Reload chart with available data
        if (combinedSymbol) {
            setupCombinedWebSocket(combinedSymbol, newIndicators, combinedResolution, combinedFromTs, combinedToTs);
        }
    }

}

function updateLayoutForIndicators(activeIndicatorIds, indicatorsWithData = []) {
    // Populate active_indicatorsState with the correct yAxisRef mapping
    window.populateActiveIndicatorsState(activeIndicatorIds);

    // Calculate price chart height here, before creating the layout
    let priceChartHeight = 1;
    if (activeIndicatorIds.length > 0) {
        const numIndicators = activeIndicatorIds.length;
        const priceChartProportion = 3; // Price chart is 3 parts
        const totalProportions = priceChartProportion + numIndicators;
        priceChartHeight = priceChartProportion / totalProportions;
    }

    const baseLayout = {
        // Remove fixed height to allow full viewport height
        autosize: true, // Enable autosizing
        dragmode: 'pan', // Set default dragmode to pan for chart navigation
        xaxis: {
            rangeslider: { visible: false },
            type: 'date',
            autorange: !window.currentXAxisRange,
            range: window.currentXAxisRange ? [
                new Date(window.currentXAxisRange[0] < 2e9 ? window.currentXAxisRange[0] * 1000 : window.currentXAxisRange[0]),
                new Date(window.currentXAxisRange[1] < 2e9 ? window.currentXAxisRange[1] * 1000 : window.currentXAxisRange[1])
            ] : undefined,
            showticklabels: true,
            side: 'bottom',
            minor: {
                ticks: 'inside',
                ticklen: 4,
                tickcolor: 'rgba(0,0,0,0.3)',
                showgrid: false
            }
        },
        yaxis: {
            title: `${combinedSymbol.replace('USDT', '/USDT')} Price`,
            autorange: !window.currentYAxisRange,
            range: window.currentYAxisRange ? window.currentYAxisRange : undefined,
            showticklabels: true,
            showgrid: true,
            side: 'left',
            // Price chart should be at the BOTTOM, so its domain starts from 0
            domain: activeIndicatorIds.length > 0 ? [0, priceChartHeight] : [0, 1]
        },
        showlegend: false,
        hovermode: 'x unified',
        margin: { l: 50, r: 10, b: 80, t: 30 } // Reduced top margin for tighter layout
    };

    if (activeIndicatorIds.length > 0) {
        // Price chart is twice the height of each individual indicator chart
        const numIndicators = activeIndicatorIds.length;
        const priceChartProportion = 3; // Price chart is 3 parts
        const totalProportions = priceChartProportion + numIndicators;

        const priceChartHeight = priceChartProportion / totalProportions;
        const indicatorHeight = 1 / totalProportions;

        // Create grid with manual row heights
        let rowHeights = [priceChartHeight];
        for (let i = 0; i < numIndicators; i++) {
            rowHeights.push(indicatorHeight);
        }


        baseLayout.grid = {
            rows: rowHeights.length,
            columns: 1,
            pattern: 'independent',
            roworder: 'top to bottom',
            rowheights: rowHeights,
            vertical_spacing: 0.05
        };


        // Add background colors to even indicators
        baseLayout.shapes = baseLayout.shapes || [];

        // Add separator lines between indicators
        // Use the same domain calculation as the y-axes
        const priceDomainEnd = priceChartProportion / totalProportions;

        for (let i = 0; i < numIndicators - 1; i++) {
            const linePosition = priceDomainEnd + ((i + 1) * indicatorHeight);
            baseLayout.shapes.push({
                type: 'line',
                xref: 'paper',
                yref: 'paper',
                x0: 0,
                y0: linePosition,
                x1: 1,
                y1: linePosition,
                line: { color: 'rgba(0, 0, 0, 0.92)', width: 1 },
                layer: 'below'
            });
        }

        // Set manual domains for each y-axis to ensure proper height allocation
        activeIndicatorIds.forEach((indicatorId, index) => {
            const yAxisName = `yaxis${index + 2}`;
            const xAxisName = `xaxis${index + 2}`;
            const indicatorIndex = index; // 0-based index for indicators
            const totalIndicators = activeIndicatorIds.length;

            // Calculate domain for this indicator
            // Price chart takes first portion, then each indicator gets equal share of remaining space
            const priceDomainEnd = priceChartProportion / totalProportions;
            const indicatorDomainStart = priceDomainEnd + (indicatorIndex * indicatorHeight);
            const indicatorDomainEnd = priceDomainEnd + ((indicatorIndex + 1) * indicatorHeight);

            // DEBUG: Log subplot coordinates and domain calculations
            console.log(`📊 SUBPLOT COORDINATES: ${indicatorId} (${yAxisName})`);
            console.log(`   Indicator index: ${indicatorIndex}/${totalIndicators}`);
            console.log(`   Price chart proportion: ${priceChartProportion}, Total proportions: ${totalProportions}`);
            console.log(`   Price domain end: ${priceDomainEnd.toFixed(4)}`);
            console.log(`   Indicator height: ${indicatorHeight.toFixed(4)}`);
            console.log(`   Domain start: ${indicatorDomainStart.toFixed(4)}, Domain end: ${indicatorDomainEnd.toFixed(4)}`);
            console.log(`   Domain range: [${indicatorDomainStart.toFixed(4)}, ${indicatorDomainEnd.toFixed(4)}]`);

            // Ensure subplot title is always set
            const titleText = (() => {
                const displayNames = {
                    'macd': 'MACD',
                    'rsi': 'RSI',
                    'stochrsi_9_3': 'StochRSI (9,3)',
                    'stochrsi_14_3': 'StochRSI (14,3)',
                    'stochrsi_40_4': 'StochRSI (40,4)',
                    'stochrsi_60_10': 'StochRSI (60,10)',
                    'jma': 'JMA',
                    'open_interest': 'Open Interest',
                    'cto_line': 'CTO'
                };
                const displayName = displayNames[indicatorId] || indicatorId.toUpperCase();
                // Safeguard: Ensure we always have a non-empty title
                const finalTitle = displayName && displayName.trim() ? displayName : `Indicator ${indicatorId}`;
                console.log(`📊 SUBPLOT TITLE: Setting title "${finalTitle}" for ${yAxisName} (indicator: ${indicatorId})`);
                return finalTitle;
            })();

            baseLayout[yAxisName] = {
                title: {
                    text: titleText,
                    standoff: 20, // Increased from 8 to 20 for longer titles like "Open Interest"
                    font: { size: 8 }
                },
                autorange: true,
                fixedrange: false, // Allow zoom on indicator y-axes
                domain: [indicatorDomainStart, indicatorDomainEnd], // Set explicit domain
                side: 'left'
            };

            // Create separate x-axis for each indicator subplot (but hide the labels)
            baseLayout[xAxisName] = {
                rangeslider: { visible: false },
                type: 'date',
                autorange: !window.currentXAxisRange,
                range: window.currentXAxisRange ? [
                    new Date(window.currentXAxisRange[0] < 2e9 ? window.currentXAxisRange[0] * 1000 : window.currentXAxisRange[0]),
                    new Date(window.currentXAxisRange[1] < 2e9 ? window.currentXAxisRange[1] * 1000 : window.currentXAxisRange[1])
                ] : undefined,
                showticklabels: false, // Hide x-axis labels for indicator subplots
                anchor: yAxisName, // Anchor to the corresponding y-axis
                overlaying: 'x' // CRITICAL FIX: Overlay on the main x-axis
            };

        });

        // Domain coverage handled by explicit domain settings for each y-axis
    } else {
        // No indicators - let Plotly handle domain automatically
    }


    // Update the plotly chart layout without removing data
    if (window.gd && window.gd.data) {
        Plotly.relayout(window.gd, baseLayout);
    }

    return baseLayout;
}



function updateChartWithLiveData(dataPoint, symbol) {
    if (!dataPoint) {
        console.warn('Combined WebSocket: No live data point to process');
        return;
    }

    // Safely extract live price for validation with comprehensive fallbacks
    let livePrice = null;

    // First try OHLC close price
    if (dataPoint.ohlc && typeof dataPoint.ohlc.close === 'number' && !isNaN(dataPoint.ohlc.close)) {
        livePrice = dataPoint.ohlc.close;
    }
    // Then try direct close property
    else if (typeof dataPoint.close === 'number' && !isNaN(dataPoint.close)) {
        livePrice = dataPoint.close;
    }
    // Then try direct price property
    else if (typeof dataPoint.price === 'number' && !isNaN(dataPoint.price)) {
        livePrice = dataPoint.price;
    }
    // Backend sometimes sends "live_price" property
    else if (typeof dataPoint.live_price === 'number' && !isNaN(dataPoint.live_price)) {
        livePrice = dataPoint.live_price;
    }

    if (livePrice === null || typeof livePrice !== 'number' || isNaN(livePrice)) {
        console.warn('🔴 Combined WebSocket: Invalid live price:', livePrice, {
            dataPointKeys: dataPoint ? Object.keys(dataPoint) : 'dataPoint is null/undefined',
            ohlcExists: !!(dataPoint && dataPoint.ohlc),
            ohlcKeys: dataPoint && dataPoint.ohlc ? Object.keys(dataPoint.ohlc) : null,
            hasClose: !!(dataPoint && dataPoint.close),
            closeType: dataPoint && dataPoint.close ? typeof dataPoint.close : null,
            hasPrice: !!(dataPoint && dataPoint.price),
            priceType: dataPoint && dataPoint.price ? typeof dataPoint.price : null
        });
        return;
    }

    const gd = document.getElementById('chart');
    if (!gd || !gd.data) {
        // console.warn('Combined WebSocket: Chart not ready for live update');
        return;
    }

    // Check if chart update is locked
    if (isChartUpdateLocked()) {
        console.warn('🔒 Chart update locked, queuing live data update');
        // Queue the latest live update, replacing any previous queued live update
        queuedLiveUpdate = dataPoint;
        if (!liveUpdateTimeout) {
            liveUpdateTimeout = setTimeout(() => {
                liveUpdateTimeout = null;
                const latestData = queuedLiveUpdate;
                queuedLiveUpdate = null;
                if (latestData) {
                    updateChartWithLiveData(latestData, symbol);
                }
            }, CHART_UPDATE_DEBOUNCE_DELAY);
        }
        return;
    }

    // Safe timestamp conversion with validation
    let timestamp;
    if (dataPoint.time && !isNaN(dataPoint.time) && dataPoint.time > 0) {
        try {
            timestamp = new Date(dataPoint.time * 1000);
            if (isNaN(timestamp.getTime())) {
                console.warn(`⚠️ Invalid timestamp in live data:`, dataPoint.time);
                return;
            }
        } catch(e) {
            console.warn(`⚠️ Error converting timestamp in live data:`, dataPoint.time);
            return;
        }
    } else {
        console.warn(`⚠️ Invalid or missing timestamp in live data:`, dataPoint.time);
        return;
    }

    // Safely extract price with fallback for different data structures
    const price = (dataPoint.ohlc && dataPoint.ohlc.close) ? dataPoint.ohlc.close :
                  (dataPoint.close !== undefined ? dataPoint.close :
                  (dataPoint.price !== undefined ? dataPoint.price :
                  (dataPoint.live_price !== undefined ? dataPoint.live_price : null)));

    // Find the candlestick trace
    const priceTraceIndex = gd.data.findIndex(trace => trace.type === 'candlestick');
    if (priceTraceIndex === -1) {
        // console.warn('Combined WebSocket: Could not find candlestick trace');
        return;
    }

    const trace = gd.data[priceTraceIndex];

    // ALWAYS update the latest candle - no timestamp checking
    // This ensures live price updates always affect the most recent candle

    // Acquire chart update lock
    if (!acquireChartUpdateLock()) {
        console.warn('🔒 Could not acquire chart update lock for live data');
        return;
    }

    // Check if this timestamp should create a new candle or if we have data
    const hasExistingCandles = trace.x && trace.x.length > 0;
    const lastTimestamp = hasExistingCandles ? trace.x[trace.x.length - 1] : null;

    // DEBUG: Log trace update details
    /*
    console.log('[DEBUG] Live update for', {
        symbol: symbol || 'unknown',
        timestamp: timestamp.toISOString(),
        livePrice,
        hasExistingCandles,
        existingTraceType: trace.type,
        existingTracePoints: trace.x ? trace.x.length : 0,
        lastClose: trace.close ? trace.close[trace.close.length - 1] : 'N/A'
    });
    */

    try {
        if (!hasExistingCandles) {
            // No candles yet - create first candle
            console.log('[DEBUG] Creating first candle from live data');
            const ohlc = dataPoint.ohlc || {};
            trace.x.push(timestamp);
            trace.open.push(ohlc.open || livePrice);
            trace.high.push(ohlc.high || livePrice);
            trace.low.push(ohlc.low || livePrice);
            trace.close.push(ohlc.close || livePrice);
            trace.volume = trace.volume || [];
            trace.volume.push(ohlc.volume || 0);

            const updateData = {
                x: [trace.x],
                open: [trace.open],
                high: [trace.high],
                low: [trace.low],
                close: [trace.close],
                volume: [trace.volume]
            };

            Plotly.restyle(gd, updateData, [priceTraceIndex]).then(() => {
                console.log('[DEBUG] First candle created successfully:', timestamp.toISOString());
                // Re-add trade history markers after live data update
                if (window.tradeHistoryData && window.tradeHistoryData.length > 0 && window.updateTradeHistoryVisualizations) {
                    window.updateTradeHistoryVisualizations();
                }
            }).catch((error) => {
                console.warn('⚠️ Restyle failed for first candle:', error);
            });

        } else {
            // ALWAYS update the latest (most recent) candle
            // This ensures live price updates are always visible on the current candle
            const ohlc = dataPoint.ohlc || {};

            // Store original values for comparison and updates
            const originalHigh = trace.high[trace.high.length - 1];
            const originalLow = trace.low[trace.low.length - 1];
            const originalClose = trace.close[trace.close.length - 1];

            // Update high with consideration for live price
            let newHigh = originalHigh;
            if (ohlc.high !== undefined) {
                newHigh = Math.max(newHigh, ohlc.high);
            }
            // Always consider live price for high update
            newHigh = Math.max(newHigh, livePrice);
            trace.high[trace.high.length - 1] = newHigh;

            // Update low with consideration for live price
            let newLow = originalLow;
            if (ohlc.low !== undefined) {
                newLow = Math.min(newLow, ohlc.low);
            }
            // Always consider live price for low update
            newLow = Math.min(newLow, livePrice);
            trace.low[trace.low.length - 1] = newLow;

            // Update close - either use OHLC close if provided, or live price
            const newClose = ohlc.close !== undefined ? ohlc.close : livePrice;
            trace.close[trace.close.length - 1] = newClose;

            // Only update x timestamp if this is a different timeframe
            const isNewTimeframe = lastTimestamp.getTime() !== timestamp.getTime();
            if (isNewTimeframe) {
                trace.x[trace.x.length - 1] = timestamp;
            }

            // Force a chart redraw by triggering a data update
            try {
                const updateData = {
                    high: [trace.high],
                    low: [trace.low],
                    close: [trace.close]
                };

                // Include x update if timeframe changed
                if (isNewTimeframe) {
                    updateData.x = [trace.x];
                }

                Plotly.restyle(gd, updateData, [priceTraceIndex]).then(() => {
                    console.log('[DEBUG] Latest candle updated successfully - timestamp:' +
                        (isNewTimeframe ? 'changed' : 'unchanged'));
                    // Re-add trade history markers after live data update
                    if (window.tradeHistoryData && window.tradeHistoryData.length > 0 && window.updateTradeHistoryVisualizations) {
                        window.updateTradeHistoryVisualizations();
                    }
                }).catch((error) => {
                    console.warn('⚠️ Restyle failed, trying react fallback:', error);
                    // Fallback to Plotly.react
                    const layoutWithShapes = { ...gd.layout };
                    if (gd.layout && gd.layout.shapes) {
                        layoutWithShapes.shapes = gd.layout.shapes;
                    }
                    Plotly.react(gd, gd.data, layoutWithShapes, window.config);
                });
            } catch (error) {
                console.error('❌ Error during candlestick restyle:', error);
                // Final fallback
                const layoutWithShapes = { ...gd.layout };
                if (gd.layout && gd.layout.shapes) {
                    layoutWithShapes.shapes = gd.layout.shapes;
                }
                Plotly.react(gd, gd.data, layoutWithShapes, window.config);
            }

        }
    } finally {
        // Always release the lock
        releaseChartUpdateLock();
    }

    // Update price line if it exists
    // updateOrAddRealtimePriceLine(gd, price, timestamp.getTime(), timestamp.getTime() + (getTimeframeSecondsJS(combinedResolution) * 1000));

}

function updateCombinedResolution(newResolution) {
    const oldResolution = combinedResolution;
    combinedResolution = newResolution;
    // Removed config message send - only saveSettings() will trigger config now
}

// Helper function to get timeframe seconds (assuming it's defined elsewhere)
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

// Window resize handler to maintain full height responsiveness
window.addEventListener('resize', function() {
    const chartElement = document.getElementById('chart');
    if (chartElement && window.gd) {
        Plotly.relayout(chartElement, { autosize: true });
    }
});

// Handle live price messages with the format:
// {
//   "type": "live",
//   "symbol": "BTCUSDT",
//   "data": {
//     "live_price": 110818.2,
//     "time": 1760213567
//   }
// }
function handle_live_message(message) {
    // console.log('📈 Received live price message:', message);

    if (!message.data || typeof message.data.live_price === 'undefined') {
        console.warn('⚠️ Invalid live message format - missing data.live_price:', message);
        return;
    }

    const livePrice = parseFloat(message.data.live_price);
    const timestamp = message.data.time;
    const symbol = message.symbol || 'UNKNOWN';

    if (isNaN(livePrice)) {
        console.warn('⚠️ Invalid live price value:', message.data.live_price);
        return;
    }

    // console.log(`💰 Live price update: ${symbol} @ ${livePrice.toFixed(2)} at ${new Date(timestamp * 1000).toLocaleString()}`);

    // Use the existing code to show live price
    // Create a data point in the format expected by handleRealtimeKlineForCombined
    const dataPoint = {
        time: timestamp,
        price: livePrice,
        close: livePrice,
        live_price: livePrice,
        ohlc: {
            close: livePrice
        }
    };

    // Call the existing function to handle the live price update
    handleRealtimeKlineForCombined(dataPoint);

    // Also update any UI elements that display the current price
    if (window.cursorPriceDisplay) {
        window.cursorPriceDisplay.textContent = livePrice.toFixed(2);
    }
}

// Make function globally available
window.handle_live_message = handle_live_message;

// Display buy signal details when clicked
function displayBuySignalDetails(signalData) {

    // Create or update a modal/dialog to show buy signal details
    let modal = document.getElementById('buy-signal-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'buy-signal-modal';
        modal.style.cssText = `
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: white;
            border: 2px solid #00ff00;
            border-radius: 10px;
            padding: 20px;
            z-index: 10000;
            max-width: 500px;
            font-family: Arial, sans-serif;
            box-shadow: 0 4px 8px rgba(0,0,0,0.3);
        `;

        const closeBtn = document.createElement('span');
        closeBtn.textContent = '×';
        closeBtn.style.cssText = `
            position: absolute;
            top: 10px;
            right: 15px;
            cursor: pointer;
            font-size: 24px;
            color: #666;
        `;
        closeBtn.onclick = () => {
            modal.style.display = 'none';
        };

        modal.appendChild(closeBtn);
        document.body.appendChild(modal);

        // Add click outside to close
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.style.display = 'none';
            }
        });
    }

    modal.innerHTML = `
        <span style="position: absolute; top: 10px; right: 15px; cursor: pointer; font-size: 24px; color: #666;" onclick="this.closest('#buy-signal-modal').style.display='none'">×</span>
        <h3 style="color: #00aa00; margin-top: 0; text-align: center; border-bottom: 2px solid #00ff00; padding-bottom: 10px;">
            📈 BUY SIGNAL DETECTED
        </h3>
        <div style="margin: 15px 0;">
            <strong>Time:</strong> ${signalData.timeDisplay || 'N/A'}<br>
            <strong>Price:</strong> ${signalData.price ? signalData.price.toFixed(2) : 'N/A'}<br>
            <strong>RSI:</strong> ${signalData.rsi ? signalData.rsi.toFixed(2) : 'N/A'}<br>
            <strong>RSI SMA14:</strong> ${signalData.rsi_sma14 ? signalData.rsi_sma14.toFixed(2) : 'N/A'}<br>
            <strong>Deviation:</strong> ${signalData.deviation ? signalData.deviation.toFixed(2) : 'N/A'}<br>
            <strong>SMA Trend Up:</strong> ${signalData.sma_trend_up == 1 || signalData.sma_trend_up === true ? '<span style="color: green;">Yes</span>' : '<span style="color: red;">No</span>'}<br>
        </div>
        <div style="margin-top: 20px; padding-top: 15px; border-top: 1px solid #ddd; font-size: 12px; color: #666;">
            <strong>Signal Logic:</strong><br>
            RSI deviates below RSI_SMA14 by >15 points while SMA14 is trending upward
        </div>
    `;

    modal.style.display = 'block';

}

// Make function globally available
window.displayBuySignalDetails = displayBuySignalDetails;

// Store trade history data globally
window.tradeHistoryData = [];

// Process and store trade history data


// Add trade history markers to the chart
function addTradeHistoryMarkersToChart(tradeHistoryData, symbol) {
    // Check if chart is ready
    const chartElement = document.getElementById('chart');

    // Get checkbox elements directly
    const tradeTraceCheckbox = document.getElementById('enable-trade-trace-checkbox');

    // Clear regular trade markers if trade trace is disabled
    if (!tradeTraceCheckbox || !tradeTraceCheckbox.checked) {
        if (window.gd && window.gd.data) {
            window.gd.data = window.gd.data.filter(trace =>
                trace.name !== 'Buy Trades' && trace.name !== 'Sell Trades'
            );
        }
        if (window.gd && window.gd.layout && window.gd.layout.shapes) {
            window.gd.layout.shapes = window.gd.layout.shapes.filter(shape =>
                !shape.name || !shape.name.startsWith('trade_')
            );
        }
        if (window.gd && window.gd.layout && window.gd.layout.annotations) {
            window.gd.layout.annotations = window.gd.layout.annotations.filter(ann =>
                !ann.name || !ann.name.startsWith('trade_annotation_')
            );
        }
        if (window.gd) {
              Plotly.react(chartElement, window.gd.data, window.gd.layout);
        }
        console.log('Combined WebSocket: Trade trace disabled. Regular trade markers cleared.');
        return;
    }


    if (!tradeHistoryData || !Array.isArray(tradeHistoryData) || tradeHistoryData.length === 0) {
        console.warn('Combined WebSocket: No trade history data to display');
        return;
    }

    if (!chartElement || !window.gd || !window.gd.layout) {
        console.warn('Combined WebSocket: Chart not ready for trade history markers');
        return;
    }

    // Ensure shapes array exists
    if (!window.gd.layout.shapes) {
        window.gd.layout.shapes = [];
    }

    // Remove existing trade history markers (preserve my trade markers)
    window.gd.layout.shapes = window.gd.layout.shapes.filter(shape =>
        !shape.name || (!shape.name.startsWith('trade_'))
    );

    // Also remove any existing trade annotations (preserve my trade annotations)
    if (window.gd.layout.annotations) {
        window.gd.layout.annotations = window.gd.layout.annotations.filter(ann =>
            !ann.name || (!ann.name.startsWith('trade_annotation_'))
        );
    }

    console.log(`📊 Combined WebSocket: Adding ${tradeHistoryData.length} trade markers to chart`);

    // Calculate marker size scaling based on ALL trade data (not just current batch)
    // This ensures consistent scaling across batches
    const allTrades = window.tradeHistoryData || [];
    const allValues = allTrades.map(trade => trade.price * trade.amount);
    const maxValue = allValues.length > 0 ? Math.max(...allValues) : 1;
    const minValue = allValues.length > 0 ? Math.min(...allValues) : 0.001;

    // Get current Y-axis range to scale marker sizes
    let yAxisMax = 200; // default fallback
    let yAxisMin = 0;   // default fallback
    try {
        if (window.gd && window.gd.layout && window.gd.layout.yaxis && window.gd.layout.yaxis.range) {
            const yRange = window.gd.layout.yaxis.range;
            yAxisMax = Math.max(yRange[0], yRange[1]);
            yAxisMin = Math.min(yRange[0], yRange[1]);
        }
    } catch (e) {
        console.warn('Failed to get Y-axis range for marker scaling, using fallback:', e);
    }

    const yAxisRange = yAxisMax - yAxisMin;
    const maxMarkerSize = Math.min(40, yAxisRange * 0.40); // Max marker size = 40% of Y-axis range, capped at 40px
    const minMarkerSize = 4; // Smaller minimum marker size

    // Function to scale value to marker size using exponential scaling
    function valueToMarkerSize(value) {
        // Handle invalid values
        if (isNaN(value) || !isFinite(value) || value <= 0) {
            return minMarkerSize;
        }
        // Handle invalid max/min values
        if (isNaN(maxValue) || !isFinite(maxValue) || isNaN(minValue) || !isFinite(minValue)) {
            return minMarkerSize;
        }
        if (maxValue === minValue || maxValue === 0) {
            return minMarkerSize; // All same size if no value variation or all zero
        }
        const normalizedValue = (value - minValue) / (maxValue - minValue);
        // Handle case where normalization results in NaN (e.g., division by zero)
        if (isNaN(normalizedValue)) {
            return minMarkerSize;
        }
        // Apply square root transformation for exponential scaling
        // This makes smaller trades more visible while preventing large trades from dominating
        const exponentialValue = Math.sqrt(normalizedValue);
        const markerSize = minMarkerSize + (exponentialValue * (maxMarkerSize - minMarkerSize));
        const finalSize = Math.max(minMarkerSize, Math.min(markerSize, maxMarkerSize)); // Clamp within bounds

        // Debug: Log scaling for first few trades
        if (value === maxValue) {
            console.log('My trade marker scaling debug:', {
                value,
                minValue,
                maxValue,
                normalizedValue,
                exponentialValue,
                markerSize,
                finalSize,
                minMarkerSize,
                maxMarkerSize
            });
        }

        return finalSize;
    }

    // DEBUG: Log first 5 trades to check price/amount formatting
    if (tradeHistoryData.length > 0) {
        console.log('🔍 First 5 trades for price/format checking:', tradeHistoryData.slice(0, 5).map((t, i) => ({
            index: i,
            price: t.price,
            priceType: typeof t.price,
            amount: t.amount,
            amountType: typeof t.amount,
            timestamp: t.timestamp,
            exchaange: t.exchange || 'N/A'
        })));
    }

    console.log(`📊 Trade marker scaling: maxValue=${maxValue}, yAxisRange=${yAxisRange.toFixed(2)}, maxMarkerSize=${maxMarkerSize.toFixed(2)}`);

    // Separate mytrades from regular trades
    const myTrades = tradeHistoryData.filter(trade => trade.exchange === 'APEX' || trade.exchange === 'APEXPRO');
    const regularTrades = tradeHistoryData.filter(trade => trade.exchange !== 'APEX' && trade.exchange !== 'APEXPRO');

    // Process regular trades
    const buyTrades = regularTrades.filter(trade => trade.side === 'BUY');
    const sellTrades = regularTrades.filter(trade => trade.side === 'SELL');

    console.log(`📊 Combined WebSocket: ${buyTrades.length} buy trades, ${sellTrades.length} sell trades`);

    // Debug timestamp format
    if (buyTrades.length > 0) {
        console.log('🔍 BUY TRADE TIMESTAMP SAMPLE:', {
            raw: buyTrades[0].timestamp,
            type: typeof buyTrades[0].timestamp,
            isISO: typeof buyTrades[0].timestamp === 'string' && buyTrades[0].timestamp.includes('T'),
            parsed: new Date(buyTrades[0].timestamp)
        });
    }

    if (buyTrades.length > 0) {
        const buyX = buyTrades.map(trade => {
            if (typeof trade.timestamp === 'string' && trade.timestamp.includes('T')) {
                // ISO timestamp string
                return new Date(trade.timestamp);
            } else {
                // Numeric timestamp (legacy)
                return new Date(trade.timestamp * 1000);
            }
        });
        const buyY = buyTrades.map(trade => trade.price);
        const buySizes = buyTrades.map(trade => valueToMarkerSize(trade.price * trade.amount));
        const buyCustomData = buyTrades.map((trade, index) => {
            const amount = trade.amount && !isNaN(trade.amount) ? trade.amount : 0;
            const exchange = trade.exchange;
            const value = trade.price && amount ? trade.price * amount : 0;
            const timestamp = trade.timestamp && !isNaN(trade.timestamp) ? trade.timestamp : Date.now() / 1000;
            const timeDisplay = (() => {
                if (typeof trade.timestamp === 'string' && trade.timestamp.includes('T')) {
                    // ISO timestamp string
                    return new Date(trade.timestamp).toLocaleString();
                } else {
                    // Numeric timestamp (legacy)
                    return new Date(timestamp * 1000).toLocaleString();
                }
            })();
            const symbol = trade.symbol;
            const price = trade.price || 0;
            return {
                exchange: exchange,
                price: price,
                amount: amount,
                timestamp: timestamp,
                symbol: symbol,
                timeDisplay: timeDisplay,
                markerSize: buySizes[index],
                value: value,
                text: `${symbol} BUY: $${price.toFixed(4)} ($${value.toFixed(2)}) [size: ${buySizes[index].toFixed(1)}]`
            };
        });
        const buyText = buyCustomData.map(data => data.text);

        const buyTrace = {
            x: buyX,
            y: buyY,
            mode: 'markers',
            type: 'scatter',
            name: 'Buy Trades',
            marker: {
                symbol: 'triangle-up',
                size: buySizes,
                color: 'rgba(94, 255, 0, 0.5)',
                line: {
                    color: 'rgba(17, 37, 5, 1)',
                    width: 1
                }
            },
            text: buyText,
            customdata: buyCustomData,
            hovertemplate: `
                <b>📈 BUY TRADE</b><br>
                <b>Exchange:</b> %{customdata.exchange}<br>
                <b>Symbol:</b> %{customdata.symbol}<br>
                <b>Price:</b> $%{customdata.price:.4f}<br>
                <b>Amount:</b> %{customdata.amount:.6f} %{customdata.symbol:/USDT}<br>
                <b>Time:</b> %{customdata.timeDisplay}<br>
                <b>Value:</b> $%{customdata.value:.2f}<br>
                <b>Marker Size:</b> %{customdata.markerSize:.1f}<br>
                <extra></extra>
            `,
            hoverlabel: {
                bgcolor: 'rgba(0, 100, 0, 0.4)',
                bordercolor: 'lime',
                font: { color: 'white', size: 11 }
            },
            xaxis: 'x',
            yaxis: 'y',
            showlegend: true
        };

        // Find existing Buy Trades trace and replace it, or add new one
        const existingBuyTraceIndex = window.gd.data.findIndex(trace => trace.name === 'Buy Trades');
        if (existingBuyTraceIndex !== -1) {
            window.gd.data[existingBuyTraceIndex] = buyTrace;
        } else {
            window.gd.data.push(buyTrace);
        }
    }

    // SELL trades as red triangles pointing down
    if (sellTrades.length > 0) {
        console.log('🔍 SELL TRADE TIMESTAMP SAMPLE:', {
            raw: sellTrades[0].timestamp,
            type: typeof sellTrades[0].timestamp,
            isISO: typeof sellTrades[0].timestamp === 'string' && sellTrades[0].timestamp.includes('T'),
            parsed: new Date(sellTrades[0].timestamp)
        });

        const sellX = sellTrades.map(trade => {
            if (typeof trade.timestamp === 'string' && trade.timestamp.includes('T')) {
                // ISO timestamp string
                return new Date(trade.timestamp);
            } else {
                // Numeric timestamp (legacy)
                return new Date(trade.timestamp * 1000);
            }
        });
        const sellY = sellTrades.map(trade => trade.price);
        const sellSizes = sellTrades.map(trade => valueToMarkerSize(trade.price * trade.amount));
        const sellCustomData = sellTrades.map((trade, index) => {
            const amount = trade.amount && !isNaN(trade.amount) ? trade.amount : 0;
            const exchange = trade.exchange;
            const value = trade.price && amount ? trade.price * amount : 0;
            const timestamp = trade.timestamp && !isNaN(trade.timestamp) ? trade.timestamp : Date.now() / 1000;
            const timeDisplay = (() => {
                if (typeof trade.timestamp === 'string' && trade.timestamp.includes('T')) {
                    // ISO timestamp string
                    return new Date(trade.timestamp).toLocaleString();
                } else {
                    // Numeric timestamp (legacy)
                    return new Date(timestamp * 1000).toLocaleString();
                }
            })();
            const symbol = trade.symbol;
            const price = trade.price || 0;
            return {
                price: price,
                exchange: exchange,
                amount: amount,
                timestamp: timestamp,
                symbol: symbol,
                timeDisplay: timeDisplay,
                markerSize: sellSizes[index],
                value: value,
                text: `${symbol} SELL: $${price.toFixed(4)} ($${value.toFixed(2)}) [size: ${sellSizes[index].toFixed(1)}]`
            };
        });
        const sellText = sellCustomData.map(data => data.text);

        const sellTrace = {
            x: sellX,
            y: sellY,
            mode: 'markers',
            type: 'scatter',
            name: 'Sell Trades',
            marker: {
                symbol: 'triangle-down',
                size: sellSizes,
                color: 'rgba(255, 0, 0, 0.5)',
                line: {
                    color: 'rgba(88, 7, 7, 1)',
                    width: 1
                }
            },
            text: sellText,
            customdata: sellCustomData,
            hovertemplate: `
                <b>📉 SELL TRADE</b><br>
                <b>Exchange:</b> %{customdata.exchange}<br>
                <b>Symbol:</b> %{customdata.symbol}<br>
                <b>Price:</b> $%{customdata.price:.4f}<br>
                <b>Amount:</b> %{customdata.amount:.6f} %{customdata.symbol:/USDT}<br>
                <b>Time:</b> %{customdata.timeDisplay}<br>
                <b>Value:</b> $%{customdata.value:.2f}<br>
                <b>Marker Size:</b> %{customdata.markerSize:.1f}<br>
                <extra></extra>
            `,
            hoverlabel: {
                bgcolor: 'rgba(100, 0, 0, 0.3)',
                bordercolor: 'red',
                font: { color: 'white', size: 11 }
            },
            xaxis: 'x',
            yaxis: 'y',
            showlegend: true
        };

        // Find existing Sell Trades trace and replace it, or add new one
        const existingSellTraceIndex = window.gd.data.findIndex(trace => trace.name === 'Sell Trades');
        if (existingSellTraceIndex !== -1) {
            window.gd.data[existingSellTraceIndex] = sellTrace;
        } else {
            window.gd.data.push(sellTrace);
        }
    }

    // Update the chart with new trade markers
    try {
        Plotly.react(chartElement, window.gd.data, window.gd.layout, window.config);
        console.log(`📊 Combined WebSocket: Successfully added trade history markers for ${tradeHistoryData.length} trades`);

        // Debug: Check if my trade traces are still present after regular trade update
        setTimeout(() => {
            if (window.gd && window.gd.data) {
                const myTradeTraces = window.gd.data.filter(t => t.name === 'My Buy Trades' || t.name === 'My Sell Trades');
                console.log('My trade traces after regular trade update:', myTradeTraces.length, myTradeTraces.map(t => ({ name: t.name, points: t.x ? t.x.length : 0 })));
                if (myTradeTraces.length === 0) {
                    console.warn('🚨 MY TRADE TRACES LOST after regular trade update!');
                }
            }
        }, 100);
    } catch (error) {
        console.error('Combined WebSocket: Error updating chart with trade markers:', error);
    }
}

// Add my trade history markers to the chart (user's own trades from APEX/APEXPRO)
function addMyTradeHistoryMarkersToChart(myTradeHistoryData, symbol) {
    // Check if chart is ready
    const chartElement = document.getElementById('chart');

    // Get checkbox element directly
    const myTradesCheckbox = document.getElementById('enable-my-trades-checkbox');

    // Debug logging
    console.log('My trades checkbox check:', {
        checkboxExists: !!myTradesCheckbox,
        checkboxChecked: myTradesCheckbox ? myTradesCheckbox.checked : 'N/A',
        myTradesLength: myTradeHistoryData.length
    });

    if (!myTradesCheckbox || !myTradesCheckbox.checked) {
        // Clear existing my trade markers if trace is disabled
        if (window.gd && window.gd.data) {
            window.gd.data = window.gd.data.filter(trace =>
                trace.name !== 'My Buy Trades' && trace.name !== 'My Sell Trades'
            );
        }
        if (window.gd && window.gd.layout && window.gd.layout.shapes) {
            window.gd.layout.shapes = window.gd.layout.shapes.filter(shape =>
                !shape.name || !shape.name.startsWith('my_trade_')
            );
        }
        if (window.gd && window.gd.layout && window.gd.layout.annotations) {
            window.gd.layout.annotations = window.gd.layout.annotations.filter(ann =>
                !ann.name || !ann.name.startsWith('my_trade_annotation_')
            );
        }
        if (window.gd) {
              Plotly.react(chartElement, window.gd.data, window.gd.layout, window.config);
        }
        console.log('Combined WebSocket: My trade trace disabled. Markers cleared.');
        return;
    }

    if (!myTradeHistoryData || !Array.isArray(myTradeHistoryData) || myTradeHistoryData.length === 0) {
        console.warn('Combined WebSocket: No my trade history data to display');
        return;
    }

    if (!chartElement || !window.gd || !window.gd.layout) {
        console.warn('Combined WebSocket: Chart not ready for my trade history markers');
        return;
    }

    // Ensure shapes array exists
    if (!window.gd.layout.shapes) {
        window.gd.layout.shapes = [];
    }

    // Remove existing my trade history markers
    window.gd.layout.shapes = window.gd.layout.shapes.filter(shape =>
        !shape.name || !shape.name.startsWith('my_trade_')
    );

    // Also remove any existing my trade annotations
    if (window.gd.layout.annotations) {
        window.gd.layout.annotations = window.gd.layout.annotations.filter(ann =>
            !ann.name || !ann.name.startsWith('my_trade_annotation_')
        );
    }

    console.log(`📊 Combined WebSocket: Adding ${myTradeHistoryData.length} my trade markers to chart`);

    // Debug: Check current chart traces before adding my trades
    if (window.gd && window.gd.data) {
        const existingTraces = window.gd.data.map(t => t.name);
        console.log('Existing chart traces before adding my trades:', existingTraces);
    }

    // Calculate marker size scaling based on the provided my trade data
    // This ensures scaling is based on the actual data being displayed
    const allValues = myTradeHistoryData.map(trade => trade.price * trade.amount);
    const maxValue = allValues.length > 0 ? Math.max(...allValues) : 1;
    const minValue = allValues.length > 0 ? Math.min(...allValues) : 0.001;

    // Get current Y-axis range to scale marker sizes
    let yAxisMax = 200; // default fallback
    let yAxisMin = 0;   // default fallback
    try {
        if (window.gd.layout && window.gd.layout.yaxis && window.gd.layout.yaxis.range) {
            const yRange = window.gd.layout.yaxis.range;
            yAxisMax = Math.max(yRange[0], yRange[1]);
            yAxisMin = Math.min(yRange[0], yRange[1]);
        }
    } catch (e) {
        console.warn('Failed to get Y-axis range for marker scaling, using fallback:', e);
    }

    const yAxisRange = yAxisMax - yAxisMin;
    const maxMarkerSize = Math.min(120, yAxisRange * 1.0); // Increased max marker size for better visibility
    const minMarkerSize = 20; // Larger minimum marker size for my trades (increased from 12)

    // Function to scale value to marker size using exponential scaling
    function valueToMarkerSize(value) {
        // Handle invalid values
        if (isNaN(value) || !isFinite(value) || value <= 0) {
            return minMarkerSize;
        }
        // Handle invalid max/min values
        if (isNaN(maxValue) || !isFinite(maxValue) || isNaN(minValue) || !isFinite(minValue)) {
            return minMarkerSize;
        }
        if (maxValue === minValue || maxValue === 0) {
            return minMarkerSize; // All same size if no value variation or all zero
        }
        const normalizedValue = (value - minValue) / (maxValue - minValue);
        // Handle case where normalization results in NaN (e.g., division by zero)
        if (isNaN(normalizedValue)) {
            return minMarkerSize;
        }
        // Apply square root transformation for exponential scaling
        // This makes smaller trades more visible while preventing large trades from dominating
        const exponentialValue = Math.sqrt(normalizedValue);
        const markerSize = minMarkerSize + (exponentialValue * (maxMarkerSize - minMarkerSize));
        return Math.max(minMarkerSize, Math.min(markerSize, maxMarkerSize)); // Clamp within bounds
    }

    // DEBUG: Log first 5 trades to check price/amount formatting
    if (myTradeHistoryData.length > 0) {
        console.log('🔍 First 5 my trades for price/format checking:', myTradeHistoryData.slice(0, 5).map((t, i) => ({
            index: i,
            price: t.price,
            priceType: typeof t.price,
            amount: t.amount,
            amountType: typeof t.amount,
            timestamp: t.timestamp,
            exchange: t.exchange || 'N/A'
        })));
    }

    console.log(`📊 My trade marker scaling: maxValue=${maxValue}, yAxisRange=${yAxisRange.toFixed(2)}, maxMarkerSize=${maxMarkerSize.toFixed(2)}`);

    // Process my trades
    const myBuyTrades = myTradeHistoryData.filter(trade => trade.side === 'BUY');
    const mySellTrades = myTradeHistoryData.filter(trade => trade.side === 'SELL');

    console.log(`📊 Combined WebSocket: ${myBuyTrades.length} my buy trades, ${mySellTrades.length} my sell trades`);

    // Debug: Log sample trade data
    if (myBuyTrades.length > 0) {
        console.log('Sample my buy trade:', {
            price: myBuyTrades[0].price,
            amount: myBuyTrades[0].amount,
            timestamp: myBuyTrades[0].timestamp,
            exchange: myBuyTrades[0].exchange
        });
    }
    if (mySellTrades.length > 0) {
        console.log('Sample my sell trade:', {
            price: mySellTrades[0].price,
            amount: mySellTrades[0].amount,
            timestamp: mySellTrades[0].timestamp,
            exchange: mySellTrades[0].exchange
        });
    }

    // Debug timestamp format
    if (myBuyTrades.length > 0) {
        console.log('🔍 MY BUY TRADE TIMESTAMP SAMPLE:', {
            raw: myBuyTrades[0].timestamp,
            type: typeof myBuyTrades[0].timestamp,
            isISO: typeof myBuyTrades[0].timestamp === 'string' && myBuyTrades[0].timestamp.includes('T'),
            parsed: new Date(myBuyTrades[0].timestamp)
        });
    }

    if (myBuyTrades.length > 0) {
        const buyX = myBuyTrades.map(trade => {
            if (typeof trade.timestamp === 'string' && trade.timestamp.includes('T')) {
                // ISO timestamp string
                return new Date(trade.timestamp);
            } else {
                // Numeric timestamp (legacy)
                return new Date(trade.timestamp * 1000);
            }
        });
        const buyY = myBuyTrades.map(trade => trade.price);
        const buySizes = myBuyTrades.map(trade => valueToMarkerSize(trade.price * trade.amount));
        const buyCustomData = myBuyTrades.map((trade, index) => {
            const amount = trade.amount && !isNaN(trade.amount) ? trade.amount : 0;
            const exchange = trade.exchange;
            const value = trade.price && amount ? trade.price * amount : 0;
            const timestamp = trade.timestamp && !isNaN(trade.timestamp) ? trade.timestamp : Date.now() / 1000;
            const timeDisplay = (() => {
                if (typeof trade.timestamp === 'string' && trade.timestamp.includes('T')) {
                    // ISO timestamp string
                    return new Date(trade.timestamp).toLocaleString();
                } else {
                    // Numeric timestamp (legacy)
                    return new Date(timestamp * 1000).toLocaleString();
                }
            })();
            const symbol = trade.symbol;
            const price = trade.price || 0;
            return {
                exchange: exchange,
                price: price,
                amount: amount,
                timestamp: timestamp,
                symbol: symbol,
                timeDisplay: timeDisplay,
                markerSize: buySizes[index],
                value: value,
                text: `MY ${symbol} BUY: $${price.toFixed(4)} ($${value.toFixed(2)}) [size: ${buySizes[index].toFixed(1)}]`
            };
        });
        const buyText = buyCustomData.map(data => data.text);

        const buyTrace = {
            x: buyX,
            y: buyY,
            mode: 'markers',
            type: 'scatter',
            name: 'My Buy Trades',
            marker: {
                symbol: 'diamond',
                size: buySizes,
                color: 'rgba(0, 255, 255, 1.0)', // More opaque cyan for my buy trades
                line: {
                    color: 'rgba(0, 139, 139, 1)', // Dark cyan border
                    width: 3 // Thicker border for visibility
                }
            },
            text: buyText,
            customdata: buyCustomData,
            hovertemplate: `
                <b>📈 MY BUY TRADE</b><br>
                <b>Exchange:</b> %{customdata.exchange}<br>
                <b>Symbol:</b> %{customdata.symbol}<br>
                <b>Price:</b> $%{customdata.price:.4f}<br>
                <b>Amount:</b> %{customdata.amount:.6f} %{customdata.symbol:/USDT}<br>
                <b>Time:</b> %{customdata.timeDisplay}<br>
                <b>Value:</b> $%{customdata.value:.2f}<br>
                <b>Marker Size:</b> %{customdata.markerSize:.1f}<br>
                <extra></extra>
            `,
            hoverlabel: {
                bgcolor: 'rgba(0, 139, 139, 0.8)',
                bordercolor: 'cyan',
                font: { color: 'white', size: 11 }
            },
            xaxis: 'x',
            yaxis: 'y',
            showlegend: true
        };

        // Find existing My Buy Trades trace and replace it, or add new one
        const existingBuyTraceIndex = window.gd.data.findIndex(trace => trace.name === 'My Buy Trades');
        if (existingBuyTraceIndex !== -1) {
            window.gd.data[existingBuyTraceIndex] = buyTrace;
            console.log('Updated existing My Buy Trades trace at index:', existingBuyTraceIndex);
        } else {
            window.gd.data.push(buyTrace);
            console.log('Added new My Buy Trades trace');
        }
    }

    // MY SELL trades as inverted triangles with different color
    if (mySellTrades.length > 0) {
        console.log('🔍 MY SELL TRADE TIMESTAMP SAMPLE:', {
            raw: mySellTrades[0].timestamp,
            type: typeof mySellTrades[0].timestamp,
            isISO: typeof mySellTrades[0].timestamp === 'string' && mySellTrades[0].timestamp.includes('T'),
            parsed: new Date(mySellTrades[0].timestamp)
        });

        const sellX = mySellTrades.map(trade => {
            if (typeof trade.timestamp === 'string' && trade.timestamp.includes('T')) {
                // ISO timestamp string
                return new Date(trade.timestamp);
            } else {
                // Numeric timestamp (legacy)
                return new Date(trade.timestamp * 1000);
            }
        });
        const sellY = mySellTrades.map(trade => trade.price);
        const sellSizes = mySellTrades.map(trade => valueToMarkerSize(trade.price * trade.amount));
        const sellCustomData = mySellTrades.map((trade, index) => {
            const amount = trade.amount && !isNaN(trade.amount) ? trade.amount : 0;
            const exchange = trade.exchange;
            const value = trade.price && amount ? trade.price * amount : 0;
            const timestamp = trade.timestamp && !isNaN(trade.timestamp) ? trade.timestamp : Date.now() / 1000;
            const timeDisplay = (() => {
                if (typeof trade.timestamp === 'string' && trade.timestamp.includes('T')) {
                    // ISO timestamp string
                    return new Date(trade.timestamp).toLocaleString();
                } else {
                    // Numeric timestamp (legacy)
                    return new Date(timestamp * 1000).toLocaleString();
                }
            })();
            const symbol = trade.symbol;
            const price = trade.price || 0;
            return {
                price: price,
                exchange: exchange,
                amount: amount,
                timestamp: timestamp,
                symbol: symbol,
                timeDisplay: timeDisplay,
                markerSize: sellSizes[index],
                value: value,
                text: `MY ${symbol} SELL: $${price.toFixed(4)} ($${value.toFixed(2)}) [size: ${sellSizes[index].toFixed(1)}]`
            };
        });
        const sellText = sellCustomData.map(data => data.text);

        const sellTrace = {
            x: sellX,
            y: sellY,
            mode: 'markers',
            type: 'scatter',
            name: 'My Sell Trades',
            marker: {
                symbol: 'diamond-tall',
                size: sellSizes,
                color: 'rgba(255, 165, 0, 1.0)', // More opaque orange for my sell trades
                line: {
                    color: 'rgba(139, 69, 19, 1)', // Saddle brown border
                    width: 3 // Thicker border for visibility
                }
            },
            text: sellText,
            customdata: sellCustomData,
            hovertemplate: `
                <b>📉 MY SELL TRADE</b><br>
                <b>Exchange:</b> %{customdata.exchange}<br>
                <b>Symbol:</b> %{customdata.symbol}<br>
                <b>Price:</b> $%{customdata.price:.4f}<br>
                <b>Amount:</b> %{customdata.amount:.6f} %{customdata.symbol:/USDT}<br>
                <b>Time:</b> %{customdata.timeDisplay}<br>
                <b>Value:</b> $%{customdata.value:.2f}<br>
                <b>Marker Size:</b> %{customdata.markerSize:.1f}<br>
                <extra></extra>
            `,
            hoverlabel: {
                bgcolor: 'rgba(139, 69, 19, 0.8)',
                bordercolor: 'orange',
                font: { color: 'white', size: 11 }
            },
            xaxis: 'x',
            yaxis: 'y',
            showlegend: true
        };

        // Find existing My Sell Trades trace and replace it, or add new one
        const existingSellTraceIndex = window.gd.data.findIndex(trace => trace.name === 'My Sell Trades');
        if (existingSellTraceIndex !== -1) {
            window.gd.data[existingSellTraceIndex] = sellTrace;
            console.log('Updated existing My Sell Trades trace at index:', existingSellTraceIndex);
        } else {
            window.gd.data.push(sellTrace);
            console.log('Added new My Sell Trades trace');
        }
    }

    // Update the chart with new my trade markers
    try {
        Plotly.react(chartElement, window.gd.data, window.gd.layout, window.config);
        console.log(`📊 Combined WebSocket: Successfully added my trade history markers for ${myTradeHistoryData.length} trades`);

        // Debug: Check chart traces after update
        if (window.gd && window.gd.data) {
            const updatedTraces = window.gd.data.map(t => t.name);
            console.log('Chart traces after adding my trades:', updatedTraces);
            const myTradeTraces = window.gd.data.filter(t => t.name === 'My Buy Trades' || t.name === 'My Sell Trades');
            console.log('My trade traces found:', myTradeTraces.length, myTradeTraces.map(t => ({ name: t.name, points: t.x ? t.x.length : 0 })));
        }
    } catch (error) {
        console.error('Combined WebSocket: Error updating chart with my trade markers:', error);
    }
}

// Handle trade history messages from WebSocket
function handleTradeHistoryMessage(message) {
    console.log(`📊 Combined WebSocket: Received trade history message with ${message.data.length} trades`);

    // Separate mytrades from regular trades
    const myTrades = message.data.filter(trade => trade.exchange === 'APEX' || trade.exchange === 'APEXPRO');
    const regularTrades = message.data.filter(trade => trade.exchange !== 'APEX' && trade.exchange !== 'APEXPRO');

    // Process regular trades
    addTradeHistoryMarkersToChart(regularTrades, message.symbol);

    // Process my trades separately
    addMyTradeHistoryMarkersToChart(myTrades, message.symbol);
}

// Export functions to global scope for use by other modules
window.setupCombinedWebSocket = setupCombinedWebSocket;
window.closeCombinedWebSocket = closeCombinedWebSocket;
window.updateCombinedIndicators = updateCombinedIndicators;
window.updateCombinedResolution = updateCombinedResolution;
window.setupWebSocketMessageHandler = setupWebSocketMessageHandler;
window.mergeDataPoints = mergeDataPoints;
window.mergeDataPointsWithIndicators = mergeDataPointsWithIndicators;
window.updateLayoutForIndicators = updateLayoutForIndicators;
function cleanupVolumeProfileForRectangle(rectangleId) {

    if (!window.gd || !window.gd.data) {
        console.warn('🧹 No chart data available for cleanup');
        return false;
    }

    const initialTraceCount = window.gd.data.length;

    // Filter out volume profile traces associated with this rectangle
    // They have names like "VP-${rectangleId} Buy: price" or "VP-${rectangleId} Sell: price"
    const filteredTraces = window.gd.data.filter(trace =>
        !trace.name || !trace.name.includes(`VP-${rectangleId}`)
    );

    const removedCount = initialTraceCount - filteredTraces.length;

    if (removedCount > 0) {

        // Update the chart with filtered traces
        Plotly.react(window.gd, filteredTraces, window.gd.layout).then(() => {
        }).catch((error) => {
            console.error(`❌ Error during volume profile cleanup:`, error);
        });

        return true;
    } else {
        return false;
    }
}

window.cleanupVolumeProfileForRectangle = cleanupVolumeProfileForRectangle;

// DEBUG: Add debugging function for chart diagnosis
window.debugChartState = function() {

    if (window.gd) {
        if (window.gd.data && window.gd.data.length > 0) {
            window.gd.data.forEach((trace, index) => {
                if (trace.x && trace.x.length > 0) {
                }
            });
        }

    } else {
    }


    return {
        websocket: combinedWebSocket ? combinedWebSocket.readyState : 'none',
        chartTraces: window.gd?.data?.length || 0,
        xAxisRange: window.currentXAxisRange,
        yAxisRange: window.currentYAxisRange
    };
};

// DEBUG: Add function to check WebSocket message handler status
window.checkWebSocketStatus = function() {

    if (!combinedWebSocket) {
        return { connected: false, readyState: 'none', hasHandler: false };
    }

    const status = {
        connected: combinedWebSocket.readyState === WebSocket.OPEN,
        readyState: combinedWebSocket.readyState,
        hasHandler: typeof combinedWebSocket.onmessage !== 'undefined',
        url: combinedWebSocket.url
    };

    return status;
};

// DEBUG: Add function to manually test historical data loading
window.testHistoricalDataLoad = function() {

    // Check if we have a chart
    if (!window.gd) {
        // console.error('❌ No chart found (window.gd is undefined)');
        return;
    }

    // Check current data
    const currentDataPoints = window.gd.data && window.gd.data[0] ? window.gd.data[0].x.length : 0;

    // Check axis ranges
    const xRange = window.gd.layout.xaxis.range;
    if (xRange) {
    }

    // Simulate a pan to the left by calling the pan detection
    if (typeof window.testPanningDetection === 'function') {
        window.testPanningDetection();
    } else {
        // console.warn('⚠️ testPanningDetection function not found');
    }

};

// DEBUG: Add chart rendering validation function
window.validateChartRendering = function() {

    if (!window.gd) {
        // console.error('❌ No chart found (window.gd is undefined)');
        return { valid: false, error: 'No chart found' };
    }

    const validation = {
        valid: true,
        errors: [],
        warnings: [],
        data: {}
    };

    // Check if chart has data
    if (!window.gd.data || window.gd.data.length === 0) {
        validation.errors.push('Chart has no data traces');
        validation.valid = false;
    } else {
        validation.data.traceCount = window.gd.data.length;
        validation.data.firstTracePoints = window.gd.data[0] ? window.gd.data[0].x.length : 0;
    }

    // Check X-axis range
    if (window.gd.layout.xaxis.range) {
        const xRange = window.gd.layout.xaxis.range;
        validation.data.xAxisRange = {
            start: xRange[0],
            end: xRange[1],
            duration: xRange[1].getTime() - xRange[0].getTime(),
            durationHours: (xRange[1].getTime() - xRange[0].getTime()) / (1000 * 60 * 60)
        };
    } else {
        validation.warnings.push('No X-axis range set');
    }

    // Check Y-axis range
    if (window.gd.layout.yaxis.range) {
        validation.data.yAxisRange = window.gd.layout.yaxis.range;
    } else {
        validation.warnings.push('No Y-axis range set');
    }

    // Compare with saved ranges
    if (window.currentXAxisRange) {
        const savedStart = new Date(window.currentXAxisRange[0] < 2e9 ? window.currentXAxisRange[0] * 1000 : window.currentXAxisRange[0]);
        const savedEnd = new Date(window.currentXAxisRange[1] < 2e9 ? window.currentXAxisRange[1] * 1000 : window.currentXAxisRange[1]);
        const chartStart = window.gd.layout.xaxis.range ? window.gd.layout.xaxis.range[0] : null;
        const chartEnd = window.gd.layout.xaxis.range ? window.gd.layout.xaxis.range[1] : null;

        if (chartStart && chartEnd) {
            const startDiff = Math.abs(savedStart.getTime() - chartStart.getTime());
            const endDiff = Math.abs(savedEnd.getTime() - chartEnd.getTime());

            validation.data.rangeComparison = {
                savedRange: [savedStart, savedEnd],
                chartRange: [chartStart, chartEnd],
                startDifferenceMs: startDiff,
                endDifferenceMs: endDiff,
                startDifferenceHours: startDiff / (1000 * 60 * 60),
                endDifferenceHours: endDiff / (1000 * 60 * 60)
            };

            if (startDiff > 60000 || endDiff > 60000) { // More than 1 minute difference
                validation.warnings.push(`Significant range mismatch: start diff ${startDiff / 1000}s, end diff ${endDiff / 1000}s`);
            }
        }
    }

    // Check for data gaps in the chart
    if (window.gd.data && window.gd.data[0] && window.gd.data[0].x) {
        const timestamps = window.gd.data[0].x.map(t => t.getTime()).sort((a, b) => a - b);
        let gaps = 0;
        const expectedInterval = 60 * 60 * 1000; // 1 hour in milliseconds for 1h resolution

        for (let i = 1; i < timestamps.length; i++) {
            const gap = timestamps[i] - timestamps[i-1];
            if (gap > expectedInterval * 1.5) { // Allow 50% tolerance
                gaps++;
            }
        }

        validation.data.dataGaps = gaps;
        if (gaps > 0) {
            validation.warnings.push(`${gaps} data gaps detected in chart`);
        }
    }

    // Check indicator data quality
    if (window.gd.data) {
        let totalNaN = 0;
        let totalPoints = 0;

        window.gd.data.forEach((trace, index) => {
            if (trace.y && Array.isArray(trace.y)) {
                const nanCount = trace.y.filter(v => v === null || v === undefined || isNaN(v)).length;
                totalNaN += nanCount;
                totalPoints += trace.y.length;

                if (nanCount > 0) {
                    validation.warnings.push(`Trace ${index} (${trace.name}): ${nanCount}/${trace.y.length} NaN values`);
                }
            }
        });

        validation.data.nanStats = {
            totalNaN,
            totalPoints,
            nanPercentage: totalPoints > 0 ? (totalNaN / totalPoints * 100).toFixed(1) : 0
        };

        if (totalNaN > totalPoints * 0.1) { // More than 10% NaN values
            validation.warnings.push(`High NaN percentage: ${validation.data.nanStats.nanPercentage}% of all data points`);
        }
    }

    // Summary

    if (validation.errors.length > 0) {
        // console.error('❌ ERRORS:', validation.errors);
    }

    if (validation.warnings.length > 0) {
        // console.warn('⚠️ WARNINGS:', validation.warnings);
    }

    return validation;
};



// Ensure mouse wheel zoom remains enabled and dragmode is set to pan after chart operations
function ensureScrollZoomEnabled() {
    // Check if chart exists
    const gd = document.getElementById('chart');
    if (!gd) {
        return;
    }

    // Check if scrollZoom is enabled in current configuration
    let scrollZoomEnabled = false;
    if (gd._fullLayout && gd._fullLayout.config) {
        scrollZoomEnabled = gd._fullLayout.config.scrollZoom !== false;
    } else {
        // If _fullLayout is not available, check the layout config directly
        scrollZoomEnabled = gd.layout && gd.layout.config ? gd.layout.config.scrollZoom !== false : true; // Default to true if not found
    }

    // If scrollZoom is disabled, re-enable it
    if (!scrollZoomEnabled) {
        try {
            // Use update instead of relayout for config changes
            Plotly.update(gd, {}, { config: { scrollZoom: true } });
        } catch (error) {
            console.warn('ensureScrollZoomEnabled: Failed to re-enable scrollZoom via update:', error);
            // Fallback to relayout
            try {
                Plotly.relayout(gd, { 'config.scrollZoom': true });
            } catch (error2) {
                console.warn('ensureScrollZoomEnabled: Failed to re-enable scrollZoom via relayout:', error2);
            }
        }
    }

    // Additional check: Ensure dragmode is set to 'pan' for default panning behavior, unless in drawing mode
    if (gd.layout && gd.layout.dragmode !== 'pan' && gd.layout.dragmode !== 'drawline' && gd.layout.dragmode !== 'drawrect') {
        try {
            Plotly.relayout(gd, { dragmode: 'pan' });
        } catch (error) {
            console.warn('ensureScrollZoomEnabled: Failed to set dragmode to pan:', error);
        }
    }
}

// Make the function globally accessible
window.ensureScrollZoomEnabled = ensureScrollZoomEnabled;

// DEBUG: Add functions for testing the new synchronization features
window.getMessageQueueStatus = function() {
    return {
        queueLength: messageQueue.length,
        isProcessing: isProcessingMessage,
        chartLocked: chartUpdateLock,
        messageTypes: messageQueue.map(msg => msg.type)
    };
};

// DEBUG: Comprehensive chart inspection for data overlapping issues
window.inspectChartData = function() {
    if (!window.gd || !window.gd.data) {
        return { error: 'No chart data' };
    }

    const traces = window.gd.data;

    const inspection = {
        totalTraces: traces.length,
        traces: [],
        overlappingDetected: false,
        issues: []
    };

    traces.forEach((trace, index) => {
        const traceInfo = {
            index,
            name: trace.name,
            type: trace.type,
            yaxis: trace.yaxis,
            dataPoints: trace.x ? trace.x.length : 0,
            firstTimestamp: trace.x && trace.x.length > 0 ? new Date(trace.x[0]).toISOString() : null,
            lastTimestamp: trace.x && trace.x.length > 0 ? new Date(trace.x[trace.x.length - 1]).toISOString() : null,
            yRange: trace.y ? [Math.min(...trace.y), Math.max(...trace.y)] : null
        };

        inspection.traces.push(traceInfo);
    });

    // Check for overlapping issues
    const priceTraces = traces.filter(t => t.type === 'candlestick');
    const indicatorTraces = traces.filter(t => t.type !== 'candlestick');

    // Check if multiple price traces exist
    if (priceTraces.length > 1) {
        inspection.issues.push(`Multiple price traces detected: ${priceTraces.length}`);
        inspection.overlappingDetected = true;
    }

    // Check for traces on wrong y-axes
    const mainAxisTraces = traces.filter(t => t.yaxis === 'y' || !t.yaxis);
    if (mainAxisTraces.length > 1) {
        inspection.issues.push(`Multiple traces on main y-axis: ${mainAxisTraces.map(t => t.name).join(', ')}`);
        inspection.overlappingDetected = true;
    }

    // Check for duplicate trace names
    const traceNames = traces.map(t => t.name);
    const duplicateNames = traceNames.filter((name, index) => traceNames.indexOf(name) !== index);
    if (duplicateNames.length > 0) {
        inspection.issues.push(`Duplicate trace names: ${[...new Set(duplicateNames)].join(', ')}`);
        inspection.overlappingDetected = true;
    }


    return inspection;
};

// DEBUG: Comprehensive diagnostic report for data overlapping issues
window.diagnoseDataOverlapping = function() {

    const diagnosis = {
        timestamp: new Date().toISOString(),
        issues: [],
        recommendations: [],
        severity: 'low'
    };

    // 1. Chart data inspection
    const chartInspection = window.inspectChartData();
    if (chartInspection.overlappingDetected) {
        diagnosis.issues.push(...chartInspection.issues);
        diagnosis.severity = 'high';
    }

    // 2. Data merging analysis
    const dataAnalysis = window.analyzeDataMerging();
    if (dataAnalysis.duplicates > 0) {
        diagnosis.issues.push(`Found ${dataAnalysis.duplicates} duplicate timestamps`);
        diagnosis.recommendations.push('Check data merging logic in mergeDataPoints functions');
    }
    if (dataAnalysis.gaps > 0) {
        diagnosis.issues.push(`Found ${dataAnalysis.gaps} data gaps`);
    }

    // 3. Layout inspection
    const layoutInspection = window.inspectLayout();
    if (layoutInspection.issues.length > 0) {
        diagnosis.issues.push(...layoutInspection.issues);
        diagnosis.severity = 'high';
    }

    // 4. WebSocket status
    const wsStatus = window.checkWebSocketStatus();

    // 5. Current state

    // 6. Message queue status
    const queueStatus = window.getMessageQueueStatus();

    // Summary and recommendations

    if (diagnosis.issues.length > 0) {
        diagnosis.issues.forEach((issue, index) => {
        });
    }

    if (diagnosis.recommendations.length > 0) {
        diagnosis.recommendations.forEach((rec, index) => {
        });
    }

    // Quick fix suggestions

    return diagnosis;
};

// DEBUG: Quick fix functions
window.clearChartData = function() {
    if (window.gd) {
        Plotly.react(window.gd, [], window.gd.layout || {});
    } else {
    }
};

window.forceReloadChart = function() {
    if (window.gd) {
        // Clear and reinitialize
        Plotly.react(window.gd, [], {});
        // Reconnect WebSocket
        if (combinedSymbol) {
            setupCombinedWebSocket(combinedSymbol, combinedIndicators, combinedResolution, combinedFromTs, combinedToTs);
        }
    } else {
    }
};

window.resetWebSocket = function() {
    closeCombinedWebSocket("Manual reset for debugging");
    setTimeout(() => {
        if (combinedSymbol) {
            setupCombinedWebSocket(combinedSymbol, combinedIndicators, combinedResolution, combinedFromTs, combinedToTs);
        }
    }, 1000);
};

// DEBUG: Test data clearing on resolution changes
window.testResolutionChangeDataClearing = function() {

    // Get current state
    const originalResolution = combinedResolution;
    const originalDataCount = window.gd && window.gd.data ? window.gd.data.length : 0;


    // Simulate resolution change from 1h to 1d
    const newResolution = originalResolution === '1h' ? '1d' : '1h';

    return {
        originalResolution,
        newResolution,
        originalDataCount,
        testCompleted: true
    };
};

// DEBUG: Monitor chart updates for overlapping
window.monitorChartUpdates = function(duration = 30000) {

    let updateCount = 0;
    let lastTraceCount = window.gd && window.gd.data ? window.gd.data.length : 0;
    let overlappingDetected = false;

    const monitorInterval = setInterval(() => {
        if (!window.gd || !window.gd.data) {
            clearInterval(monitorInterval);
            return;
        }

        const currentTraceCount = window.gd.data.length;
        updateCount++;

        if (currentTraceCount !== lastTraceCount) {

            // Check for potential overlapping
            const priceTraces = window.gd.data.filter(t => t.type === 'candlestick');
            if (priceTraces.length > 1) {
                overlappingDetected = true;
            }

            lastTraceCount = currentTraceCount;
        }
    }, 1000); // Check every second

    // Stop monitoring after duration
    setTimeout(() => {
        clearInterval(monitorInterval);
    }, duration);

    return {
        monitoring: true,
        duration,
        monitorInterval
    };
};

// DEBUG: Check data merging logic for duplicates and overlaps
window.analyzeDataMerging = function() {
    if (!window.gd || !window.gd.data) {
        return { error: 'No chart data' };
    }

    const priceTrace = window.gd.data.find(t => t.type === 'candlestick');
    if (!priceTrace || !priceTrace.x) {
        return { error: 'No price data' };
    }

    const timestamps = priceTrace.x.map(t => t.getTime());
    const analysis = {
        totalPoints: timestamps.length,
        duplicates: 0,
        gaps: 0,
        timeRange: {
            start: new Date(Math.min(...timestamps)).toISOString(),
            end: new Date(Math.max(...timestamps)).toISOString()
        }
    };

    // Check for duplicate timestamps
    const seenTimestamps = new Set();
    timestamps.forEach(ts => {
        if (seenTimestamps.has(ts)) {
            analysis.duplicates++;
        }
        seenTimestamps.add(ts);
    });

    // Check for gaps in data
    const sortedTimestamps = [...timestamps].sort((a, b) => a - b);
    const expectedInterval = getTimeframeSecondsJS(combinedResolution) * 1000; // Convert to milliseconds

    for (let i = 1; i < sortedTimestamps.length; i++) {
        const gap = sortedTimestamps[i] - sortedTimestamps[i - 1];
        if (gap > expectedInterval * 1.5) { // Allow 50% tolerance
            analysis.gaps++;
        }
    }


    return analysis;
};

// DEBUG: Inspect subplot layout for overlapping issues
window.inspectLayout = function() {
    if (!window.gd || !window.gd.layout) {
        return { error: 'No layout' };
    }

    const layout = window.gd.layout;
    const inspection = {
        hasGrid: !!layout.grid,
        gridRows: layout.grid ? layout.grid.rows : 0,
        gridColumns: layout.grid ? layout.grid.columns : 0,
        yAxes: [],
        xAxes: [],
        issues: []
    };

    // Check Y-axes
    Object.keys(layout).forEach(key => {
        if (key.startsWith('yaxis')) {
            const axis = layout[key];
            inspection.yAxes.push({
                name: key,
                domain: axis.domain,
                range: axis.range,
                title: axis.title ? axis.title.text : 'No title'
            });
        }
    });

    // Check X-axes
    Object.keys(layout).forEach(key => {
        if (key.startsWith('xaxis')) {
            const axis = layout[key];
            inspection.xAxes.push({
                name: key,
                domain: axis.domain,
                range: axis.range
            });
        }
    });


    inspection.yAxes.forEach(axis => {
    });

    // Check for overlapping domains
    for (let i = 0; i < inspection.yAxes.length - 1; i++) {
        const current = inspection.yAxes[i];
        const next = inspection.yAxes[i + 1];

        if (current.domain && next.domain) {
            const currentEnd = current.domain[1];
            const nextStart = next.domain[0];

            if (currentEnd > nextStart) {
                inspection.issues.push(`Overlapping Y-axis domains: ${current.name} ends at ${currentEnd}, ${next.name} starts at ${nextStart}`);
            }
        }
    }

    // Check for missing domains
    inspection.yAxes.forEach(axis => {
        if (!axis.domain) {
            inspection.issues.push(`Missing domain for ${axis.name}`);
        }
    });


    return inspection;
};

window.clearMessageQueue = function() {
    const clearedCount = messageQueue.length;
    messageQueue = [];
    return clearedCount;
};

window.forceReleaseChartLock = function() {
    if (chartUpdateLock) {
        releaseChartUpdateLock();
        return true;
    } else {
        return false;
    }
};

// DEBUG: Get WebSocket logs for debugging
window.getWebSocketLogs = function(limit = 20) {
    const logsToShow = websocketLogs.slice(-limit);

    if (logsToShow.length === 0) {
        return [];
    }

    logsToShow.forEach((log, index) => {
        const time = new Date(log.timestamp).toLocaleTimeString();
    });

    return logsToShow;
};

// DEBUG: Clear WebSocket logs
window.clearWebSocketLogs = function() {
    const clearedCount = websocketLogs.length;
    websocketLogs.length = 0;
    return clearedCount;
};

// DEBUG: Fix for OHLC to vertical lines issue
window.fixCandlestickDisplay = function() {
    if (!window.gd || !window.gd.data) {
        console.warn('🔧 No chart data available for candlestick fix');
        return false;
    }

    const candlestickTrace = window.gd.data.find(trace => trace.type === 'candlestick');
    if (!candlestickTrace) {
        console.warn('🔧 No candlestick trace found');
        return false;
    }

    console.log('[DEBUG] Fixing candlestick display - current trace:', {
        type: candlestickTrace.type,
        points: candlestickTrace.x ? candlestickTrace.x.length : 0,
        hasOHLC: !!(candlestickTrace.open && candlestickTrace.high && candlestickTrace.low && candlestickTrace.close)
    });

    // Force candlestick display by ensuring all required OHLC arrays exist and are properly formatted
    const requiredArrays = ['open', 'high', 'low', 'close'];
    let needsFix = false;

    requiredArrays.forEach(arrayName => {
        if (!candlestickTrace[arrayName] || !Array.isArray(candlestickTrace[arrayName])) {
            console.warn(`🔧 Missing or invalid ${arrayName} array, creating new one`);
            candlestickTrace[arrayName] = [];
            needsFix = true;
        }
    });

    // Ensure all OHLC arrays have the same length
    const maxLength = Math.max(
        candlestickTrace.open.length,
        candlestickTrace.high.length,
        candlestickTrace.low.length,
        candlestickTrace.close.length
    );

    if (candlestickTrace.x && candlestickTrace.x.length !== maxLength) {
        console.warn(`🔧 X-axis length (${candlestickTrace.x.length}) doesn't match OHLC arrays (${maxLength})`);
        needsFix = true;
    }

    if (needsFix) {
        // Force a complete chart redraw with proper candlestick configuration
        const fixedTrace = {
            x: candlestickTrace.x || [],
            open: candlestickTrace.open || [],
            high: candlestickTrace.high || [],
            low: candlestickTrace.low || [],
            close: candlestickTrace.close || [],
            volume: candlestickTrace.volume || [],
            type: 'candlestick',
            xaxis: 'x',
            yaxis: 'y',
            name: candlestickTrace.name || 'Price',
            increasing: { line: { color: 'green' } },
            decreasing: { line: { color: 'red' } },
            hoverinfo: 'all'
        };

        // Replace the problematic trace
        const traceIndex = window.gd.data.findIndex(trace => trace.type === 'candlestick');
        if (traceIndex !== -1) {
            window.gd.data[traceIndex] = fixedTrace;

            // Force chart update
            Plotly.react(window.gd, window.gd.data, window.gd.layout).then(() => {
                console.log('✅ Candlestick display fixed successfully');
                return true;
            }).catch((error) => {
                console.error('❌ Error fixing candlestick display:', error);
                return false;
            });
        }
    } else {
        console.log('✅ Candlestick display is already correct');
        return true;
    }
};

// DEBUG: Monitor for candlestick display issues
window.monitorCandlestickDisplay = function(interval = 5000) {
    const monitorInterval = setInterval(() => {
        if (!window.gd || !window.gd.data) return;

        const candlestickTrace = window.gd.data.find(trace => trace.type === 'candlestick');
        if (!candlestickTrace) {
            console.warn('🚨 MONITOR: No candlestick trace found!');
            return;
        }

        // Check if OHLC arrays are properly maintained
        const issues = [];

        if (!candlestickTrace.open || candlestickTrace.open.length === 0) issues.push('Missing open array');
        if (!candlestickTrace.high || candlestickTrace.high.length === 0) issues.push('Missing high array');
        if (!candlestickTrace.low || candlestickTrace.low.length === 0) issues.push('Missing low array');
        if (!candlestickTrace.close || candlestickTrace.close.length === 0) issues.push('Missing close array');

        // Check for length mismatches
        const lengths = [candlestickTrace.open, candlestickTrace.high, candlestickTrace.low, candlestickTrace.close]
            .filter(arr => arr)
            .map(arr => arr.length);

        if (lengths.length > 0) {
            const minLength = Math.min(...lengths);
            const maxLength = Math.max(...lengths);
            if (maxLength !== minLength) {
                issues.push(`OHLC array length mismatch: ${minLength}-${maxLength}`);
            }
        }

        if (issues.length > 0) {
            console.warn('🚨 MONITOR: Candlestick display issues detected:', issues);
            console.warn('🚨 MONITOR: Auto-fixing candlestick display...');

            // Auto-fix the issue
            window.fixCandlestickDisplay();
        } else {
            console.log('✅ MONITOR: Candlestick display is healthy');
        }
    }, interval);

    return monitorInterval;
};

// DEBUG: Force candlestick mode validation
window.validateCandlestickMode = function() {
    if (!window.gd) {
        return { valid: false, error: 'No chart found' };
    }

    const validation = {
        valid: true,
        issues: [],
        candlestickTrace: null
    };

    const candlestickTrace = window.gd.data.find(trace => trace.type === 'candlestick');
    if (!candlestickTrace) {
        validation.valid = false;
        validation.issues.push('No candlestick trace found');
        return validation;
    }

    validation.candlestickTrace = {
        name: candlestickTrace.name,
        points: candlestickTrace.x ? candlestickTrace.x.length : 0,
        hasOpen: !!(candlestickTrace.open && candlestickTrace.open.length > 0),
        hasHigh: !!(candlestickTrace.high && candlestickTrace.high.length > 0),
        hasLow: !!(candlestickTrace.low && candlestickTrace.low.length > 0),
        hasClose: !!(candlestickTrace.close && candlestickTrace.close.length > 0),
        openLength: candlestickTrace.open ? candlestickTrace.open.length : 0,
        highLength: candlestickTrace.high ? candlestickTrace.high.length : 0,
        lowLength: candlestickTrace.low ? candlestickTrace.low.length : 0,
        closeLength: candlestickTrace.close ? candlestickTrace.close.length : 0
    };

    // Check for missing OHLC arrays
    if (!validation.candlestickTrace.hasOpen) validation.issues.push('Missing open array');
    if (!validation.candlestickTrace.hasHigh) validation.issues.push('Missing high array');
    if (!validation.candlestickTrace.hasLow) validation.issues.push('Missing low array');
    if (!validation.candlestickTrace.hasClose) validation.issues.push('Missing close array');

    // Check for length mismatches
    const lengths = [
        validation.candlestickTrace.openLength,
        validation.candlestickTrace.highLength,
        validation.candlestickTrace.lowLength,
        validation.candlestickTrace.closeLength
    ].filter(l => l > 0);

    if (lengths.length > 0) {
        const uniqueLengths = [...new Set(lengths)];
        if (uniqueLengths.length > 1) {
            validation.issues.push(`OHLC array length mismatch: ${uniqueLengths.join(', ')}`);
            validation.valid = false;
        }
    }

    if (validation.issues.length > 0) {
        validation.valid = false;
    }

    return validation;
};

// Define global function to process historical data for chart updates
window.processHistoricalDataForChart = function(dataPoints, symbol) {
    updateChartWithHistoricalData(dataPoints, symbol);
};


