
async function handleNewShapeSave(shapeObject) {
    const symbol = window.symbolSelect.value; // Assumes symbolSelect is global
    const resolution = window.resolutionSelect.value;
    if (!symbol) {
        console.warn("Cannot save drawing: No symbol selected.");
        return null;
    }

    if ((shapeObject.type === 'line' || shapeObject.type === 'rect') && typeof shapeObject.x0 !== 'undefined' && typeof shapeObject.y0 !== 'undefined' && typeof shapeObject.x1 !== 'undefined' && typeof shapeObject.y1 !== 'undefined') {
        try {
            const start_time_ms = (shapeObject.x0 instanceof Date) ? shapeObject.x0.getTime() : new Date(shapeObject.x0).getTime();
            const end_time_ms = (shapeObject.x1 instanceof Date) ? shapeObject.x1.getTime() : new Date(shapeObject.x1).getTime();
            const drawingData = {
                symbol: symbol,
                type: shapeObject.type,
                start_time: Math.floor(start_time_ms / 1000),
                end_time: Math.floor(end_time_ms / 1000),
                start_price: parseFloat(shapeObject.y0),
                end_price: parseFloat(shapeObject.y1),
                subplot_name: determineSubplotNameForShape(shapeObject), // Assumes determineSubplotNameForShape is global
                resolution: resolution,
                properties: {
                    sendEmailOnCross: true,
                    buyOnCross: false,
                    sellOnCross: false
                }
            };

            // Send shape data via WebSocket using wsAPI
            if (window.wsAPI) {
                // If WebSocket is not connected yet, wait for it to connect
                if (!window.wsAPI.connected) {
                    console.log('WebSocket not connected yet, waiting for connection...');
                    // Wait for connection with timeout
                    let connectionAttempts = 0;
                    while (!window.wsAPI.connected && connectionAttempts < 50) { // 5 seconds max
                        await new Promise(resolve => setTimeout(resolve, 100));
                        connectionAttempts++;
                    }

                    if (!window.wsAPI.connected) {
                        throw new Error('WebSocket connection timeout - please refresh the page');
                    }
                }
                const shapeMessage = {
                    type: 'shape',
                    action: 'save',
                    data: drawingData,
                    request_id: Date.now().toString()
                };

                // Set up a promise to wait for the shape_success response
                const shapeSavePromise = new Promise((resolve, reject) => {
                    const timeout = setTimeout(() => {
                        reject(new Error('Timeout waiting for shape save confirmation'));
                    }, 5000); // 5 second timeout

                    const messageHandler = (message) => {
                        if (message.type === 'shape_success' && message.request_id === shapeMessage.request_id) {
                            clearTimeout(timeout);
                            window.wsAPI.offMessage('shape_success', messageHandler);
                            resolve(message.data);
                        } else if (message.type === 'error' && message.request_id === shapeMessage.request_id) {
                            clearTimeout(timeout);
                            window.wsAPI.offMessage('error', messageHandler);
                            reject(new Error(message.message || 'Failed to save shape'));
                        }
                    };

                    // Listen for both success and error messages
                    window.wsAPI.onMessage('shape_success', messageHandler);
                    window.wsAPI.onMessage('error', messageHandler);
                });

                window.wsAPI.sendMessage(shapeMessage);
                const result = await shapeSavePromise;

                // Validate the result ID
                if (!result || !result.id || result.id === 'undefined' || result.id === undefined || result.id === null || result.id === '') {
                    console.warn('Invalid or missing ID in shape save response:', result);
                    return null;
                }

                // Request volume profile calculation for rectangles immediately after saving
                if (shapeObject.type === 'rect' && result.id) {
                    const volumeProfileRequest = {
                        type: 'get_volume_profile',
                        rectangle_id: result.id,
                        symbol: window.symbolSelect ? window.symbolSelect.value : window.activeSymbol,
                        resolution: window.resolutionSelect ? window.resolutionSelect.value : '1h'
                    };
                    try {
                        window.wsAPI.sendMessage(volumeProfileRequest);
                    } catch (error) {
                        console.error('📊 Failed to send volume profile request:', error);
                    }
                }

                return result.id;
            } else {
                throw new Error('WebSocket API not available');
            }
        } catch (error) {
            console.error('Failed to save drawing:', error);
            alert(`Failed to save drawing: ${error.message}`);
            // Note: loadDrawingsAndRedraw is not defined, so we skip this call
            return null;
        }
    }
    return null;
}

function initializePlotlyEventHandlers(gd) {


    // Add test event listeners to verify event system is working
    gd.on('plotly_click', function() {
    });

    // Add keyboard shortcut for testing (press 'T' to trigger test event)
    document.addEventListener('keydown', function(event) {

        // Add keyboard shortcut for testing YouTube modal (press 'Y' to test modal)
        if (event.key === 'y' || event.key === 'Y') {
            try {
                if (window.youtubeMarkersManager && window.youtubeMarkersManager.showTranscriptModal) {
                    window.youtubeMarkersManager.showTranscriptModal(
                        'Test Video Title',
                        'This is a test transcript content for debugging purposes.',
                        'dQw4w9WgXcQ',
                        '2024-01-15'
                    );
                } else {
                    console.warn('[DEBUG] YouTube markers manager not available for testing');
                }
            } catch (error) {
                console.error('[DEBUG] YouTube modal test failed:', error);
            }
        }
    });

    let currentDragMode = gd.layout.dragmode || 'zoom';
    let previousDragMode = gd.layout.dragmode || 'zoom'; // Store previous mode for restoration
    let isDragging = false;
    let shapeWasMoved = false;
    
    /* OLD DRAG DETECTION DISABLED - Using SVG observer instead
    // Track drag state
    gd.addEventListener('mousedown', function() {
        isDragging = true;
    });
    
    gd.addEventListener('mouseup', function() {
        isDragging = false;
        if (shapeWasMoved) {
            currentDragMode = gd.layout.dragmode;
            Plotly.relayout(gd, 'dragmode', 'drawline');
            shapeWasMoved = false;
        }
    });
    
    // Detect shape movement
    gd.on('plotly_relayout', function(eventData) {
        const shapeKeys = Object.keys(eventData).filter(key => key.startsWith('shapes['));
        if (shapeKeys.length > 0 && isDragging) {
            shapeWasMoved = true;
        }
    });
    */
    
    gd.on('plotly_shapedrawn', async function(eventShapeData) {
        console.log('[SHAPE DRAWN] plotly_shapedrawn event fired');
        console.log('[SHAPE DRAWN] eventShapeData:', eventShapeData);
        console.log('[SHAPE DRAWN] Current dragmode:', gd.layout.dragmode);

        // The shape drawn is usually the last one added to gd.layout.shapes
        // and it won't have an id yet.
        const currentLayoutShapes = gd.layout.shapes || [];
        let newlyAddedShapeInLayout = null;

        if (currentLayoutShapes.length > 0) {
            const lastShapeInLayout = currentLayoutShapes[currentLayoutShapes.length - 1];

            // Basic check: if it's a line, has no id, and isn't already being processed.
            // A more robust match would compare coordinates if Plotly guarantees eventShapeData matches.
            if (lastShapeInLayout.type === eventShapeData.type &&
                !lastShapeInLayout.id &&
                !lastShapeInLayout.isSystemShape && // Should not be needed for user-drawn shapes
                !lastShapeInLayout._savingInProgress) {
                newlyAddedShapeInLayout = lastShapeInLayout;
            }
        }

        if (newlyAddedShapeInLayout) {
            newlyAddedShapeInLayout._savingInProgress = true; // Set flag on the actual layout shape

            // Ensure the layout shape has editable and layer properties consistent with eventShapeData or defaults
            newlyAddedShapeInLayout.editable = true;
            if (newlyAddedShapeInLayout.line) {
                newlyAddedShapeInLayout.line.layer = 'above';
            } else if (newlyAddedShapeInLayout.type === 'line') { // Ensure line property exists
                newlyAddedShapeInLayout.line = { color: DEFAULT_DRAWING_COLOR, width: 2, layer: 'above' };
            }

            // Add larger markers for mobile touch targets (only for user-drawn lines)
            if (newlyAddedShapeInLayout.type === 'line' && !newlyAddedShapeInLayout.isSystemShape) {
                newlyAddedShapeInLayout.marker = {
                    size: isMobileDevice() ? 34 : 24, // Even larger markers for maximum visibility
                    color: DEFAULT_DRAWING_COLOR,
                    symbol: 'circle', // Circle markers for line endpoints
                    line: { width: 3, color: 'white' }, // Thicker white border
                    opacity: 0.95 // Make markers more opaque for better visibility
                };
            }

            // Pass the layout shape to handleNewShapeSave, as it has resolved xref/yref
            const backendId = await handleNewShapeSave(newlyAddedShapeInLayout);

            if (backendId) {
                newlyAddedShapeInLayout.id = backendId;
                // editable is already true
                window.activeShapeForPotentialDeletion = { id: backendId, index: currentLayoutShapes.indexOf(newlyAddedShapeInLayout), shape: newlyAddedShapeInLayout };
                updateSelectedShapeInfoPanel(window.activeShapeForPotentialDeletion);
                await updateShapeVisuals();
            } else {
                console.warn('[plotly_shapedrawn] handleNewShapeSave did not return a backendId. Removing shape from layout to prevent duplicates.');
                const indexToRemove = currentLayoutShapes.indexOf(newlyAddedShapeInLayout);
                if (indexToRemove > -1) currentLayoutShapes.splice(indexToRemove, 1);
                Plotly.relayout(gd, {shapes: currentLayoutShapes}); // Update layout
            }
            delete newlyAddedShapeInLayout._savingInProgress; // Clear the flag
        } else {
            console.warn('[plotly_shapedrawn] Could not identify the newly drawn shape in gd.layout.shapes or it was already processed. Relayout might handle it.');
        }
    });

    const originalRelayoutHandler = gd.onplotly_relayout;

    let shapeDragEndTimer = null;
    let settingsSaveTimer = null;
    let panTimer = null; // Timer for pan events like trade filter timer
    const DEBOUNCE_DELAY = 500; // ms
    const SETTINGS_SAVE_DEBOUNCE_DELAY = 3000; // 3 seconds for settings save during chart resize
    const PAN_TIMER_DELAY = 2000; // 2 seconds for pan timer

    // Add mouse event listeners to detect dragging
    let isMouseDown = false;
    let dragStartTime = null;

    gd.addEventListener('mousedown', function(event) {
        isMouseDown = true;
        dragStartTime = Date.now();
        console.log('🖱️ MOUSE DOWN: Detected mouse down event, isDraggingShape =', window.isDraggingShape);
    });

    gd.addEventListener('mousemove', function(event) {
        if (isMouseDown && dragStartTime) {
            const dragDuration = Date.now() - dragStartTime;
            if (dragDuration > 100) { // Only consider it dragging after 100ms
                // console.log('🔄 MOUSE DRAG: Mouse is being dragged, checking for shape interaction, isDraggingShape =', window.isDraggingShape);

                // More robust check: look for drag helper elements that Plotly creates during dragging
                const hasDragHelpers = document.querySelectorAll('g[drag-helper="true"]').length > 0;
                const shapes = gd.layout.shapes || [];
                const hasEditableShapes = shapes.some(shape => shape.editable === true);

                // console.log('🔄 DRAG CHECK: hasDragHelpers =', hasDragHelpers, 'hasEditableShapes =', hasEditableShapes);

                if ((hasDragHelpers || hasEditableShapes) && !window.isDraggingShape) {
                    console.log('🔄 DRAG START: Setting isDraggingShape = true (detected drag helpers or editable shapes)');
                    window.isDraggingShape = true;
                }
            }
        }
    });

    gd.addEventListener('mouseup', function(event) {
        if (isMouseDown) {
            console.log('🖱️ MOUSE UP: Detected mouse up event, isDraggingShape =', window.isDraggingShape);
            console.log('🔄 DRAG END: Setting isDraggingShape = false (from mouse events)');
            window.isDraggingShape = false;
        }
        isMouseDown = false;
        dragStartTime = null;
    });

    gd.on('plotly_relayouting', function(eventData) {
        console.log('🔄 PLOTLY_RELAYOUTING EVENT:', eventData);

        // Clean up duplicate drag helpers that might interfere with dragging
        if (window.d3) {
            d3.selectAll('g[drag-helper="true"]').remove();
        }

        // Analyze the eventData to distinguish drag types
        const eventKeys = Object.keys(eventData);
        const hasShapeChanges = eventKeys.some(key => key.startsWith('shapes['));
        const hasAxisRangeChanges = eventKeys.some(key => key.includes('axis.range'));
        const isDragModeChange = eventKeys.includes('dragmode');

        console.log('🔄 Event analysis:', {
            eventKeys,
            hasShapeChanges,
            hasAxisRangeChanges,
            isDragModeChange,
            currentIsDraggingShape: window.isDraggingShape
        });

        // Only set dragging flag for actual shape interactions, NOT dragmode changes
        if (hasShapeChanges && !isDragModeChange) {
            console.log('🔄 DRAG START: Setting isDraggingShape = true (from plotly_relayouting)');
            window.isDraggingShape = true;

            // Store current mode before switching
            previousDragMode = gd.layout.dragmode;

            // Switch to drawline mode when dragging shapes
            if (gd.layout.dragmode !== 'drawline') {
                Plotly.relayout(gd, { dragmode: 'drawline' });
            }

            // 🚨 Live data is always enabled now - no need to disable during dragging
        } else if (isDragModeChange) {
            // Handle dragmode changes separately - don't set dragging flag
            // Allow line coloring in draw mode by not setting isDraggingShape
        } else {
            // For axis range changes or unknown events, don't set dragging flag
            if (window.isDraggingShape && !hasShapeChanges) {
                console.log('🔄 DRAG END: Setting isDraggingShape = false (from plotly_relayouting - no shape changes)');
                window.isDraggingShape = false;
            }
        }

        // Prevent line disappearing during drag by ensuring visual updates happen after drag mode changes
        if (window.isDraggingShape && hasShapeChanges) {
            // Delay visual updates slightly to avoid conflicts with drag mode switching
            setTimeout(() => {
                if (window.updateShapeVisuals && !window.isDraggingShape) {
                    // Only update if we're still not dragging (drag might have ended)
                    return;
                }
                // Force a visual update to keep the line visible during drag
                if (window.updateShapeVisuals) {
                    window.updateShapeVisuals();
                }
            }, 10);
        }
    });


    // Handle Plotly click events for better coordinate handling
    gd.on('plotly_click', function(plotlyEventData) {

        // Check if YouTube markers are loaded
        const youtubeTraces = gd.data ? gd.data.filter(trace => trace.name === 'YouTube Videos') : [];

        // Add manual click test for debugging
        if (youtubeTraces.length > 0 && window.youtubeMarkersManager && window.youtubeMarkersManager.showTranscriptModal) {
            try {
                const firstTrace = youtubeTraces[0];
                if (firstTrace.x && firstTrace.x.length > 0) {
                    const testTitle = firstTrace.text ? firstTrace.text[0] : 'Test Title';
                    const testTranscript = firstTrace.transcripts ? firstTrace.transcripts[0] : 'Test transcript content';
                    const testVideoId = firstTrace.video_ids ? firstTrace.video_ids[0] : '';
                    const testPublishedDate = firstTrace.customdata ? firstTrace.customdata[0] : '';

                    window.youtubeMarkersManager.showTranscriptModal(testTitle, testTranscript, testVideoId, testPublishedDate);
                }
            } catch (error) {
                console.error('[DEBUG] Modal test failed:', error);
            }
        }

        // Plotly provides better coordinate handling
        if (plotlyEventData.points && plotlyEventData.points.length > 0) {
            const point = plotlyEventData.points[0];

            // Check if this is a YouTube marker click first
            if (point.fullData && point.fullData.name === 'YouTube Videos') {

                // Get YouTube marker data and open the modal
                const pointIndex = point.pointIndex;
                const transcript = point.fullData.transcripts ?
                    point.fullData.transcripts[pointIndex] : 'No description available';
                const title = point.fullData.text ?
                    point.fullData.text[pointIndex] : 'Unknown title';
                const videoId = point.fullData.video_ids ?
                    point.fullData.video_ids[pointIndex] : '';
                const publishedDate = point.fullData.customdata ?
                    point.fullData.customdata[pointIndex] : '';

                // Open the YouTube video description modal
                if (window.youtubeMarkersManager && window.youtubeMarkersManager.showTranscriptModal) {
                    window.youtubeMarkersManager.showTranscriptModal(title, transcript, videoId, publishedDate);
                } else {
                    console.warn('[DEBUG] YouTube markers manager not available');
                }

                return; // Don't process as a drawing shape
            }

            // Check if this is a YouTube marker from the shape selection system
            if (point.marker && point.marker.symbol === 'diamond' && point.marker.color === 'red') {

                // Try to get YouTube data from the point
                const pointIndex = point.pointIndex;
                const traceData = point.fullData;

                // Extract YouTube data if available
                const transcript = traceData.transcripts ?
                    traceData.transcripts[pointIndex] : 'No description available';
                const title = traceData.text ?
                    traceData.text[pointIndex] : 'Unknown title';
                const videoId = traceData.video_ids ?
                    traceData.video_ids[pointIndex] : '';
                const publishedDate = traceData.customdata ?
                    traceData.customdata[pointIndex] : '';

                // Open the YouTube video description modal
                if (window.youtubeMarkersManager && window.youtubeMarkersManager.showTranscriptModal) {
                    window.youtubeMarkersManager.showTranscriptModal(title, transcript, videoId, publishedDate);
                } else {
                    console.warn('[DEBUG] YouTube markers manager not available');
                }

                return; // Don't process as a regular drawing shape
            }

        }
    });



    // Add custom double-click handler using DOM event
    const chartElement = document.getElementById('chart');
    if (chartElement) {
        chartElement.addEventListener('dblclick', function(event) {
            console.log('[DOUBLE-CLICK] Custom double-click detected on chart element');
            alert('Double-click detected! This would normally zoom out/reset the chart view.');
            event.preventDefault();
            event.stopPropagation();
            return false;
        });
    }

    gd.on('plotly_relayout', async function(eventData) {

        // 🚨 PANNING DETECTION 🚨 - Check for ANY range changes
        const hasXRangeChange = eventData['xaxis.range[0]'] !== undefined || eventData['xaxis.range[1]'] !== undefined;
        const hasYRangeChange = eventData['yaxis.range[0]'] !== undefined || eventData['yaxis.range[1]'] !== undefined;
        const hasAutorange = eventData['xaxis.autorange'] === true || eventData['yaxis.autorange'] === true;

        const isAnyRangeChange = hasXRangeChange || hasYRangeChange || hasAutorange;

        // Skip processing if relayout events are being ignored (programmatic updates)
        if (window.ignoreRelayoutEvents && isAnyRangeChange) {
            return;
        }

        if (isAnyRangeChange) {
            // Extract x-axis range values for debugging
            const xMin = gd.layout.xaxis?.range?.[0];
            const xMax = gd.layout.xaxis?.range?.[1];

            // Convert to timestamps, accounting for timezone offset
            // Plotly displays in local time, but we need UTC timestamps for the server
            // getTimezoneOffset() returns minutes, convert to milliseconds
            let xMinTimestamp = null;
            let xMaxTimestamp = null;

            if (xMin) {
                const minDate = new Date(xMin);
                // getTimezoneOffset() returns the offset in minutes from UTC to local time
                // For UTC+2, it returns 120 (local is 120 minutes ahead of UTC)
                // To get UTC timestamp from local time, we need to subtract this offset
                const timezoneOffsetMs = minDate.getTimezoneOffset() * 60 * 1000;
                xMinTimestamp = Math.floor((minDate.getTime() - timezoneOffsetMs) / 1000);
            }

            if (xMax) {
                const maxDate = new Date(xMax);
                const timezoneOffsetMs = maxDate.getTimezoneOffset() * 60 * 1000;
                xMaxTimestamp = Math.floor((maxDate.getTime() - timezoneOffsetMs) / 1000);
            }

            /*
                xRangeChanged: hasXRangeChange,
                yRangeChanged: hasYRangeChange,
                autorange: hasAutorange,
                xRange: [xMin, xMax],
                xTimestamps: [xMinTimestamp, xMaxTimestamp],
                xRangeHuman: xMin && xMax ? `${new Date(xMin).toISOString()} to ${new Date(xMax).toISOString()}` : 'N/A',
                yRange: gd.layout.yaxis?.range,
                timestamp: new Date().toISOString(),
                eventData: eventData
            });
            */

            // Specific logging for server comparison
            if (xMinTimestamp && xMaxTimestamp) {
                // xMinTimestamp and xMaxTimestamp are already in seconds (converted above)
                const clientRange = {
                    xMinTimestamp,
                    xMaxTimestamp,
                    xMinDate: new Date(xMinTimestamp * 1000).toISOString(),
                    xMaxDate: new Date(xMaxTimestamp * 1000).toISOString(),
                    rangeSeconds: xMaxTimestamp - xMinTimestamp,
                    rangeHours: (xMaxTimestamp - xMinTimestamp) / 3600
                };


                // Store for comparison with server response
                window.lastClientRange = clientRange;
            }


            // Dispatch custom event for other parts of the app to listen to
            const panEvent = new CustomEvent('chartPanned', {
                detail: {
                    xRange: gd.layout.xaxis?.range,
                    yRange: gd.layout.yaxis?.range,
                    eventData: eventData
                }
            });
            document.dispatchEvent(panEvent);

            // Update UI indicator
            updatePanningStatus(true);

            // Reset status after 3 seconds
            delay(3000).then(() => {
                updatePanningStatus(false);
            });
        }

        if(eventData.shape) {
            return;
        }
        if (originalRelayoutHandler) originalRelayoutHandler(eventData);

        let interactedShapeIndex = -1;
        for (const key in eventData) {
            if (key.startsWith('shapes[') && key.includes('].x')) {
                const match = key.match(/shapes\[(\d+)\]/);
                if (match && match[1]) {
                    interactedShapeIndex = parseInt(match[1], 10);
                    break;
                }
            }
        }

        if (interactedShapeIndex !== -1 && gd.layout.shapes && gd.layout.shapes[interactedShapeIndex]) {
            const interactedShape = gd.layout.shapes[interactedShapeIndex];
            if (interactedShape.id && !interactedShape.isSystemShape) {

                // --- Visual feedback on drag start (Re-enabled with safeguards to prevent line disappearing)---
                // Only apply temporary color if not currently dragging to avoid conflicts
                if (!window.isDraggingShape && interactedShape.line && interactedShape.line.color !== 'rgba(255, 0, 255, 0.5)') {
                    const currentShapes = [...gd.layout.shapes];
                    currentShapes[interactedShapeIndex].line.color = 'rgba(255, 0, 255, 0.5)'; // Temporary color
                    Plotly.relayout(gd, { shapes: currentShapes });
                }
                // --- Debounced save on drag end ---
                clearTimeout(shapeDragEndTimer);
                shapeDragEndTimer = delay(DEBOUNCE_DELAY).then(async () => {

                    // It's possible the shape was deleted or changed, so we get the latest version
                    const finalShapeState = gd.layout.shapes[interactedShapeIndex];
                    if (finalShapeState && finalShapeState.id === interactedShape.id) {
                        const saveSuccess = await sendShapeUpdateToServer(finalShapeState, window.symbolSelect.value);

                        if (!saveSuccess) {
                            console.warn(`[plotly_relayout] Shape update failed, reloading drawings`);
                            loadDrawingsAndRedraw(window.symbolSelect.value);
                        } else {
                            // Restore original color after successful save
                            await updateShapeVisuals(); // This should handle restoring the color
                            // Force immediate visual update to ensure line is visible after save
                            delay(25).then(() => {
                                if (window.updateShapeVisuals) {
                                    window.updateShapeVisuals();
                                }
                            });
                        }
                    }
                });

                const selectionChanged = !window.activeShapeForPotentialDeletion || window.activeShapeForPotentialDeletion.id !== interactedShape.id;
                if (selectionChanged) {
                    window.activeShapeForPotentialDeletion = { id: interactedShape.id, index: interactedShapeIndex, shape: interactedShape };
                    await updateShapeVisuals();
                } else {
                    window.activeShapeForPotentialDeletion.shape = interactedShape;
                }
                updateSelectedShapeInfoPanel(window.activeShapeForPotentialDeletion);
            }
        }

        // New shape detection logic (fallback or for shapes not caught by plotly_shapedrawn)
        const currentLayoutShapesForNewCheck = window.gd.layout.shapes || [];
        let newShapesProcessedInRelayout = false;
        for (let i = 0; i < currentLayoutShapesForNewCheck.length; i++) {
            const shapeInLayout = currentLayoutShapesForNewCheck[i];

            // Check if this is already a processed drawing (has drawing_ prefix in name)
            const isAlreadyProcessedDrawing = shapeInLayout.name && shapeInLayout.name.startsWith('drawing_');

            // Additional check: warn if we have an id but it's still being processed
            if (shapeInLayout.id && !shapeInLayout._savingInProgress) {
                /*
                console.warn('[plotly_relayout] WARNING: Shape already has id but is being considered for processing:', {
                    name: shapeInLayout.name,
                    id: shapeInLayout.id,
                    type: shapeInLayout.type,
                    isSystemShape: shapeInLayout.isSystemShape,
                    isAlreadyProcessedDrawing: isAlreadyProcessedDrawing
                });
                */
                continue; // Skip this shape entirely
            }

            // CRITICAL FIX: System shapes (including buy signals) should NEVER be processed for saving
            // Additional check: Buy signals have special properties that should explicitly prevent saving
            const isBuySignal = shapeInLayout.name && shapeInLayout.name.startsWith('buy_signal_');
            const isSystemBuySignal = shapeInLayout.systemType === 'buy_signal';

            // Skip system shapes that should be managed elsewhere, but allow buy signals through
            if (shapeInLayout.isSystemShape && !(isSystemBuySignal || isBuySignal)) {
                continue; // Skip this shape entirely
            }

            // Skip shapes that use 'paper' coordinates (grid lines, subplot separators, etc.)
            if (shapeInLayout.yref === 'paper' || shapeInLayout.xref === 'paper') {
                continue; // Skip paper coordinate shapes
            }

            if ((shapeInLayout.type === 'line' || shapeInLayout.type === 'rect') &&
                !shapeInLayout.id &&
                !shapeInLayout.isSystemShape && // Already correctly ignores system shapes
                !shapeInLayout._savingInProgress && // Check this flag
                !isAlreadyProcessedDrawing // Additional check for already processed drawings
               ) {

                // DETAILED LOGGING FOR POTENTIALLY MISIDENTIFIED NEW SHAPE
                console.warn('[plotly_relayout] Fallback: Potential new shape to save. Inspecting properties:', {
                    name: shapeInLayout.name,
                    type: shapeInLayout.type,
                    x0: shapeInLayout.x0, y0: shapeInLayout.y0,
                    x1: shapeInLayout.x1, y1: shapeInLayout.y1,
                    xref: shapeInLayout.xref, yref: shapeInLayout.yref,
                    id: shapeInLayout.id, // Should be undefined/null
                    isSystemShape: shapeInLayout.isSystemShape, // CRITICAL: Should be true for system shapes to be ignored
                    _savingInProgress: shapeInLayout._savingInProgress, // Should be false or undefined
                    isAlreadyProcessedDrawing: isAlreadyProcessedDrawing
                });
                shapeInLayout._savingInProgress = true; // Set flag on the layout shape
                
                // Ensure shape has necessary properties before saving
                shapeInLayout.editable = true;
                if (shapeInLayout.line) {
                    shapeInLayout.line.layer = 'above';
                } else if (shapeInLayout.type === 'line') {
                    shapeInLayout.line = { color: DEFAULT_DRAWING_COLOR, width: 2, layer: 'above' };
                }

                // Add larger markers for mobile touch targets (only for user-drawn lines)
                if (shapeInLayout.type === 'line' && !shapeInLayout.isSystemShape) {
                    shapeInLayout.marker = {
                        size: isMobileDevice() ? 34 : 24, // Even larger markers for maximum visibility
                        color: DEFAULT_DRAWING_COLOR,
                        symbol: 'circle', // Circle markers for line endpoints
                        line: { width: 3, color: 'white' }, // Thicker white border
                        opacity: 0.95 // Make markers more opaque for better visibility
                    };
                }

                const backendId = await handleNewShapeSave(shapeInLayout);

                if (backendId) {
                    // Find the current index of the shape in the live layout (it might have shifted due to removals)
                    const currentIndex = window.gd.layout.shapes.indexOf(shapeInLayout);
                    if (currentIndex > -1 && !window.gd.layout.shapes[currentIndex].id) {
                        window.gd.layout.shapes[currentIndex].id = backendId;
                        // editable and layer already set
                        window.activeShapeForPotentialDeletion = { id: backendId, index: currentIndex, shape: window.gd.layout.shapes[currentIndex] };
                        updateSelectedShapeInfoPanel(window.activeShapeForPotentialDeletion);
                        newShapesProcessedInRelayout = true;
                    } else {
                          console.warn(`[plotly_relayout] Fallback: Shape was already processed, changed, or removed before id could be assigned by relayout.`);
                    }
                } else {
                    console.warn(`[plotly_relayout] Fallback: Failed to save new shape. Removing from layout.`);
                    // Remove the shape if save failed to prevent it from being re-processed
                    const indexToRemove = window.gd.layout.shapes.indexOf(shapeInLayout);
                    if (indexToRemove > -1) window.gd.layout.shapes.splice(indexToRemove, 1);
                    // No need to Plotly.relayout here, as this loop is part of a relayout,
                    // but a full redraw might be needed if things get out of sync.
                    // For now, just remove from the array being iterated if possible, or mark it.
                    // A full loadDrawingsAndRedraw might be too disruptive here.
                }
                delete shapeInLayout._savingInProgress; // Clear the flag
            }
        }
        if (newShapesProcessedInRelayout) {
            await updateShapeVisuals(); // Update visuals if relayout processed new shapes
        }
        // Axis range change handling
        let xRangesChangedByEvent = false;
        let yRangesChangedByEvent = false;
        let userModifiedRanges = false;


        if (eventData['xaxis.range[0]'] || eventData['xaxis.range[1]']) {
            const currentLayoutShapes = gd.layout.shapes || [];
            const xRange = gd.layout.xaxis.range;
            if (xRange && xRange.length === 2) {
                // Keep in milliseconds - Plotly already provides proper Date objects/timestamps
                const minTimestamp = (xRange[0] instanceof Date) ? xRange[0].getTime() : new Date(xRange[0]).getTime();
                const maxTimestamp = (xRange[1] instanceof Date) ? xRange[1].getTime() : new Date(xRange[1]).getTime();


                // 🚨 OLD ALARM CHECK: Validate zoom/pan timestamps before saving
                const minDate = new Date(minTimestamp);
                const maxDate = new Date(maxTimestamp);
                if (minDate.getFullYear() < 2000 || maxDate.getFullYear() < 2000) {
                    const minDateStr = isNaN(minDate.getTime()) ? 'Invalid Date' : minDate.toISOString();
                    const maxDateStr = isNaN(maxDate.getTime()) ? 'Invalid Date' : maxDate.toISOString();
                    console.warn('🚨 OLD ALARM DETECTED: Chart zoom/pan resulted in very old dates!', {
                        source: 'plotly_relayout_zoom',
                        xRange: xRange,
                        minTimestamp: minTimestamp,
                        maxTimestamp: maxTimestamp,
                        minDate: minDateStr,
                        maxDate: maxDateStr,
                        minYear: minDate.getFullYear(),
                        maxYear: maxDate.getFullYear(),
                        action: 'Chart zoom/pan calculated timestamps before year 2000 - this should not happen!'
                    });
                }

        // Store timestamps in milliseconds for consistency with the rest of the system
        window.currentXAxisRange = [minTimestamp, maxTimestamp];

        // Update display elements with validation
        try {
            window.xAxisMinDisplay.textContent = new Date(minTimestamp).toISOString();
        } catch (e) {
            console.error("Error formatting xAxisMinDisplay:", e);
            window.xAxisMinDisplay.textContent = `Invalid Date: ${minTimestamp}`;
        }

        try {
            window.xAxisMaxDisplay.textContent = new Date(maxTimestamp).toISOString();
        } catch (e) {
            console.error("Error formatting xAxisMaxDisplay:", e);
            window.xAxisMaxDisplay.textContent = `Invalid Date: ${maxTimestamp}`;
        }

        if (!window.isApplyingAutoscale) {
            xRangesChangedByEvent = true;
            userModifiedRanges = true;
        }
            } else {
            }
        } else if (eventData['xaxis.autorange'] === true) {
            if (window.currentXAxisRange !== null) {
                window.currentXAxisRange = null;
                window.xAxisMinDisplay.textContent = 'Auto';
                window.xAxisMaxDisplay.textContent = 'Auto';
                xRangesChangedByEvent = true;
            } else {
                // On first load or when no previous range, update to actual range
                const xRange = gd.layout.xaxis.range;
                if (xRange && xRange.length === 2) {
                    const minTimestamp = (xRange[0] instanceof Date) ? xRange[0].getTime() : new Date(xRange[0]).getTime();
                    const maxTimestamp = (xRange[1] instanceof Date) ? xRange[1].getTime() : new Date(xRange[1]).getTime();
                    window.currentXAxisRange = [minTimestamp, maxTimestamp];

                    // Update display elements with validation
                    try {
                        window.xAxisMinDisplay.textContent = new Date(minTimestamp).toISOString();
                    } catch (e) {
                        console.error("Error formatting xAxisMinDisplay on autorange:", e);
                        window.xAxisMinDisplay.textContent = `Invalid Date: ${minTimestamp}`;
                    }

                    try {
                        window.xAxisMaxDisplay.textContent = new Date(maxTimestamp).toISOString();
                    } catch (e) {
                        console.error("Error formatting xAxisMaxDisplay on autorange:", e);
                        window.xAxisMaxDisplay.textContent = `Invalid Date: ${maxTimestamp}`;
                    }

                    // Don't save settings for initial autorange
                    xRangesChangedByEvent = false;
                }
            }
        } else {
        }

        if (eventData['yaxis.range[0]'] || eventData['yaxis.range[1]']) {
            const yRange = gd.layout.yaxis.range;
            if (yRange && yRange.length === 2) {
                window.currentYAxisRange = [yRange[0], yRange[1]];
                window.yAxisMinDisplay.textContent = window.currentYAxisRange[0].toFixed(2);
                window.yAxisMaxDisplay.textContent = window.currentYAxisRange[1].toFixed(2);
                if (!window.isApplyingAutoscale) {
                    yRangesChangedByEvent = true;
                    userModifiedRanges = true;
                }
            }
        } else if (eventData['yaxis.autorange'] === true && window.currentYAxisRange !== null) {
            window.currentYAxisRange = null;
            window.yAxisMinDisplay.textContent = 'Auto';
            window.yAxisMaxDisplay.textContent = 'Auto';
            yRangesChangedByEvent = true;
        }

// Function to debounced save settings for chart resize events
function debouncedSaveSettingsForResize() {
    clearTimeout(settingsSaveTimer);
    settingsSaveTimer = setTimeout(() => {
        saveSettings();
    }, SETTINGS_SAVE_DEBOUNCE_DELAY);
}

        // Pan timer logic like trade filter timer
        if (xRangesChangedByEvent && userModifiedRanges) {
            debouncedSaveSettingsForResize(); // Debounced settings save for chart resize events

            // 🚨 CLEAR CHART DATA BEFORE REQUESTING NEW DATA 🚨
            // This prevents data overlapping when user pans/zooms to new time ranges
            if (window.gd) {
                removeRealtimePriceLine(window.gd);
                // Clear all chart data and reset to empty state
                Plotly.react(window.gd, [], window.gd.layout || {});
            }

            // Reset pan timer on each pan event (like trade filter timer)
            clearTimeout(panTimer);
            panTimer = setTimeout(() => {
                // Only trigger config when timer reaches 0 (2 seconds)
                triggerPanConfigUpdate();
            }, PAN_TIMER_DELAY);

            // Function to trigger the actual config update when pan timer expires
            function triggerPanConfigUpdate() {
                const symbol = window.symbolSelect ? window.symbolSelect.value : null;
                const resolution = window.resolutionSelect ? window.resolutionSelect.value : '1h';
                if (symbol && resolution && window.currentXAxisRange) {
                    const active_indicators = Array.from(document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]:checked')).map(cb => cb.value);
                    // currentXAxisRange is in milliseconds, convert to seconds for checkIfHistoricalDataNeeded
                    const fromTsSeconds = Math.floor(window.currentXAxisRange[0] / 1000);
                    const toTsSeconds = Math.floor(window.currentXAxisRange[1] / 1000);

                    // Request historical data for the current visible range
                    sendCombinedConfig(window.currentXAxisRange[0], window.currentXAxisRange[1]);
                }
            }
        } else if ((xRangesChangedByEvent || yRangesChangedByEvent) && !userModifiedRanges) {
            // Don't save settings for programmatic changes
        }

        // Reset dragging flag and restore previous drag mode after relayout completes
        if (window.isDraggingShape) {
            window.isDraggingShape = false;
            // Restore previous drag mode
            if (gd.layout.dragmode !== previousDragMode) {
                Plotly.relayout(gd, { dragmode: previousDragMode });
            } else {
            }

            // Re-trigger hover detection after drag completion to restore line coloring
            delay(100).then(() => {
                if (window.colorTheLine) {
                    window.colorTheLine();
                }
            }); // Small delay to ensure relayout is complete

            // Force update shape visuals immediately after drag to ensure line is visible
            // Use a shorter delay and ensure it happens after drag mode restoration
            delay(50).then(() => {
                if (window.updateShapeVisuals) {
                    window.updateShapeVisuals();
                }
            });
        }
    });

    gd.on('plotly_shapeupdate', async function(eventData) {
        // This event name 'plotly_shapeupdate' is not standard Plotly.
        // Shape modifications (dragging handles) trigger 'plotly_relayout'.
        // The logic for handling shape modifications is already within the 'plotly_relayout' handler
        // using 'interactedShapeIndex'. If this was a custom event or a specific
        // interpretation in the old index.html, its exact trigger is unclear.
        // For now, assuming this was intended for modifications caught by 'plotly_relayout'.
        console.warn('[plotly_shapeupdate] This event handler was called. Standard shape modifications are handled by plotly_relayout. Event data:', eventData);
        // If eventData contains a shape index and the update:
        const shapeIndex = eventData.shapeindex; // Hypothetical eventData structure
        if (shapeIndex !== undefined && gd.layout.shapes && gd.layout.shapes[shapeIndex]) {
            const updatedShape = gd.layout.shapes[shapeIndex];
            if (updatedShape.id) {
                const saveSuccess = await sendShapeUpdateToServer(updatedShape, window.symbolSelect.value);
                if (!saveSuccess) {
                    loadDrawingsAndRedraw(window.symbolSelect.value);
                    return;
                }
                // Update active shape state if this is the one being edited
                if (!window.activeShapeForPotentialDeletion || window.activeShapeForPotentialDeletion.id !== updatedShape.id) {
                    window.activeShapeForPotentialDeletion = { id: updatedShape.id, index: shapeIndex, shape: updatedShape };
                } else { // It is the active shape, ensure its 'shape' property is updated
                    window.activeShapeForPotentialDeletion.shape = updatedShape;
                }
                updateSelectedShapeInfoPanel(window.activeShapeForPotentialDeletion);
                await updateShapeVisuals();
            }
        }
    });

    gd.on('plotly_remove_shape', async function(eventData) {
        // This event name 'plotly_remove_shape' is not standard Plotly.
        // Deletions (e.g., via modebar if configured, or programmatically) would trigger 'plotly_relayout'.
        // The primary deletion mechanism in this app is the 'Delete' key, handled by a separate listener.
        // This handler is speculative, assuming 'eventData' contains the removed shape.
        console.warn('[plotly_remove_shape] This event handler was called. Deletions are primarily handled by keydown. Event data:', eventData);
        const removedShape = eventData.shape; // Hypothetical: eventData directly provides the removed shape object
        if (removedShape && removedShape.id) {
            const symbol = window.symbolSelect.value;
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
                                if (message.type === 'shape_success' && message.data && message.data.id === removedShape.id) {
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
                                drawing_id: removedShape.id,
                                symbol: symbol
                            },
                            request_id: requestId
                        });
                    });
                } else {
                    throw new Error('WebSocket not connected');
                }

                const shapeId = removedShape.id;
                // Remove the shape from the chart
                if (window.gd && window.gd.layout && window.gd.layout.shapes) {
                    const shapes = window.gd.layout.shapes.filter(shape => shape.id !== shapeId);
                    Plotly.relayout(window.gd, { shapes: shapes });
                }

                // Remove associated volume profile traces for rectangles
                if (window.gd && window.gd.data) {
                    const filteredData = window.gd.data.filter(trace =>
                        !trace.name || !trace.name.startsWith(`VP-${shapeId}`)
                    );
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

                // Clear the active shape state
                if (window.activeShapeForPotentialDeletion && window.activeShapeForPotentialDeletion.id === removedShape.id) {
                    window.activeShapeForPotentialDeletion = null;
                    updateSelectedShapeInfoPanel(null);
                }
                await updateShapeVisuals(); // Refresh visuals
            } catch (error) {
                console.error(`Error deleting drawing ${removedShape.id} via plotly_remove_shape:`, error);
                alert(`Failed to delete drawing: ${error.message}`);
            }
        }
    });
}

function observeShapeDAttributeChanges(plotDivId) {
    const plotDiv = document.getElementById(plotDivId);
    if (!plotDiv) {
        console.error(`Plotly div with id "${plotDivId}" not found.`);
        return;
    }

    // Target the 'g' element with class 'shapelayer'
    const shapeLayer = plotDiv.querySelector('.shapelayer');
    if (!shapeLayer) {
        console.error('Shape layer not found. Is a shape being drawn?');
        return;
    }

    // Select all 'path' elements that are part of the 'drag-helper' groups
    const dragHelperPaths = shapeLayer.querySelectorAll('g[drag-helper="true"] > path');

    if (dragHelperPaths.length === 0) {
        console.warn('No draggable shape paths found. Make sure shape editing is enabled.');
        return;
    }

    dragHelperPaths.forEach(pathElement => {
        // Create an observer instance
        const observer = new MutationObserver(mutationsList => {
            for (const mutation of mutationsList) {
                // Check if the 'd' attribute was the one that changed
                if (mutation.type === 'attributes' && mutation.attributeName === 'd') {
                    const newDValue = mutation.target.getAttribute('d');
                    // You can perform an action here, like updating a different element or triggering a function
                }
            }
        });

        // Start observing the target path element for attribute changes
        observer.observe(pathElement, { attributes: true, attributeFilter: ['d'] });
    });
}

// Function to observe changes to the 'd' attribute of paths in the shape layer
function observeShapeDAttributeChanges(plotDivId) {
    const plotDiv = document.getElementById(plotDivId);
    if (!plotDiv) {
        console.error(`Plotly div with id "${plotDivId}" not found.`);
        return;
    }

    // Target the 'g' element with class 'shapelayer'
    const shapeLayer = plotDiv.querySelector('.shapelayer');
    if (!shapeLayer) {
        console.error('Shape layer not found. Is a shape being drawn?');
        return;
    }

    // Select all 'path' elements that are part of the 'drag-helper' groups
    const dragHelperPaths = shapeLayer.querySelectorAll('g[drag-helper="true"] > path');

    if (dragHelperPaths.length === 0) {
        console.warn('No draggable shape paths found. Make sure shape editing is enabled.');
        return;
    }

    dragHelperPaths.forEach(pathElement => {
        // Create an observer instance
        const observer = new MutationObserver(mutationsList => {
            for (const mutation of mutationsList) {
                // Check if the 'd' attribute was the one that changed
                if (mutation.type === 'attributes' && mutation.attributeName === 'd') {
                    const newDValue = mutation.target.getAttribute('d');
                    // You can perform an action here, like updating a different element or triggering a function
                }
            }
        });

        // Start observing the target path element for attribute changes
        observer.observe(pathElement, { attributes: true, attributeFilter: ['d'] });
    });
}

// Function to check if we need more historical data for the current visible range
function checkIfHistoricalDataNeeded(fromTs, toTs, resolution) {

    // Get current chart data to see what time range we already have
    const gd = window.gd;
    if (!gd || !gd.data || gd.data.length === 0) {
        return true; // No data at all, definitely need historical data
    }

    // Find the main price trace (candlestick)
    const priceTrace = gd.data.find(trace => trace.type === 'candlestick');
    if (!priceTrace || !priceTrace.x || priceTrace.x.length === 0) {
        return true;
    }


    // Get the time range of current data
    const currentDataMin = Math.min(...priceTrace.x.map(x => (x instanceof Date) ? x.getTime() : new Date(x).getTime())) / 1000;
    const currentDataMax = Math.max(...priceTrace.x.map(x => (x instanceof Date) ? x.getTime() : new Date(x).getTime())) / 1000;


    // Check if the requested range extends beyond current data
    const rangeExtension = 0.1; // 10% buffer
    const extendedFromTs = fromTs - (toTs - fromTs) * rangeExtension;
    const extendedToTs = toTs + (toTs - fromTs) * rangeExtension;

    const needsEarlierData = extendedFromTs < currentDataMin;
    const needsLaterData = extendedToTs > currentDataMax;


    if (needsEarlierData || needsLaterData) {
        return true;
    }

    return false;
}

// Initialize observer for main chart
observeShapeDAttributeChanges('chart'); // Use your actual chart div ID

// Global function to test panning detection
window.testPanningDetection = function() {


    // Also trigger a manual test event
    delay(1000).then(() => {
        try {
            if (window.gd && window.gd.emit) {
                const testEventData = {
                    'xaxis.range[0]': new Date(Date.now() - 24 * 60 * 60 * 1000),
                    'xaxis.range[1]': new Date()
                };
                window.gd.emit('plotly_relayout', testEventData);
            }
        } catch (e) {
            alert('🧪 Manual test event failed: ' + e.message);
        }
    });
};

// Function to update the panning status indicator in the UI
function updatePanningStatus(isPanning = false) {
    const indicator = document.getElementById('panning-indicator');
    const lastPanTime = document.getElementById('last-pan-time');

    if (indicator && lastPanTime) {
        if (isPanning) {
            indicator.textContent = '🚨 Panning Detected!';
            indicator.style.color = '#28a745'; // Green
            lastPanTime.textContent = new Date().toLocaleTimeString();
        } else {
            indicator.textContent = 'Waiting for pan/zoom...';
            indicator.style.color = '#666'; // Gray
        }
    }
}

// Add additional event listeners for debugging
document.addEventListener('DOMContentLoaded', function() {

    // Initialize panning status
    updatePanningStatus(false);
});

// Global function to compare client and server timestamp ranges
window.compareClientServerRanges = function() {
    try {
        if (typeof console !== 'undefined' && console.log) {

            if (!window.lastClientRange) {
                return;
            }


            // Try to get server range from WebSocket state if available
            if (window.ws && window.ws.readyState === WebSocket.OPEN) {
            } else {
            }

            // Manual comparison if we have both ranges
            if (window.lastServerRange) {

                const clientFrom = window.lastClientRange.xMinTimestamp;
                const clientTo = window.lastClientRange.xMaxTimestamp;
                const serverFrom = window.lastServerRange.fromTs;
                const serverTo = window.lastServerRange.toTs;

                const fromDiff = Math.abs(clientFrom - serverFrom);
                const toDiff = Math.abs(clientTo - serverTo);


                if (fromDiff > 60 || toDiff > 60) {
                } else {
                }
            } else {
            }
        } else {
            alert('🔍 COMPARING CLIENT AND SERVER TIMESTAMP RANGES 🔍\nConsole not available in this environment.');
        }
    } catch (e) {
        console.error('Error in compareClientServerRanges:', e);
        alert('Error comparing ranges: ' + e.message);
    }
};
