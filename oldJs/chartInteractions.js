window.newHoveredShapeId = null;
// Removed lastClickTime and lastClickedShapeId - long press functionality has been disabled

function updateOrAddCrosshairVLine(gd, xDataValue, doRelayout = true) {
    if (!gd || !gd.layout || !gd.layout.xaxis || !xDataValue) {
        return;
    }

    let shapes = gd.layout.shapes || [];
    shapes = shapes.filter(shape => shape.name !== CROSSHAIR_VLINE_NAME); // From config.js

    const lineDefinition = {
        type: 'line',
        name: CROSSHAIR_VLINE_NAME, // From config.js
        isSystemShape: true, // Mark as a system shape
        xref: 'x',
        yref: 'paper',
        x0: xDataValue,
        y0: 0,
        x1: xDataValue,
        y1: 1,
        line: {
            color: 'rgba(100, 100, 100, 0.6)',
            width: 1,
            dash: 'dash'
        },
        layer: 'above',
        editable: false // Explicitly make crosshair not editable
    };
    shapes.push(lineDefinition);

    if (doRelayout) {
        Plotly.relayout(gd, { shapes: shapes });
    } else {
        gd.layout.shapes = shapes;
    }
}


function colorTheLine(eventParam)
{
        // Reset newHoveredShapeId at the start of each call
        window.newHoveredShapeId = null;

        // Add detailed event logging

        // Skip if a shape is currently being dragged
        if (window.isDraggingShape) {
            return;
        }



        // Use the passed event or fall back to global event
        const currentEvent = eventParam || event;

        if (!window.gd || !window.gd.layout) {
            if (window.hoveredShapeBackendId !== null) { // Assumes hoveredShapeBackendId is global from state.js
                window.hoveredShapeBackendId = null;
                if (typeof updateShapeVisuals === 'function') {
                    updateShapeVisuals();
                }
            }
            //console.groupEnd();
            return;
        }

        if (currentEvent && (currentEvent.target.closest('.modebar') ||
            currentEvent.target.closest('select') ||
            currentEvent.target.closest('input') ||
            currentEvent.target.closest('.rangeslider') ||
            currentEvent.target.closest('.legend'))) {
            //console.groupEnd();
            return;
        }

        const rect = window.chartDiv.getBoundingClientRect();

        // Try to get mouse coordinates from event, or use a fallback method
        let mouseX_div, mouseY_div;
        if (currentEvent && currentEvent.clientX !== undefined && currentEvent.clientY !== undefined &&
            currentEvent.clientX !== 0 && currentEvent.clientY !== 0) {
            mouseX_div = currentEvent.clientX - rect.left;
            mouseY_div = currentEvent.clientY - rect.top;
        } else if (currentEvent && currentEvent.touches && currentEvent.touches.length > 0) {
            // Handle touch events
            const touch = currentEvent.touches[0] || currentEvent.changedTouches[0];
            mouseX_div = touch.clientX - rect.left;
            mouseY_div = touch.clientY - rect.top;
        } else {
            // Fallback: try to get mouse position from window.event or other methods
            const globalEvent = window.event;
            if (globalEvent && globalEvent.clientX !== undefined && globalEvent.clientY !== undefined &&
                globalEvent.clientX !== 0 && globalEvent.clientY !== 0) {
                mouseX_div = globalEvent.clientX - rect.left;
                mouseY_div = globalEvent.clientY - rect.top;
            } else {
                // Try to get mouse position from document.elementFromPoint or other methods
                try {
                    const centerX = rect.left + rect.width/2;
                    const centerY = rect.top + rect.height/2;
                    const elementsAtCenter = document.elementsFromPoint(centerX, centerY);
                    if (elementsAtCenter && elementsAtCenter.length > 0) {
                        mouseX_div = rect.width / 2;
                        mouseY_div = rect.height / 2;
                    } else {
                        throw new Error("No elements found at center");
                    }
                } catch (e) {
                    // Last resort: assume center of chart for testing
                    mouseX_div = rect.width / 2;
                    mouseY_div = rect.height / 2;
                }
            }
        }

        if (!window.gd._fullLayout || typeof window.gd._fullLayout.height === 'undefined' || !window.gd._fullLayout.yaxis || typeof window.gd._fullLayout.yaxis._length === 'undefined') {
            return;
        }

        // Convert DOM coordinates to Plotly paper coordinates
        // Paper coordinates are relative to the full chart area (including margins)
        const plotMargin = window.gd._fullLayout.margin;
    const mouseX_paper = mouseX_div ;
    const mouseY_paper = mouseY_div ;

        // Check if mouse is within the chart's plotting area (with some tolerance for edge cases)
        const tolerance = 10; // Allow 10px tolerance for edge cases
        // Since mouseX_paper and mouseY_paper are in div coordinates (not subtracting margins),
        // we need to account for margins in the bounds checking
        const plotLeft = plotMargin.l;
        const plotTop = plotMargin.t;
        const plotRight = plotMargin.l + (window.gd._fullLayout.width - plotMargin.l - plotMargin.r);
        const plotBottom = plotMargin.t + (window.gd._fullLayout.height - plotMargin.t - plotMargin.b);

        const isOutsideBounds = mouseX_paper < plotLeft - tolerance ||
                               mouseX_paper > plotRight + tolerance ||
                               mouseY_paper < plotTop - tolerance ||
                               mouseY_paper > plotBottom + tolerance;


        if (isOutsideBounds) {
            if (window.hoveredShapeBackendId !== null) {
                window.hoveredShapeBackendId = null;
                if (typeof debouncedUpdateShapeVisuals === 'function') {
                    debouncedUpdateShapeVisuals();
                }
            }
            return;
        }

        const mainYAxis = window.gd._fullLayout.yaxis;
        const plotAreaHeight = mainYAxis._length;
        const mouseX_plotArea = mouseX_paper;
        const mouseY_plotArea = plotAreaHeight - mouseY_paper;
        let minDistanceSq = Infinity;
        const HOVER_THRESHOLD_PIXELS_SQ = 15 * 15;

        const hoveredSubplotRefs = getSubplotRefsAtPaperCoords(mouseX_paper, mouseY_paper, window.gd._fullLayout);
        if (hoveredSubplotRefs) {
        } else {
            if (window.hoveredShapeBackendId !== null) {
                window.hoveredShapeBackendId = null;
                if (typeof updateShapeVisuals === 'function') {
                    updateShapeVisuals();
                }
            }
        }

        const currentShapes = window.gd.layout.shapes || [];
        // console.groupCollapsed("Checking shapes for hover detection");
        for (let i = 0; i < currentShapes.length; i++) {
            const shape = currentShapes[i];
            if ((shape.type === 'line' || shape.type === 'rect' || shape.type === 'rectangle' || shape.type === 'box') && shape.id && !shape.isSystemShape) { // Ignore system shapes
                // Process all valid shapes for hover detection, regardless of selection state
                // Selected shapes should still show hover effects when hovered over
                const xrefKeyForFilter = getAxisLayoutKey(shape.xref, 'xaxis'); // Assumes getAxisLayoutKey is global
                const yrefKeyForFilter = getAxisLayoutKey(shape.yref, 'yaxis');
                const shapeXaxisForFilter = window.gd._fullLayout[xrefKeyForFilter];
                const shapeYaxisForFilter = window.gd._fullLayout[yrefKeyForFilter];

                if (!shapeXaxisForFilter || !shapeYaxisForFilter) continue;

                // Allow shapes in any valid subplot - process all shapes regardless of subplot detection
                // This ensures hover detection works on any shape anywhere on the chart

                const xrefKey = getAxisLayoutKey(shape.xref, 'xaxis');
                const yrefKey = getAxisLayoutKey(shape.yref, 'yaxis');
                const shapeXaxis = window.gd._fullLayout[xrefKey];
                const shapeYaxis = window.gd._fullLayout[yrefKey];

                if (!shapeXaxis || !shapeYaxis || typeof shapeXaxis.d2p !== 'function' || typeof shapeYaxis.d2p !== 'function') {
                    //console.warn(`[NativeMousemove DEBUG] Could not find valid axes for shape ${i} (ID: ${shape.id}) with xref=${shape.xref}, yref=${shape.yref}. Skipping hover test.`);
                    continue;
                }

                /*
                console.group(`[NativeMousemove DEBUG] Checking Shape ${i} (ID: ${shape.id})`);
                */

                let shapeX0Val = (shapeXaxis.type === 'date') ? ((shape.x0 instanceof Date) ? shape.x0.getTime() : new Date(shape.x0).getTime()) : Number(shape.x0);
                let shapeX1Val = (shapeXaxis.type === 'date') ? ((shape.x1 instanceof Date) ? shape.x1.getTime() : new Date(shape.x1).getTime()) : Number(shape.x1);
                const p0y_subplot_relative_hover = shapeYaxis.d2p(shape.y0);
                const p1y_subplot_relative_hover = shapeYaxis.d2p(shape.y1);
                const p0x_subplot_relative_hover = shapeXaxis.d2p(shapeX0Val);
                const p1x_subplot_relative_hover = shapeXaxis.d2p(shapeX1Val);

                const p0 = { x: shapeXaxis._offset + p0x_subplot_relative_hover, y: shapeYaxis._offset + p0y_subplot_relative_hover };
                const p1 = { x: shapeXaxis._offset + p1x_subplot_relative_hover, y: shapeYaxis._offset + p1y_subplot_relative_hover };

            if (shape.type === 'line') {
                // Handle line shapes with segment distance calculation
                if (isNaN(p0.x) || isNaN(p0.y) || isNaN(p1.x) || isNaN(p1.y) || !isFinite(p0.x) || !isFinite(p0.y) || !isFinite(p1.x) || !isFinite(p1.y)) {
                    /*console.warn(`[NativeMousemove DEBUG] Shape ${i} (ID: ${shape.id}) had NaN/Infinite pixel coordinates. Skipping.`);

                    */
                    continue;
                }
                const distSq = distToSegmentSquared({ x: mouseX_paper, y: mouseY_paper }, p0, p1); // From utils.js
                if (distSq < HOVER_THRESHOLD_PIXELS_SQ && distSq < minDistanceSq) {
                    minDistanceSq = distSq;
                    window.newHoveredShapeId = shape.id;
                }
            } else if (shape.type === 'rect' || shape.type === 'rectangle' || shape.type === 'box') {
                // Handle rectangle shapes with point-in-rectangle calculation
                const rectLeft = Math.min(shapeXaxis.d2p(shapeX0Val), shapeXaxis.d2p(shapeX1Val)) + shapeXaxis._offset;
                const rectRight = Math.max(shapeXaxis.d2p(shapeX0Val), shapeXaxis.d2p(shapeX1Val)) + shapeXaxis._offset;
                const rectTop = Math.min(shapeYaxis.d2p(Math.min(shape.y0, shape.y1)), shapeYaxis.d2p(Math.max(shape.y0, shape.y1))) + shapeYaxis._offset;
                const rectBottom = Math.max(shapeYaxis.d2p(Math.min(shape.y0, shape.y1)), shapeYaxis.d2p(Math.max(shape.y0, shape.y1))) + shapeYaxis._offset;

                // Check if mouse point is inside rectangle bounds (with some tolerance for hover)
                const hoverTolerance = 5; // pixels of tolerance around rectangle for hover
                if (mouseX_paper >= rectLeft - hoverTolerance && mouseX_paper <= rectRight + hoverTolerance &&
                    mouseY_paper >= rectTop - hoverTolerance && mouseY_paper <= rectBottom + hoverTolerance) {
                    // For rectangles, use distance from center as the hover distance metric
                    const rectCenterX = (rectLeft + rectRight) / 2;
                    const rectCenterY = (rectTop + rectBottom) / 2;
                    const centerDistSq = Math.pow(mouseX_paper - rectCenterX, 2) + Math.pow(mouseY_paper - rectCenterY, 2);

                    if (centerDistSq < HOVER_THRESHOLD_PIXELS_SQ && centerDistSq < minDistanceSq) {
                        minDistanceSq = centerDistSq;
                        window.newHoveredShapeId = shape.id;
                    }
                }
            }
            }
        }
        // End group for this shape

        if (window.hoveredShapeBackendId !== window.newHoveredShapeId) {
                window.hoveredShapeBackendId = window.newHoveredShapeId;

                // Call updateShapeVisuals directly for immediate color changes
                if (typeof updateShapeVisuals === 'function') {
                    updateShapeVisuals();
                } else {
                    console.warn('[DEBUG] colorTheLine - updateShapeVisuals not available!');
                }
        }
        // Only clear shape ID if we're sure no shape should be hovered (mouse outside chart area)
        if (isOutsideBounds && window.hoveredShapeBackendId !== null) {
            window.hoveredShapeBackendId = null;
            if (typeof updateShapeVisuals === 'function') {
                updateShapeVisuals();
            }
        }

        // Crosshair logic - disabled on mobile devices and when in drawing mode for better performance
        if (!isMobileDevice() && window.gd.layout.dragmode !== 'drawline' && window.gd.layout.dragmode !== 'drawrect') {
            const mainXAxis = window.gd._fullLayout.xaxis;
            if (mainXAxis && typeof mainXAxis.p2d === 'function') {
                const xDataValueAtMouse = mainXAxis.p2d(mouseX_plotArea);
                if (xDataValueAtMouse !== undefined && xDataValueAtMouse !== null && !isNaN(new Date(xDataValueAtMouse).getTime())) {
                    const dateAtCursor = new Date(xDataValueAtMouse);
                    if (window.cursorTimeDisplay) { // Assumes cursorTimeDisplay is global
                        window.cursorTimeDisplay.textContent = dateAtCursor.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' });
                    }
                    // DISABLED: Crosshair interferes with panning
                    // window.debouncedUpdateCrosshair(window.gd, dateAtCursor); // Assumes debouncedUpdateCrosshair is global

                    const mainTrace = window.gd.data[0];
                    let candleIndex = -1;
                    if (mainTrace && mainTrace.x && mainTrace.x.length > 0 && mainTrace.close && mainTrace.close.length > 0) {
                        const cursorTime = dateAtCursor.getTime();
                        for (let i = mainTrace.x.length - 1; i >= 0; i--) {
                            const candleTimeValue = mainTrace.x[i];
                            const candleTime = (candleTimeValue instanceof Date) ? candleTimeValue.getTime() : new Date(candleTimeValue).getTime();
                            if (!isNaN(candleTime) && candleTime <= cursorTime) {
                                candleIndex = i;
                                break;
                            }
                        }
                    }

                    if (candleIndex !== -1 && mainTrace.close[candleIndex] !== undefined) {
                        const closePrice = mainTrace.close[candleIndex];
                        window.cursorPriceDisplay.textContent = parseFloat(closePrice).toFixed(2); // Assumes cursorPriceDisplay is global
                    } else {
                        window.cursorPriceDisplay.textContent = 'N/A';
                    }
                } else {
                    // DISABLED: Crosshair interferes with panning
                    // removeCrosshairVLine(window.gd, true);
                    if (window.cursorTimeDisplay) window.cursorTimeDisplay.textContent = 'N/A';
                    if (window.cursorPriceDisplay) window.cursorPriceDisplay.textContent = 'N/A';
                }
            } else {
                // DISABLED: Crosshair interferes with panning
                // removeCrosshairVLine(window.gd, true);
                if (window.cursorTimeDisplay) window.cursorTimeDisplay.textContent = 'N/A';
                if (window.cursorPriceDisplay) window.cursorPriceDisplay.textContent = 'N/A';
            }
        } else {
            // On mobile or in drawing mode, clear crosshair and cursor displays
            // DISABLED: Crosshair interferes with panning
            // removeCrosshairVLine(window.gd, true);
            if (window.cursorTimeDisplay) window.cursorTimeDisplay.textContent = 'N/A';
            if (window.cursorPriceDisplay) window.cursorPriceDisplay.textContent = 'N/A';
        }

}

function getSubplotRefsAtPaperCoords(paperX, paperY, fullLayout) {
    const paperHeight = fullLayout.height;

    const yAxisKeys = Object.keys(fullLayout)
        .filter(key => key.startsWith('yaxis') && fullLayout[key] && typeof fullLayout[key]._offset === 'number')
        .sort((a, b) => {
            const numA = (a === 'yaxis') ? 1 : parseInt(a.substring(5)) || Infinity;
            const numB = (b === 'yaxis') ? 1 : parseInt(b.substring(5)) || Infinity;
            return numA - numB;
        });

    for (const yAxisKey of yAxisKeys) {
        const yAxis = fullLayout[yAxisKey];
        if (!yAxis || typeof yAxis._offset !== 'number' || typeof yAxis._length !== 'number') continue;
        // Corrected yMinPaper and yMaxPaper calculation:
        const yMinPaper = yAxis._offset;
        const yMaxPaper = yAxis._offset + yAxis._length;

        //Consider the possible inversion
        const isYInverted = yAxis.range && yAxis.range[0] > yAxis.range[1];
        const isMouseInYBand = paperY >= yMinPaper && paperY <= yMaxPaper;



        if (isMouseInYBand) {
            let xAxisKeyToTest;
            if (yAxis._id === 'y') {
                //if (fullLayout['xaxis'] && typeof fullLayout['xaxis']._offset === 'number')
                xAxisKeyToTest = 'xaxis';
            } else {
                const potentialXAxisKey = 'xaxis' + yAxis._id.substring(1);
                if (fullLayout[potentialXAxisKey] && (fullLayout[potentialXAxisKey].anchor === yAxis._id || (typeof fullLayout[potentialXAxisKey].matches === 'undefined' && typeof fullLayout[potentialXAxisKey].domain !== 'undefined'))) {
                    xAxisKeyToTest = potentialXAxisKey;
                } else {
                    //xAxisKeyToTest = 'xaxis';
                    //console.warn(`Could not find X axis for ${yAxis._id}. Skipping.`);
                    continue;
                }
            }
            const xAxis = fullLayout[xAxisKeyToTest];
            if (xAxis && typeof xAxis._offset === 'number' && typeof xAxis._length === 'number') {
                const xMinPaper = xAxis._offset;
                const xMaxPaper = xAxis._offset + xAxis._length;
                const isMouseInXBand = paperX >= xMinPaper && paperX <= xMaxPaper;

                if (isMouseInXBand) {
                    return { xref: xAxis._id, yref: yAxis._id };
                }
            }
        }
    }
    return null;
}

function updateOrAddCrosshairVLine(gd, xDataValue, doRelayout = true) {
    if (!gd || !gd.layout || !gd.layout.xaxis || !xDataValue) {
        return;
    }
    let shapes = gd.layout.shapes || [];
    shapes = shapes.filter(shape => shape.name !== CROSSHAIR_VLINE_NAME); // From config.js

    const lineDefinition = {
        type: 'line',
        name: CROSSHAIR_VLINE_NAME, // From config.js
        isSystemShape: true, // Mark as a system shape
        xref: 'x',
        yref: 'paper',
        x0: xDataValue,
        y0: 0,
        x1: xDataValue,
        y1: 1,
        line: {
            color: 'rgba(100, 100, 100, 0.6)',
            width: 1,
            dash: 'dash'
        },
           layer: 'above',
           editable: false // Explicitly make crosshair not editable
    };
    shapes.push(lineDefinition);

    if (doRelayout) {
        Plotly.relayout(gd, { shapes: shapes });
    } else {
        gd.layout.shapes = shapes;
    }
}

function removeCrosshairVLine(gd, doRelayout = true) {
    if (!gd || !gd.layout || !gd.layout.shapes) return false;
    const initialLength = gd.layout.shapes.length;
    gd.layout.shapes = gd.layout.shapes.filter(shape => shape.name !== CROSSHAIR_VLINE_NAME); // From config.js
    const removed = gd.layout.shapes.length < initialLength;
    if (removed && doRelayout) {
        Plotly.relayout(gd, { shapes: gd.layout.shapes });
    }
    return removed;
}

// Make functions globally available for main.js
window.updateOrAddCrosshairVLine = updateOrAddCrosshairVLine;
window.initializeChartInteractions = initializeChartInteractions;

function handleShapeClick(event) {

    // Only handle clicks if we're not in drawing mode and not dragging
    if (window.isDraggingShape || !window.gd) {
        return;
    }

    // Check if this might be a buy signal click FIRST before checking other shapes
    if (window.gd && window.gd.layout && window.gd.layout.shapes) {
        const currentShapes = window.gd.layout.shapes;
        const rect = window.chartDiv.getBoundingClientRect();
        const mouseX_div = event.clientX - rect.left;
        const mouseY_div = event.clientY - rect.top;
        const plotMargin = window.gd._fullLayout.margin;
        const mouseX_paper = mouseX_div;
        const mouseY_paper = mouseY_div;

        // Look for buy signal shapes specifically
        for (let i = 0; i < currentShapes.length; i++) {
            const shape = currentShapes[i];

            // Check if this is a buy signal shape
            if (shape.name && shape.name.startsWith('buy_signal_') && shape.systemType === 'buy_signal' && shape.signalData) {

                // Check if the click is close to this shape
                const xrefKey = getAxisLayoutKey(shape.xref, 'xaxis');
                const yrefKey = getAxisLayoutKey(shape.yref, 'yaxis');
                const shapeXaxis = window.gd._fullLayout[xrefKey];
                const shapeYaxis = window.gd._fullLayout[yrefKey];

                if (!shapeXaxis || !shapeYaxis || typeof shapeXaxis.d2p !== 'function' || typeof shapeYaxis.d2p !== 'function') {
                    continue;
                }

                let shapeX0Val = (shapeXaxis.type === 'date') ? ((shape.x0 instanceof Date) ? shape.x0.getTime() : new Date(shape.x0).getTime()) : Number(shape.x0);
                let shapeX1Val = (shapeXaxis.type === 'date') ? ((shape.x1 instanceof Date) ? shape.x1.getTime() : new Date(shape.x1).getTime()) : Number(shape.x1);

                const p0y = shapeYaxis.d2p(shape.y0);
                const p1y = shapeYaxis.d2p(shape.y1);
                const p0x = shapeXaxis.d2p(shapeX0Val);
                const p1x = shapeXaxis.d2p(shapeX1Val);

                const p0 = { x: shapeXaxis._offset + p0x, y: shapeYaxis._offset + p0y };
                const p1 = { x: shapeXaxis._offset + p1x, y: shapeYaxis._offset + p1y };

                if (!isNaN(p0.x) && !isNaN(p0.y) && !isNaN(p1.x) && !isNaN(p1.y)) {
                    const distSq = distToSegmentSquared({ x: mouseX_paper, y: mouseY_paper }, p0, p1);
                    const CLICK_THRESHOLD = 30; // Larger threshold for buy signals since they're horizontal lines

                    if (distSq < CLICK_THRESHOLD * CLICK_THRESHOLD) {
                        // Call the buy signal modal
                        if (typeof window.displayBuySignalDetails === 'function') {
                            window.displayBuySignalDetails(shape.signalData);
                        } else {
                            console.error('[DEBUG] handleShapeClick - displayBuySignalDetails function not found');
                        }

                        // Prevent event bubbling
                        event.stopPropagation();
                        return; // Exit early, don't check other shapes
                    }
                }
            }
        }
    }

    // Get mouse coordinates early - needed for YouTube marker detection
    const rect = window.chartDiv.getBoundingClientRect();
    const mouseX_div = event.clientX - rect.left;
    const mouseY_div = event.clientY - rect.top;

    const plotMargin = window.gd._fullLayout.margin;
    const mouseX_paper = mouseX_div;
    const mouseY_paper = mouseY_div;

    // Check if this might be a YouTube marker click - use same coordinate system as line shapes
    if (window.gd && window.gd.data && window.gd._fullLayout) {
        // Check if any YouTube marker traces exist
        for (let i = 0; i < window.gd.data.length; i++) {
            const trace = window.gd.data[i];
            if (trace.name === 'YouTube Videos' && trace.type === 'scatter' && trace.mode === 'markers') {

                // Find the closest marker to the click position using same coordinate system as lines
                if (trace.x && trace.y && trace.x.length > 0) {
                    let closestIndex = -1;
                    let minDistance = Infinity;

                    for (let j = 0; j < trace.x.length; j++) {
                        // Convert data coordinates to pixel coordinates (same as line shapes)
                        const xVal = trace.x[j];
                        const yVal = trace.y[j];

                        // Get the axis information
                        const xAxis = window.gd._fullLayout.xaxis;
                        const yAxis = window.gd._fullLayout.yaxis;

                        if (xAxis && yAxis && typeof xAxis.d2p === 'function' && typeof yAxis.d2p === 'function') {
                            const pixelX = xAxis._offset + xAxis.d2p(xVal instanceof Date ? xVal.getTime() : xVal);
                            const pixelY = yAxis._offset + yAxis.d2p(yVal);


                            // Calculate distance using same paper coordinates as line shapes
                            const distance = Math.sqrt(Math.pow(mouseX_paper - pixelX, 2) + Math.pow(mouseY_paper - pixelY, 2));


                            if (distance < minDistance && distance < 25) { // Slightly larger threshold for markers
                                minDistance = distance;
                                closestIndex = j;
                            }
                        }
                    }

                    if (closestIndex !== -1) {

                        // Get marker data
                        const transcript = trace.transcripts ? trace.transcripts[closestIndex] : 'No description available';
                        const title = trace.text ? trace.text[closestIndex] : 'Unknown title';
                        const videoId = trace.video_ids ? trace.video_ids[closestIndex] : '';
                        const publishedDate = trace.customdata ? trace.customdata[closestIndex] : '';


                        // Show the YouTube modal directly
                        if (window.youtubeMarkersManager && window.youtubeMarkersManager.showTranscriptModal) {
                            window.youtubeMarkersManager.showTranscriptModal(title, transcript, videoId, publishedDate);
                        } else {
                            console.error('[DEBUG] handleShapeClick - YouTube markers manager not available');
                        }

                        // Prevent event bubbling to avoid conflicts
                        event.stopPropagation();
                        return;
                    }
                }

            }
        }
    }


    // Check if click is on a shape by finding the closest shape to the click position

    if (!window.gd._fullLayout) {
        return;
    }


    // Get the hovered subplot first (similar to colorTheLine function)
    const hoveredSubplotRefs = getSubplotRefsAtPaperCoords(mouseX_paper, mouseY_paper, window.gd._fullLayout);

    // Find the closest shape to the click position
    const currentShapes = window.gd.layout.shapes || [];
    let closestShape = null;
    let minDistance = Infinity;
    const CLICK_THRESHOLD = 20; // pixels

    for (let i = 0; i < currentShapes.length; i++) {
        const shape = currentShapes[i];
        if (((shape.type === 'line' || shape.type === 'rect' || shape.type === 'rectangle' || shape.type === 'box') && shape.id && !shape.isSystemShape) ||
            (shape.type === 'rect' && shape.id && !shape.isSystemShape)) {
            const xrefKeyForFilter = getAxisLayoutKey(shape.xref, 'xaxis');
            const yrefKeyForFilter = getAxisLayoutKey(shape.yref, 'yaxis');
            const shapeXaxisForFilter = window.gd._fullLayout[xrefKeyForFilter];
            const shapeYaxisForFilter = window.gd._fullLayout[yrefKeyForFilter];

            if (!shapeXaxisForFilter || !shapeYaxisForFilter) {
                continue;
            }

            // Allow all shapes for click detection regardless of subplot location
            // This ensures clicks work on any shape anywhere on the chart

            const xrefKey = getAxisLayoutKey(shape.xref, 'xaxis');
            const yrefKey = getAxisLayoutKey(shape.yref, 'yaxis');
            const shapeXaxis = window.gd._fullLayout[xrefKey];
            const shapeYaxis = window.gd._fullLayout[yrefKey];

            if (!shapeXaxis || !shapeYaxis || typeof shapeXaxis.d2p !== 'function' || typeof shapeYaxis.d2p !== 'function') {
                continue;
            }

            if (shape.type === 'line') {
                // Handle line shapes with segment distance calculation
                let shapeX0Val = (shapeXaxis.type === 'date') ? ((shape.x0 instanceof Date) ? shape.x0.getTime() : new Date(shape.x0).getTime()) : Number(shape.x0);
                let shapeX1Val = (shapeXaxis.type === 'date') ? ((shape.x1 instanceof Date) ? shape.x1.getTime() : new Date(shape.x1).getTime()) : Number(shape.x1);

                const p0y = shapeYaxis.d2p(shape.y0);
                const p1y = shapeYaxis.d2p(shape.y1);
                const p0x = shapeXaxis.d2p(shapeX0Val);
                const p1x = shapeXaxis.d2p(shapeX1Val);

                const p0 = { x: shapeXaxis._offset + p0x, y: shapeYaxis._offset + p0y };
                const p1 = { x: shapeXaxis._offset + p1x, y: shapeYaxis._offset + p1y };


                if (!isNaN(p0.x) && !isNaN(p0.y) && !isNaN(p1.x) && !isNaN(p1.y)) {
                    const distSq = distToSegmentSquared({ x: mouseX_paper, y: mouseY_paper }, p0, p1);
                    if (distSq < CLICK_THRESHOLD * CLICK_THRESHOLD && distSq < minDistance) {
                        minDistance = distSq;
                        closestShape = shape;
                    }
                } else {
                }
            } else if (shape.type === 'rect' || shape.type === 'rectangle' || shape.type === 'box') {
                // Handle rectangle shapes with point-in-rectangle calculation
                let shapeX0Val = (shapeXaxis.type === 'date') ? ((shape.x0 instanceof Date) ? shape.x0.getTime() : new Date(shape.x0).getTime()) : Number(shape.x0);
                let shapeX1Val = (shapeXaxis.type === 'date') ? ((shape.x1 instanceof Date) ? shape.x1.getTime() : new Date(shape.x1).getTime()) : Number(shape.x1);

                const rectLeft = Math.min(shapeXaxis.d2p(shapeX0Val), shapeXaxis.d2p(shapeX1Val)) + shapeXaxis._offset;
                const rectRight = Math.max(shapeXaxis.d2p(shapeX0Val), shapeXaxis.d2p(shapeX1Val)) + shapeXaxis._offset;
                const rectTop = Math.min(shapeYaxis.d2p(Math.min(shape.y0, shape.y1)), shapeYaxis.d2p(Math.max(shape.y0, shape.y1))) + shapeYaxis._offset;
                const rectBottom = Math.max(shapeYaxis.d2p(Math.min(shape.y0, shape.y1)), shapeYaxis.d2p(Math.max(shape.y0, shape.y1))) + shapeYaxis._offset;

                // Check if mouse point is inside rectangle bounds
                if (mouseX_paper >= rectLeft && mouseX_paper <= rectRight &&
                    mouseY_paper >= rectTop && mouseY_paper <= rectBottom) {
                    // For rectangles, use a small distance from center as a tiebreaker for closest selection
                    const rectCenterX = (rectLeft + rectRight) / 2;
                    const rectCenterY = (rectTop + rectBottom) / 2;
                    const centerDistSq = Math.pow(mouseX_paper - rectCenterX, 2) + Math.pow(mouseY_paper - rectCenterY, 2);

                    if (centerDistSq < minDistance) {
                        minDistance = centerDistSq;
                        closestShape = shape;
                    }
                }
            }
        }
    }
    // console.groupEnd();

    // Handle shape selection
    if (closestShape) {
        const isCtrlPressed = event.ctrlKey || event.metaKey; // Support both Ctrl (Windows/Linux) and Cmd (Mac)
        const shapeId = closestShape.id;

        if (isCtrlPressed) {
            // Multi-select: toggle selection
            if (window.isShapeSelected(closestShape.id)) {
                window.deselectShape(closestShape.id);
            } else {
                window.selectShape(closestShape.id, true); // true for multi-select
            }
        } else {
            // Single select: clear previous selections and select this shape
            if (!window.isShapeSelected(closestShape.id) || window.getSelectedShapeCount() > 1) {
                window.selectShape(closestShape.id, false); // false for single select
            } else {
                // Clicking on already selected shape - deselect it
                window.deselectShape(closestShape.id);
            }
        }

        // Update visual feedback by calling updateShapeVisuals directly for immediate selection color change
        if (typeof updateShapeVisuals === 'function') {
            updateShapeVisuals();
        } else {
            console.error('[DEBUG] handleShapeClick - updateShapeVisuals not available!');
        }

        // Update info panel with the currently selected shape object
        const currentShapes = window.gd.layout.shapes || [];
        const selectedShape = currentShapes.find(s => s.id === shapeId);
        if (selectedShape) {
            const selectedShapeObject = {
                id: selectedShape.id,
                index: currentShapes.indexOf(selectedShape),
                shape: selectedShape
            };
            updateSelectedShapeInfoPanel(selectedShapeObject);
        }

        // Prevent event bubbling to avoid conflicts
        event.stopPropagation();
    } else {
        // Clicked on empty space - deselect all shapes
        if (window.getSelectedShapeCount() > 0) {
            window.deselectAllShapes();
            if (typeof colorTheLine === 'function') {
                colorTheLine(event);
            }
            updateSelectedShapeInfoPanel(null);
        }
    }
}

function initializeChartInteractions() {

    // Double-click handling is now done by plotlyEventHandlers.js plotly_click event
    // addDoubleClickHandler(); // Disabled to prevent conflicts

    // Add shape selection click handler
    window.chartDiv.addEventListener('click', handleShapeClick, { capture: true, passive: true });

    // Throttle mousemove events to prevent excessive processing
    let mousemoveThrottleTimer = null;
    let isMouseDown = false;
    let isTouchActive = false;


    // Make throttling state global for debugging
    window.mousemoveThrottleTimer = mousemoveThrottleTimer;
    window.isMouseDown = isMouseDown;
    window.isTouchActive = isTouchActive;

    window.chartDiv.addEventListener('mousedown', function() {
        isMouseDown = true;
    }, { capture: true, passive: true });

    window.chartDiv.addEventListener('mouseup', function() {
        isMouseDown = false;
    }, { capture: true, passive: true });

    // Global mouseup handler to ensure isMouseDown is reset even if mouse is released outside chart
    document.addEventListener('mouseup', function() {
        isMouseDown = false;
    }, { capture: true, passive: true });

    // Store the last mouse event globally for accurate hover detection
    window.lastMouseEvent = null;

    // Add a test function to manually trigger colorTheLine
    window.testColorTheLine = function() {
        if (window.lastMouseEvent) {
            colorTheLine(window.lastMouseEvent);
        } else {
            const rect = window.chartDiv.getBoundingClientRect();
            const testEvent = {
                clientX: rect.left + rect.width / 2,
                clientY: rect.top + rect.height / 2,
                target: window.chartDiv
            };
            colorTheLine(testEvent);
        }
    };

    window.chartDiv.addEventListener('mousemove', async function(event) {

        // Only process mousemove when mouse is not pressed (to avoid interfering with drags)
        if (isMouseDown) {
            return;
        }

        if (mousemoveThrottleTimer) {
            return; // Skip if already processing
        }

        // Store the current mouse event globally for use in other functions
        window.lastMouseEvent = event;

        mousemoveThrottleTimer = true; // Set flag
        await delay(16); // ~60fps throttling
        colorTheLine(event); // Pass the event for coordinate detection
        mousemoveThrottleTimer = null;
    }, { capture: true, passive: true });

    // Touch event handling for mobile devices
    window.chartDiv.addEventListener('touchstart', function(event) {
        isTouchActive = true;
        // Prevent default to avoid scrolling/zooming conflicts
        event.preventDefault();

        // Convert touch to mouse-like event for colorTheLine function
        const touch = event.touches[0] || event.changedTouches[0];
        const mouseEvent = {
            clientX: touch.clientX,
            clientY: touch.clientY,
            target: event.target,
            preventDefault: () => {},
            stopPropagation: () => {}
        };

        colorTheLine(mouseEvent);
    }, { capture: true, passive: false });

    window.chartDiv.addEventListener('touchmove', async function(event) {
        if (!isTouchActive) return;

        // Throttle touchmove events
        if (mousemoveThrottleTimer) return;

        // Prevent default to avoid scrolling/zooming
        event.preventDefault();

        mousemoveThrottleTimer = true; // Set flag
        await delay(16); // ~60fps throttling
        // Convert touch to mouse-like event
        const touch = event.touches[0] || event.changedTouches[0];
        const mouseEvent = {
            clientX: touch.clientX,
            clientY: touch.clientY,
            target: event.target,
            preventDefault: () => {},
            stopPropagation: () => {}
        };

        colorTheLine(mouseEvent);
        mousemoveThrottleTimer = null;
    }, { capture: true, passive: false });

    window.chartDiv.addEventListener('touchend', function(event) {
        isTouchActive = false;

        // Clear any pending throttled events
        if (mousemoveThrottleTimer) {
            clearTimeout(mousemoveThrottleTimer);
            mousemoveThrottleTimer = null;
        }

        // On mobile, if a shape was being hovered (touched), select it before clearing hover
        if (window.hoveredShapeBackendId !== null) {
            // Select the shape on tap
            window.selectShape(window.hoveredShapeBackendId, false); // false for single select

            // Update the info panel with the selected shape details
            if (!window.selectedShapeInfoDiv) {
                window.selectedShapeInfoDiv = document.getElementById('selected-shape-info');
            }
            if (window.selectedShapeInfoDiv) {
                const currentShapes = window.gd.layout.shapes || [];
                const selectedShape = currentShapes.find(s => s.id === window.hoveredShapeBackendId);
                if (selectedShape) {
                    const selectedShapeObject = {
                        id: selectedShape.id,
                        index: currentShapes.indexOf(selectedShape),
                        shape: selectedShape
                    };
                    updateSelectedShapeInfoPanel(selectedShapeObject);
                }
            }

            // Update visuals for the new selection
            if (typeof updateShapeVisuals === 'function') {
                updateShapeVisuals();
            }
        }

        // Clear hover state when touch ends
        // This prevents shapes from staying highlighted after touch ends
        if (window.hoveredShapeBackendId !== null) {
            window.hoveredShapeBackendId = null;
            if (typeof updateShapeVisuals === 'function') {
                updateShapeVisuals();
            }
        }
    }, { capture: true, passive: true });

    window.chartDiv.addEventListener('touchcancel', function(event) {
        isTouchActive = false;

        // Clear any pending throttled events
        if (mousemoveThrottleTimer) {
            clearTimeout(mousemoveThrottleTimer);
            mousemoveThrottleTimer = null;
        }

        // Clear hover state when touch is cancelled
        if (window.hoveredShapeBackendId !== null) {
            window.hoveredShapeBackendId = null;
            if (typeof updateShapeVisuals === 'function') {
                updateShapeVisuals();
            }
        }
    }, { capture: true, passive: true });

    window.chartDiv.addEventListener('mouseleave', function() {
        if (window.hoveredShapeBackendId !== null) { // From state.js
            window.hoveredShapeBackendId = null;
            if (typeof updateShapeVisuals === 'function') {
                updateShapeVisuals();
            }
        }
        // DISABLED: Crosshair interferes with panning
        /*
        removeCrosshairVLine(window.gd, true); // removeCrosshairVLine from this file
        if (window.cursorTimeDisplay) window.cursorTimeDisplay.textContent = 'N/A'; // From main.js
        updateSelectedShapeInfoPanel(null)
        if (window.cursorPriceDisplay) window.cursorPriceDisplay.textContent = 'N/A'; // From main.js
        */
    });
    
    // Add mouse wheel event listener to handle zoom if Plotly's default doesn't work
    window.chartDiv.addEventListener('wheel', function(event) {
        // Only handle zoom if Ctrl key is pressed (standard browser zoom behavior)
        if (event.ctrlKey) {
            event.preventDefault(); // Prevent default browser zoom behavior
            
            // Use Plotly's zoom functionality
            if (window.gd && window.gd.layout && window.gd._fullLayout) {
                // Calculate zoom factor based on wheel delta
                const zoomIntensity = 0.1; // Adjust as needed
                const direction = event.deltaY > 0 ? 1 : -1; // Positive deltaY means scrolling down (zoom out)
                
                // Get current axis ranges
                const currentXRange = window.gd.layout.xaxis.range;
                const currentYRange = window.gd.layout.yaxis.range;
                
                if (currentXRange && currentYRange) {
                    // Calculate new ranges based on zoom direction
                    const xRange = currentXRange[1] - currentXRange[0];
                    const newXRange = [
                        currentXRange[0] + (xRange * direction * zoomIntensity) / 2,
                        currentXRange[1] - (xRange * direction * zoomIntensity) / 2
                    ];
                    
                    const yRange = currentYRange[1] - currentYRange[0];
                    const newYRange = [
                        currentYRange[0] + (yRange * direction * zoomIntensity) / 2,
                        currentYRange[1] - (yRange * direction * zoomIntensity) / 2
                    ];
                    
                    // Apply new ranges
                    Plotly.relayout(window.gd, {
                        'xaxis.range[0]': newXRange[0],
                        'xaxis.range[1]': newXRange[1],
                        'yaxis.range[0]': newYRange[0],
                        'yaxis.range[1]': newYRange[1]
                    });
                }
            }
        }
    }, { passive: false }); // Use passive: false to allow preventDefault

    document.addEventListener('keydown', async function(event) {
        if (!window.gd || !window.gd.layout) return;

        // Handle Escape key to deselect all shapes
        if (event.key === 'Escape') {
            if (window.getSelectedShapeCount() > 0) {
                event.preventDefault();
                window.deselectAllShapes();
                if (typeof updateShapeVisuals === 'function') {
                    updateShapeVisuals();
                }
                updateSelectedShapeInfoPanel(null);
            }
            return;
        }

        // Handle Ctrl+A to select all shapes
        if (event.key === 'a' && (event.ctrlKey || event.metaKey)) {
            event.preventDefault();
            const currentShapes = window.gd?.layout?.shapes || [];
            const shapeIds = currentShapes
                .filter(s => s.id && !s.isSystemShape)
                .map(s => s.id);

            if (shapeIds.length > 0) {
                shapeIds.forEach(id => window.selectShape(id, true));
                if (typeof updateShapeVisuals === 'function') {
                    updateShapeVisuals();
                }
                updateSelectedShapeInfoPanel(window.activeShapeForPotentialDeletion);
            }
            return;
        }

        // Handle arrow keys for navigation between selected shapes
        if (['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'].includes(event.key)) {
            const selectedIds = window.getSelectedShapeIds();
            if (selectedIds.length > 1) {
                event.preventDefault();
                const currentIndex = selectedIds.indexOf(window.lastSelectedShapeId);
                let newIndex;

                switch (event.key) {
                    case 'ArrowUp':
                    case 'ArrowLeft':
                        newIndex = currentIndex > 0 ? currentIndex - 1 : selectedIds.length - 1;
                        break;
                    case 'ArrowDown':
                    case 'ArrowRight':
                        newIndex = currentIndex < selectedIds.length - 1 ? currentIndex + 1 : 0;
                        break;
                }

                if (newIndex !== currentIndex) {
                    window.lastSelectedShapeId = selectedIds[newIndex];
                    // Update activeShapeForPotentialDeletion for the new last selected shape
                        const currentShapes = window.gd?.layout?.shapes || [];
                        const shape = currentShapes.find(s => s.id === window.lastSelectedShapeId);
                        if (shape) {
                            const shapeIndex = currentShapes.indexOf(shape);
                            window.activeShapeForPotentialDeletion = {
                                id: window.lastSelectedShapeId,
                                index: shapeIndex,
                                shape: shape
                            };
                        }
                    if (typeof updateShapeVisuals === 'function') {
                        updateShapeVisuals();
                    }
                    updateSelectedShapeInfoPanel(window.activeShapeForPotentialDeletion);
                }
            }
            return;
        }

        // Handle Delete/Backspace for shape deletion
        if (event.key === 'Delete' || event.key === 'Backspace') {
            const targetElement = event.target;
            if (targetElement.tagName === 'INPUT' || targetElement.tagName === 'TEXTAREA' || targetElement.isContentEditable) {
                if (targetElement !== document.body && targetElement !== window.chartDiv && !window.chartDiv.contains(targetElement)) {
                    return;
                }
            }

            const selectedShapeIds = window.getSelectedShapeIds();
            if (selectedShapeIds.length > 0) {
                event.preventDefault();

                const symbol = window.symbolSelect.value; // From main.js

                if (!symbol) {
                    console.warn("Cannot delete drawings via key: No symbol selected.");
                    return;
                }

                // Delete all selected shapes via WebSocket
                const deletePromises = selectedShapeIds.map(async (drawingId) => {
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
                            return drawingId;
                        } else {
                            throw new Error('WebSocket not connected');
                        }
                    } catch (error) {
                        console.error(`Error deleting drawing ${drawingId} via key press:`, error);
                        return null; // Return null for failed deletions
                    }
                });

                try {
                    const results = await Promise.all(deletePromises);
                    const successfulDeletions = results.filter(id => id !== null);

                    if (successfulDeletions.length > 0) {
                        // Remove successfully deleted shapes from the chart
                        const currentShapes = window.gd.layout.shapes || [];
                        const remainingShapes = currentShapes.filter(s => !successfulDeletions.includes(s.id));

                        if (remainingShapes.length !== currentShapes.length) {
                            Plotly.relayout(window.gd, { shapes: remainingShapes }).catch(err => {
                                console.error("Error relayouting after shape removal by keydown:", err);
                                loadDrawingsAndRedraw(symbol);
                            });
                        } else {
                            loadDrawingsAndRedraw(symbol);
                        }

                    }

                    // Clear selection regardless of success/failure
                    window.deselectAllShapes();
                    if (typeof updateShapeVisuals === 'function') {
                        updateShapeVisuals();
                    }
                    updateSelectedShapeInfoPanel(null);

                    // Show error message if some deletions failed
                    const failedCount = selectedShapeIds.length - (results.filter(id => id !== null).length);
                    if (failedCount > 0) {
                        alert(`Failed to delete ${failedCount} out of ${selectedShapeIds.length} shapes. Check console for details.`);
                    }

                } catch (error) {
                    console.error('Error during batch shape deletion:', error);
                    alert(`Failed to delete shapes: ${error.message}`);
                    window.deselectAllShapes();
                    if (typeof updateShapeVisuals === 'function') {
                        updateShapeVisuals();
                    }
                    updateSelectedShapeInfoPanel(null);
                }
            }
        }
    });
}

// debouncedUpdateCrosshair will be defined in main.js

function removeCrosshairVLine(gd, doRelayout = true) {
    if (!gd || !gd.layout || !gd.layout.shapes) return false;
    const initialLength = gd.layout.shapes.length;
    gd.layout.shapes = gd.layout.shapes.filter(shape => shape.name !== CROSSHAIR_VLINE_NAME); // From config.js
    const removed = gd.layout.shapes.length < initialLength;
    if (removed && doRelayout) {
        Plotly.relayout(gd, { shapes: gd.layout.shapes });
    }
    return removed;
}


function findAndupdateSelectedShapeInfoPanel(id) {
        const currentShapes = window.gd.layout.shapes || [];
        let foundShape = null
        for (let i = 0; i < currentShapes.length; i++) {
            const shape = currentShapes[i];
            if(shape.id === id) {
            foundShape = shape;
            break;
            }
        }

        if(foundShape) {
            const activeShape = { id: foundShape.id, index: currentShapes.indexOf(foundShape), shape: foundShape };
            updateSelectedShapeInfoPanel(activeShape);
        } else {
            updateSelectedShapeInfoPanel(null)
        }
}

// Long press detection is now handled by plotlyEventHandlers.js
// Removed handleShapeDoubleClick function

// Long press detection is now handled by plotlyEventHandlers.js
// Removed addDoubleClickHandler function
