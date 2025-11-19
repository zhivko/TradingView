async function updateSelectedShapeInfoPanel(activeShape) {
    // Get the element directly to ensure it works across all contexts
    let selectedShapeInfoDiv = window.selectedShapeInfoDiv;
    if (!selectedShapeInfoDiv) {
        selectedShapeInfoDiv = document.getElementById('selected-shape-info');
        window.selectedShapeInfoDiv = selectedShapeInfoDiv; // Cache it globally
    }
    if (!selectedShapeInfoDiv) {
        return;
    }

    const selectedCount = window.getSelectedShapeCount();
    const selectedIds = window.getSelectedShapeIds();

    // Determine what to display based on selection and hover state
    const isHovering = activeShape && activeShape.id;
    const hasSelection = selectedCount > 0;

    function formatShapeInfo(shape, prefix = '') {
        let info = `<p><strong>${prefix}Type:</strong> ${shape.type}</p>`;
        info += `<p><strong>${prefix}ID:</strong> ${shape.id}</p>`;

        // Format coordinates
        const x0 = shape.x0 ? (shape.x0 instanceof Date ? shape.x0.toISOString() : shape.x0) : 'N/A';
        const y0 = shape.y0 !== undefined ? Number(shape.y0).toFixed(6) : 'N/A';
        const x1 = shape.x1 ? (shape.x1 instanceof Date ? shape.x1.toISOString() : shape.x1) : 'N/A';
        const y1 = shape.y1 !== undefined ? Number(shape.y1).toFixed(6) : 'N/A';

        info += `<p><strong>${prefix}Start:</strong> (${x0}, ${y0})</p>`;
        info += `<p><strong>${prefix}End:</strong> (${x1}, ${y1})</p>`;

        // Add other relevant properties
        if (shape.xref) info += `<p><strong>${prefix}X Reference:</strong> ${shape.xref}</p>`;
        if (shape.yref) info += `<p><strong>${prefix}Y Reference:</strong> ${shape.yref}</p>`;
        if (shape.line && shape.line.color) info += `<p><strong>${prefix}Color:</strong> ${shape.line.color}</p>`;

        return info;
    }

    async function formatShapeProperties(shapeId) {
        try {
            const symbol = window.symbolSelect ? window.symbolSelect.value : null;
            if (!symbol) return '';

            if (window.wsAPI && window.wsAPI.connected) {
                const requestId = Date.now().toString();

                const fetchPromise = new Promise((resolve, reject) => {
                    const timeout = setTimeout(() => {
                        reject(new Error('Timeout waiting for shape properties'));
                    }, 5000);

                    const messageHandler = (message) => {
                        if (message.type === 'shape_properties_response' && message.request_id === requestId) {
                            clearTimeout(timeout);
                            window.wsAPI.offMessage('shape_properties_response', messageHandler);
                            window.wsAPI.offMessage('error', messageHandler);
                            resolve(message.data);
                        } else if (message.type === 'error' && message.request_id === requestId) {
                            clearTimeout(timeout);
                            window.wsAPI.offMessage('shape_properties_response', messageHandler);
                            window.wsAPI.offMessage('error', messageHandler);
                            reject(new Error(message.message || 'Failed to fetch shape properties'));
                        }
                    };

                    window.wsAPI.onMessage('shape_properties_response', messageHandler);
                    window.wsAPI.onMessage('error', messageHandler);
                });

                window.wsAPI.sendMessage({
                    type: 'shape',
                    action: 'get_properties',
                    data: {
                        symbol: symbol,
                        drawing_id: shapeId
                    },
                    request_id: requestId
                });

                const result = await fetchPromise;

                if (result && result.properties) {
                    const props = result.properties;
                    let propHtml = '<p><strong>Trading Properties:</strong></p>';

                    if (props.buyOnCross !== undefined) {
                        propHtml += `<p><strong>Buy on Cross:</strong> ${props.buyOnCross ? 'Yes' : 'No'}</p>`;
                    }
                    if (props.sellOnCross !== undefined) {
                        propHtml += `<p><strong>Sell on Cross:</strong> ${props.sellOnCross ? 'Yes' : 'No'}</p>`;
                    }
                    if (props.sendEmailOnCross !== undefined) {
                        propHtml += `<p><strong>Email on Cross:</strong> ${props.sendEmailOnCross ? 'Yes' : 'No'}</p>`;
                    }
                    if (props.amount !== undefined) {
                        propHtml += `<p><strong>Amount:</strong> ${props.amount}</p>`;
                    }
                    if (props.amountPercent !== undefined) {
                        propHtml += `<p><strong>Amount %:</strong> ${props.amountPercent}%</p>`;
                    }
                    if (props.amountUsdt !== undefined) {
                        propHtml += `<p><strong>Amount USDT:</strong> ${props.amountUsdt}</p>`;
                    }
                    if (props.emailSent !== undefined) {
                        propHtml += `<p><strong>Email Sent:</strong> ${props.emailSent ? 'Yes' : 'No'}</p>`;
                    }
                    if (props.buy_sent !== undefined) {
                        propHtml += `<p><strong>Buy Sent:</strong> ${props.buy_sent ? 'Yes' : 'No'}</p>`;
                    }
                    if (props.sell_sent !== undefined) {
                        propHtml += `<p><strong>Sell Sent:</strong> ${props.sell_sent ? 'Yes' : 'No'}</p>`;
                    }
                    if (props.emailDate) {
                        propHtml += `<p><strong>Email Date:</strong> ${new Date(props.emailDate).toLocaleString()}</p>`;
                    }
                    if (props.alert_actions) {
                        const actions = Array.isArray(props.alert_actions) ? props.alert_actions.join(', ') : props.alert_actions;
                        propHtml += `<p><strong>Alert Actions:</strong> ${actions}</p>`;
                    }

                    return propHtml;
                }
            }
        } catch (error) {
            console.error('Error fetching shape properties for panel:', error);
        }
        return '';
    }

    if (hasSelection || isHovering) {
        let infoHtml = '';

        if (hasSelection) {
            infoHtml += `<p><strong>${selectedCount} shape${selectedCount > 1 ? 's' : ''} selected</strong></p>`;

            if (selectedCount === 1 && activeShape && activeShape.id) {
                infoHtml += formatShapeInfo(activeShape.shape);
                // Fetch and display trading properties for single selected shape
                const propertiesHtml = await formatShapeProperties(activeShape.id);
                infoHtml += propertiesHtml;
            } else if (selectedCount > 1) {
                infoHtml += '<p><strong>Selected Shapes:</strong></p><ul>';
                selectedIds.forEach(id => {
                    const isLastSelected = id === window.lastSelectedShapeId;
                    const shape = window.gd && window.gd.layout.shapes ? window.gd.layout.shapes.find(s => s.id === id) : null;
                    const shapeInfo = shape ? `${shape.type} (${shape.x0 ? '...' : 'rect'})` : 'Unknown';
                    infoHtml += `<li${isLastSelected ? ' style="font-weight: bold; color: #00FF00;"' : ''}>${id} - ${shapeInfo}${isLastSelected ? ' (last selected)' : ''}</li>`;
                });
                infoHtml += '</ul>';
                if (activeShape && activeShape.shape) {
                    infoHtml += '<p><strong>Last Selected Shape Details:</strong></p>';
                    infoHtml += formatShapeInfo(activeShape.shape);
                    // Fetch and display trading properties for last selected shape
                    const propertiesHtml = await formatShapeProperties(activeShape.id);
                    infoHtml += propertiesHtml;
                }
            }
        } else if (isHovering) {
            // Handle hovered shape (no shapes selected but hovering over one)
            infoHtml = `<p><strong>Hovered Shape:</strong></p>`;
            infoHtml += formatShapeInfo(activeShape.shape, 'Hovered ');
        }

        window.selectedShapeInfoDiv.innerHTML = infoHtml;
        window.activeShapeForPotentialDeletion = activeShape;

        // Show the "Delete line" and "Edit line" buttons when shapes are selected OR hovered
        if (window.deleteShapeBtn) {
            window.deleteShapeBtn.style.display = 'inline-block';
        } else {
        }
        if (window.editShapeBtn) {
            window.editShapeBtn.style.display = 'inline-block';
        } else {
        }
    } else {
        window.selectedShapeInfoDiv.innerHTML = '<p>No shape selected.</p>';

        // Hide the "Delete line" and "Edit line" buttons when no shape is selected
        if (window.deleteShapeBtn) {
            window.deleteShapeBtn.style.display = 'none';
        }
        if (window.editShapeBtn) {
            window.editShapeBtn.style.display = 'none';
        }
    }
}

async function updateShapeVisuals() {
    if (!window.gd || !window.gd.layout) {
        return;
    }
    const currentShapes = window.gd.layout.shapes || [];

    const newShapes = currentShapes.map(s => {
        const newShape = { ...s };
        if (s.line) {
            newShape.line = { ...s.line };
        } else if (s.type === 'line') {
            newShape.line = {};
        }

        // Ensure markers are present on line shapes
        if (s.type === 'line' && s.id && !s.isSystemShape) {
            // Add markers for user-drawn lines if not already present
            if (!newShape.marker) {
                newShape.marker = {
                    size: isMobileDevice() ? 34 : 24,
                    color: window.DEFAULT_DRAWING_COLOR,
                    symbol: 'circle',
                    line: { width: 3, color: 'white' },
                    opacity: 0.95
                };
            } else {
                // Ensure marker properties are complete
                newShape.marker = { ...newShape.marker };
                if (newShape.marker.size === undefined) newShape.marker.size = isMobileDevice() ? 34 : 24;
                if (newShape.marker.color === undefined) newShape.marker.color = window.DEFAULT_DRAWING_COLOR;
                if (newShape.marker.symbol === undefined) newShape.marker.symbol = 'circle';
                if (!newShape.marker.line) newShape.marker.line = { width: 3, color: 'white' };
                if (newShape.marker.opacity === undefined) newShape.marker.opacity = 0.95;
            }
        }

        if (s.id) {
            const isSelected = window.isShapeSelected ? window.isShapeSelected(s.id) : false;
            const isHovered = s.id === window.hoveredShapeBackendId;
            const isLastSelected = s.id === window.lastSelectedShapeId;


            newShape.editable = isSelected;
            if (newShape.line) {
                // Priority: Selected > Hovered > Default
                if (isSelected) {
                    // Use a different shade for the last selected shape in multi-selection
                    if ((window.getSelectedShapeCount && window.getSelectedShapeCount() > 1) && isLastSelected) {
                        newShape.line.color = '#00FF00'; // Bright green for last selected
                        newShape.line.width = 3; // Thicker line for last selected
                    } else {
                        newShape.line.color = window.SELECTED_DRAWING_COLOR;
                        newShape.line.width = 2.5; // Slightly thicker for selected
                    }
                } else if (isHovered) {
                    newShape.line.color = window.HOVER_DRAWING_COLOR;
                    newShape.line.width = 2; // Normal width for hover
                } else {
                    newShape.line.color = window.DEFAULT_DRAWING_COLOR;
                    newShape.line.width = 2; // Normal width for default
                }
                // Removed problematic onmousedown handler that was interfering with drag functionality
                // Shape properties functionality has been completely removed
            }
        }
        return newShape;
    });
    try {
        await Plotly.relayout(window.gd, { 'shapes': newShapes });
    } catch (error) {
        console.error("Error during relayout for shape editability:", error);
        console.error("Error details:", error.message, error.stack);
    }
}

// Define debounced version of updateShapeVisuals here so it's available when chartInteractions.js loads
// This needs to be defined after updateShapeVisuals function
if (typeof VISUAL_UPDATE_DEBOUNCE_DELAY === 'undefined') {
    window.VISUAL_UPDATE_DEBOUNCE_DELAY = 30; // Default value if not defined yet
}
window.debouncedUpdateShapeVisuals = debounce(updateShapeVisuals, VISUAL_UPDATE_DEBOUNCE_DELAY);
window.updateShapeVisuals = updateShapeVisuals; // Export updateShapeVisuals to global scope

function logEventToPanel(message, type = 'INFO') {
    if (!window.eventOutput) {
        console.warn("logEventToPanel: window.eventOutput element not found.");
        return;
    }

    const timestamp = new Date().toLocaleTimeString();
    const typePrefix = type.toUpperCase();
    // Ensure message is a string, in case other types are passed
    const messageString = (typeof message === 'string' || message instanceof String) ? message : JSON.stringify(message);
    const formattedMessage = `[${timestamp} ${typePrefix}] ${messageString}`;

    let currentLog = window.eventOutput.value;
    let lines = currentLog.split('\n');
    lines.unshift(formattedMessage); // Prepend new message

    if (lines.length > 100) {
        lines = lines.slice(0, 100); // Keep the latest 100 lines
    }

    window.eventOutput.value = lines.join('\n');
    window.eventOutput.scrollTop = 0; // Scroll to top to see the latest (prepended) message
}

// Update trade history visualizations
function updateTradeHistoryVisualizations() {
    if (!window.tradeHistoryData || window.tradeHistoryData.length === 0) {
        return;
    }

    // Prevent re-entrant calls that cause infinite recursion
    if (window.isUpdatingTradeHistoryVisualizations) {
        console.log('📊 Skipping trade history visualization update - already in progress');
        return;
    }

    window.isUpdatingTradeHistoryVisualizations = true;

    try {
        // Get current symbol
        const symbol = window.symbolSelect ? window.symbolSelect.value : 'UNKNOWN';

        console.log(`📊 Updating trade history visualizations: ${window.tradeHistoryData.length} trades`);

        // Re-add trade history markers to chart with all data
        if (window.addTradeHistoryMarkersToChart) {
            window.addTradeHistoryMarkersToChart(window.tradeHistoryData, symbol);
        }
    } finally {
        window.isUpdatingTradeHistoryVisualizations = false;
    }
}

// Make function globally available
window.updateTradeHistoryVisualizations = updateTradeHistoryVisualizations;
