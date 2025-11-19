// WebSocket-based API client for AppTradingView2
// This replaces HTTP calls with WebSocket message passing

class WebSocketAPI {
    constructor() {
        this.ws = null;
        this.connected = false;
        this.messageHandlers = new Map();
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.messageQueue = [];
        this.isSending = false;
    }

    connect() {
        if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
            return Promise.resolve();
        }

        return new Promise((resolve, reject) => {
            try {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const wsUrl = `${protocol}//${window.location.host}/ws`;
                console.log('Connecting to WebSocket:', wsUrl);

                this.ws = new WebSocket(wsUrl);

                this.ws.onopen = (event) => {
                    console.log('WebSocket connected');
                    this.connected = true;
                    this.reconnectAttempts = 0;
                    resolve(event);
                };

                this.ws.onmessage = (event) => {
                    this.handleMessage(event);
                };

                this.ws.onclose = (event) => {
                    console.log('WebSocket closed:', event.code, event.reason);
                    this.connected = false;
                    if (!event.wasClean && this.reconnectAttempts < this.maxReconnectAttempts) {
                        this.attemptReconnect();
                    }
                };

                this.ws.onerror = (error) => {
                    console.error('WebSocket error:', error);
                    reject(error);
                };

            } catch (error) {
                console.error('WebSocket connection failed:', error);
                reject(error);
            }
        });
    }

    attemptReconnect() {
        this.reconnectAttempts++;
        const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);

        console.log(`Attempting to reconnect in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

        setTimeout(() => {
            this.connect().catch(() => {
                if (this.reconnectAttempts >= this.maxReconnectAttempts) {
                    console.error('Max reconnection attempts reached');
                }
            });
        }, delay);
    }

    disconnect() {
        if (this.ws) {
            this.ws.close(1000, 'Client disconnecting');
        }
    }

    sendMessage(message) {
        // If not connected, try to reconnect first
        if (!this.connected || !this.ws || this.ws.readyState !== WebSocket.OPEN) {
            console.log('WebSocket not connected, attempting to reconnect...');
            // Attempt reconnection but don't block - queue the message for when connected
            this.connect().then(() => {
                console.log('WebSocket reconnected, processing queued messages');
                this.processQueue();
            }).catch(error => {
                console.error('Failed to reconnect WebSocket:', error);
            });
            
            // Queue the message anyway so it gets processed when reconnected
            this.messageQueue.push(message);
            return;
        }

        this.messageQueue.push(message);
        this.processQueue();
    }

    processQueue() {
        if (this.isSending || this.messageQueue.length === 0) {
            return;
        }

        this.isSending = true;
        const message = this.messageQueue.shift();
        const messageStr = JSON.stringify(message);
        this.ws.send(messageStr);
        console.debug('Sent WS message:', message.type);
        this.isSending = false;

        // Process next message if any
        if (this.messageQueue.length > 0) {
            setTimeout(() => this.processQueue(), 10);
        }
    }


    handleMessage(event) {
        try {
            const message = JSON.parse(event.data);

            console.debug('Received WS message:', message.type, message.action || '');

            // Dispatch all messages to handlers
            this.dispatchMessage(message);

        } catch (error) {
            console.error('Error parsing WebSocket message:', error, event.data);
        }
    }

    dispatchMessage(message) {
        // Dispatch to any registered handlers
        const handlers = this.messageHandlers.get(message.type) || [];
        handlers.forEach(handler => {
            try {
                handler(message);
            } catch (error) {
                console.error('Error in message handler:', error);
            }
        });
    }

    onMessage(type, handler) {
        if (!this.messageHandlers.has(type)) {
            this.messageHandlers.set(type, []);
        }
        this.messageHandlers.get(type).push(handler);
    }

    offMessage(type, handler) {
        const handlers = this.messageHandlers.get(type) || [];
        const index = handlers.indexOf(handler);
        if (index > -1) {
            handlers.splice(index, 1);
        }
    }
}

// Global WebSocket API instance
window.wsAPI = new WebSocketAPI();

// Legacy API functions - now using WebSocket

function getDrawings(symbol) {
    try {
        window.wsAPI.sendMessage({
            type: 'config',
            action: 'get_drawings',
            data: { symbol }
        });
    } catch (error) {
        console.error(`Error sending get_drawings for ${symbol}:`, error);
    }
}

window.getDrawings = getDrawings; // Make getDrawings globally accessible

async function sendShapeUpdateToServer(shapeToUpdate, symbol) {
    if (!shapeToUpdate || !shapeToUpdate.id || !symbol) {
        console.warn("sendShapeUpdateToServer: Missing shape, id, or symbol.");
        return false;
    }

    const resolution = window.resolutionSelect.value;
    const start_time_ms = (shapeToUpdate.x0 instanceof Date) ? shapeToUpdate.x0.getTime() : new Date(shapeToUpdate.x0).getTime();
    const end_time_ms = (shapeToUpdate.x1 instanceof Date) ? shapeToUpdate.x1.getTime() : new Date(shapeToUpdate.x1).getTime();

    const drawingData = {
        drawing_id: shapeToUpdate.id,
        symbol: symbol,
        type: shapeToUpdate.type,
        start_time: Math.floor(start_time_ms / 1000),
        end_time: Math.floor(end_time_ms / 1000),
        start_price: parseFloat(shapeToUpdate.y0),
        end_price: parseFloat(shapeToUpdate.y1),
        subplot_name: determineSubplotNameForShape(shapeToUpdate), // Assumes determineSubplotNameForShape is global
        resolution: resolution,
        properties: shapeToUpdate.properties || {
            sendEmailOnCross: true,
            buyOnCross: false,
            sellOnCross: false
        }
    };

    try {
        // Send shape update via WebSocket using wsAPI
        if (window.wsAPI && window.wsAPI.connected) {
            const shapeMessage = {
                type: 'shape',
                action: drawingData.drawing_id ? 'update' : 'save',
                data: drawingData,
                request_id: Date.now().toString()
            };

            return new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for shape update response'));
                }, 10000); // 10 second timeout

                const messageHandler = (event) => {
                    try {
                        const message = JSON.parse(event.data);
                        if (message.type === 'shape_success' && message.request_id === shapeMessage.request_id) {
                            clearTimeout(timeout);
                            window.wsAPI.ws.removeEventListener('message', messageHandler);
                            resolve(true);
                        } else if (message.type === 'error' && message.request_id === shapeMessage.request_id) {
                            clearTimeout(timeout);
                            window.wsAPI.ws.removeEventListener('message', messageHandler);
                            reject(new Error(message.message || 'Failed to update shape'));
                        }
                    } catch (e) {
                        console.error('Error parsing WebSocket response:', e);
                    }
                };

                // Listen for messages directly on the WebSocket
                window.wsAPI.ws.addEventListener('message', messageHandler);
                window.wsAPI.sendMessage(shapeMessage);
            });
        } else {
            throw new Error('WebSocket not connected');
        }
    } catch (error) {
        console.error(`Error in sendShapeUpdateToServer for drawing ${shapeToUpdate.id}:`, error);
        alert(`Failed to update drawing on server: ${error.message}`);
        return false;
    }
}

window.sendShapeUpdateToServer = sendShapeUpdateToServer; // Export to global scope

function getPlotlyRefsFromSubplotName(subplotName) {
    const currentSymbol = window.symbolSelect.value; // Assumes symbolSelect is global
    const hasActiveIndicators = window.active_indicatorsState && window.active_indicatorsState.length > 0;

    if (!subplotName || subplotName === currentSymbol) {
        // This is for a drawing on the main chart.
        // Price chart is on yaxis1 if indicators, else on 'y' (which becomes layout.yaxis)
        // Corresponding x-axis is xaxis1 if indicators, else 'xaxis'
        const yRefToUse = hasActiveIndicators ? 'yaxis1' : 'y';
        const xRefToUse = hasActiveIndicators ? 'xaxis1' : 'xaxis';
        return { xref: xRefToUse, yref: yRefToUse };
    }

    // Handle the temporary name used during loading if subplot_name was just the symbol
    if (subplotName === `${currentSymbol}-main`) { // This was a temporary name
        const yRefToUse = hasActiveIndicators ? 'yaxis1' : 'y';
        const xRefToUse = hasActiveIndicators ? 'xaxis1' : 'xaxis';
        return { xref: xRefToUse, yref: yRefToUse };
    }

    const parts = subplotName.split('-');
    if (parts.length >= 2) {
        const indicatorId = parts.slice(1).join('-');
        const indicator = window.active_indicatorsState && window.active_indicatorsState.find(ind => ind.id === indicatorId);

        if (indicator && indicator.xAxisRef && indicator.yAxisRef) {
            return { xref: indicator.xAxisRef, yref: indicator.yAxisRef };
        } else {
             //console.warn(`[getPlotlyRefsFromSubplotName] Indicator '${indicatorId}' (from subplotName '${subplotName}') is not currently active or its refs are missing. Active state:`, JSON.parse(JSON.stringify(window.active_indicatorsState)));
             return null; // Explicitly return null if indicator not active for this drawing
        }
    }
    // Fallback if subplotName doesn't match an indicator or if parts.length < 2
    // This should only be hit if subplotName is malformed or for a context not anticipated.
    // For the main chart without indicators, actualMainChartYAxisRef would be 'y', handled by the first 'if'.
    console.warn(`[getPlotlyRefsFromSubplotName] Fallback for subplotName '${subplotName}'. This might indicate an issue. Defaulting to x/y.`);
    return { xref: 'x', yref: 'y' };
}

function getOrderHistory(symbol) {
    try {
        window.wsAPI.sendMessage({
            type: 'config',
            action: 'get_order_history',
            data: { symbol, email: window.userEmail || '' }
        });
    } catch (error) {
        console.error(`Error sending get_order_history for ${symbol}:`, error);
    }
}

// Additional WebSocket-based API functions

function getAgentTrades() {
    try {
        window.wsAPI.sendMessage({
            type: 'config',
            action: 'get_agent_trades',
            data: { email: window.userEmail || '' }
        });
    } catch (error) {
        console.error('Error sending get_agent_trades:', error);
    }
}

function getBuySignals(symbol) {
    try {
        window.wsAPI.sendMessage({
            type: 'config',
            action: 'get_buy_signals',
            data: { symbol, email: window.userEmail || '' }
        });
    } catch (error) {
        console.error(`Error sending get_buy_signals for ${symbol}:`, error);
    }
}

function getSettings() {
    try {
        window.wsAPI.sendMessage({
            type: 'config',
            action: 'get_settings',
            data: { email: window.userEmail || '' }
        });
    } catch (error) {
        console.error('Error sending get_settings:', error);
    }
}

function getLivePrice() {
    try {
        window.wsAPI.sendMessage({
            type: 'config',
            action: 'get_live_price',
            data: { email: window.userEmail || '' }
        });
    } catch (error) {
        console.error('Error sending get_live_price:', error);
    }
}

function getAvailableIndicators() {
    try {
        window.wsAPI.sendMessage({
            type: 'config',
            action: 'get_available_indicators',
            data: { email: window.userEmail || '' }
        });
    } catch (error) {
        console.error('Error sending get_available_indicators:', error);
    }
}

function getAIResponse(aiRequestData) {
    try {
        window.wsAPI.sendMessage({
            type: 'config',
            action: 'ai_suggestion',
            data: { ...aiRequestData, email: window.userEmail || '' }
        });
    } catch (error) {
        console.error('Error sending ai_suggestion:', error);
    }
}

// Export new WebSocket-based functions
window.getAgentTrades = getAgentTrades;
window.getBuySignals = getBuySignals;
window.getSettings = getSettings;
window.getLivePrice = getLivePrice;
window.getAvailableIndicators = getAvailableIndicators;
window.getAIResponse = getAIResponse;
