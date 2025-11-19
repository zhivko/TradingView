const REALTIME_PRICE_LINE_NAME = 'realtimePriceLine';
const CROSSHAIR_VLINE_NAME = 'crosshair-vline';
const DEFAULT_DRAWING_COLOR = 'blue';
const HOVER_DRAWING_COLOR = 'red';
const SELECTED_DRAWING_COLOR = 'green';
const MAX_LIVE_CANDLES = 1000; // Maximum number of candles to keep in memory for live chart
const VISUAL_UPDATE_DEBOUNCE_DELAY = 30;
const FETCH_DEBOUNCE_DELAY = 2500;
const INDICATOR_OB_LINE_NAME = 'indicator_ob_line';
const INDICATOR_OS_LINE_NAME = 'indicator_os_line';
const BUY_EVENT_MARKER_COLOR = 'green';
const SELL_EVENT_MARKER_COLOR = 'red';
const BUY_EVENT_MARKER_SYMBOL = 'triangle-up';
const SELL_EVENT_MARKER_SYMBOL = 'triangle-down';
const REALTIME_PRICE_TEXT_ANNOTATION_NAME = 'realtimePriceTextAnnotation';

// Function to detect mobile devices using modern browser APIs
function isMobileDevice() {
    // Log client resolution information

    // Use the modern User-Agent Client Hints API if available
    if (navigator.userAgentData && typeof navigator.userAgentData.mobile === 'boolean') {
        return navigator.userAgentData.mobile;
    }

    // Fallback: Check for coarse pointer (typical of touch devices)
    if (window.matchMedia && window.matchMedia('(pointer: coarse)').matches) {
        return true;
    }

    // Final fallback: Check user agent for mobile devices (legacy method)
    const userAgentMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);

    const isMobile = userAgentMobile;
    return isMobile;
}


const config = {
    responsive: true,
    displayModeBar: true, // Enable Plotly mode bar for drawing functionality
    modeBarButtonsToRemove: ['sendDataToCloud', 'editInChartStudio', 'zoom2d', 'pan2d', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d', 'hoverClosestCartesian', 'hoverCompareCartesian', 'toggleSpikelines', 'toImage'], // Remove unwanted buttons but keep drawing buttons
    modeBarButtonsToAdd: ['drawline', 'drawrect'], // Add drawing buttons
    scrollZoom: true, // Keep scroll zoom enabled for mouse wheel zooming
    displaylogo: false, // Hide Plotly logo to save space
    showTips: false, // Disable tips that might interfere on mobile
    editable: true,
    autosize: true, // Keep autosize for responsive behavior, but grid rowheights should take precedence
    dragmode: 'pan', // Set default dragmode to pan for chart navigation
    edits: {
        shapePosition: true,
        annotationPosition: false,
        annotationText: false,
        axisTitleText: false,
        axisTitleFont: false,
        titleText: false
    },
    // Enable pinch-to-zoom and other mobile gestures
    staticPlot: false,
    doubleClick: false // Disable default double-click reset to prevent 1970 issue
};

// Function to disable hover on mobile devices
function disableMobileHover(gd) {
    if (isMobileDevice() && gd) {
        Plotly.relayout(gd, {
            hovermode: false,
            hoverdistance: 0
        });
    }
}

// Function to enable mobile pinch zoom features
function enableMobilePinchZoom(gd) {
    if (!isMobileDevice() || !gd) {
        return; // Only enable on mobile devices, and return early if no chart
    }

    // Ensure the chart responds to touch gestures for pinch-to-zoom without interfering with desktop
    Plotly.relayout(gd, {
        scrollZoom: true,
        responsive: true,
        dragmode: 'zoom', // Use zoom mode instead of pan for better mobile pinch behavior
        doubleClick: false // Disable default double-click reset to prevent 1970 issue
    });

    // Add touch event handling specifically for pinch gestures only
    const chartDiv = document.getElementById('chart');
    if (!chartDiv) return;

    let pinchInProgress = false;

    chartDiv.addEventListener('touchstart', function(event) {
        if (event.touches.length === 2) {
            // Pinch gesture started
            pinchInProgress = true;
            // Let Plotly handle pinch zoom naturally - don't change drag mode
        }
    }, { passive: true });

    chartDiv.addEventListener('touchmove', function(event) {
        // Only prevent default on actual pinch gestures (2 touches)
        if (event.touches.length === 2 && pinchInProgress) {
            // Prevent default browser pinch handling to let Plotly handle it
            event.preventDefault();
        }
        // Single touch gestures and other interactions should work normally
    }, { passive: false });

    chartDiv.addEventListener('touchend', function(event) {
        // Reset pinch state when touches end
        if (pinchInProgress && event.touches.length === 0) {
            pinchInProgress = false;
            // Re-enable mouse wheel zoom after pinch gestures end
            if (window.ensureScrollZoomEnabled) {
                window.ensureScrollZoomEnabled();
            }
        }
    }, { passive: true });
}

// Function to force hide hover elements via CSS
function forceHideHoverElements() {
    if (isMobileDevice()) {
        // Create a style element to hide hover elements
        const style = document.createElement('style');
        style.id = 'mobile-hover-blocker';
        style.textContent = `
            .js-plotly-plot .hoverlayer,
            .js-plotly-plot .hovertext,
            .js-plotly-plot g.hoverlayer,
            .js-plotly-plot .hoverlayer text,
            .js-plotly-plot [class*="hover"] {
                display: none !important;
                visibility: hidden !important;
                opacity: 0 !important;
            }
        `;
        document.head.appendChild(style);
    }
}


// Make them available globally for other scripts, or consider a module system later
window.REALTIME_PRICE_LINE_NAME = REALTIME_PRICE_LINE_NAME;
window.DEFAULT_DRAWING_COLOR = DEFAULT_DRAWING_COLOR;
window.HOVER_DRAWING_COLOR = HOVER_DRAWING_COLOR;
window.SELECTED_DRAWING_COLOR = SELECTED_DRAWING_COLOR;
window.REALTIME_PRICE_TEXT_ANNOTATION_NAME = REALTIME_PRICE_TEXT_ANNOTATION_NAME;
window.BUY_EVENT_MARKER_COLOR = BUY_EVENT_MARKER_COLOR;
window.SELL_EVENT_MARKER_COLOR = SELL_EVENT_MARKER_COLOR;
window.BUY_EVENT_MARKER_SYMBOL = BUY_EVENT_MARKER_SYMBOL;
window.SELL_EVENT_MARKER_SYMBOL = SELL_EVENT_MARKER_SYMBOL;
window.isMobileDevice = isMobileDevice;
window.disableMobileHover = disableMobileHover;
window.enableMobilePinchZoom = enableMobilePinchZoom;
window.forceHideHoverElements = forceHideHoverElements;

// Make config globally available
window.config = config;
