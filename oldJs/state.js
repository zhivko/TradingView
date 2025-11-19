let liveWebSocket = null;
let currentSymbolForStream = '';
let currentResolution = '1h'; // Current chart resolution
let currentXAxisRange = null;
let currentYAxisRange = null;
let active_indicatorsState = [];
let fetchDataDebounceTimer = null;
let activeShapeForPotentialDeletion = null;
let hoveredShapeBackendId = null;
let aiSuggestionAbortController = null;
let currentStreamDeltaTime = 0; // Added for live stream update interval
let isDraggingShape = false;

// Shape selection state management
let selectedShapeIds = new Set(); // Set of selected shape backend IDs
let lastSelectedShapeId = null; // Last shape that was selected (for keyboard operations)

// Make critical variables global for chart interactions
window.hoveredShapeBackendId = null;
window.activeShapeForPotentialDeletion = null;
window.isDraggingShape = false;
window.selectedShapeIds = selectedShapeIds;
window.lastSelectedShapeId = lastSelectedShapeId;

// Make current chart state variables globally available
window.currentSymbolForStream = currentSymbolForStream;
window.currentResolution = currentResolution;

window.currentDataStart = null; // Will be set in updateChart
window.currentDataEnd = null;   // Will be set in updateChart

// Shape selection management functions
function selectShape(shapeId, multiSelect = false) {
    if (!multiSelect) {
        // Single selection - clear previous selections
        selectedShapeIds.clear();
    }

    if (shapeId) {
        selectedShapeIds.add(shapeId);
        lastSelectedShapeId = shapeId;

        // Update activeShapeForPotentialDeletion for backward compatibility
        const currentShapes = window.gd?.layout?.shapes || [];
        const shape = currentShapes.find(s => s.id === shapeId);
        if (shape) {
            const shapeIndex = currentShapes.indexOf(shape);
            window.activeShapeForPotentialDeletion = {
                id: shapeId,
                index: shapeIndex,
                shape: shape
            };
        }
    }


}

function deselectShape(shapeId) {
    selectedShapeIds.delete(shapeId);
    if (lastSelectedShapeId === shapeId) {
        lastSelectedShapeId = selectedShapeIds.size > 0 ? Array.from(selectedShapeIds)[selectedShapeIds.size - 1] : null;
    }

    // Update activeShapeForPotentialDeletion
    if (selectedShapeIds.size === 0) {
        window.activeShapeForPotentialDeletion = null;
    } else if (lastSelectedShapeId) {
        const currentShapes = window.gd?.layout?.shapes || [];
        const shape = currentShapes.find(s => s.id === lastSelectedShapeId);
        if (shape) {
            const shapeIndex = currentShapes.indexOf(shape);
            window.activeShapeForPotentialDeletion = {
                id: lastSelectedShapeId,
                index: shapeIndex,
                shape: shape
            };
        }
    }
}

function deselectAllShapes() {
    selectedShapeIds.clear();
    lastSelectedShapeId = null;
    window.activeShapeForPotentialDeletion = null;
}

function isShapeSelected(shapeId) {
    return selectedShapeIds.has(shapeId);
}

function getSelectedShapeCount() {
    return selectedShapeIds.size;
}

function getSelectedShapeIds() {
    return Array.from(selectedShapeIds);
}

// Make functions globally available
window.selectShape = selectShape;
window.deselectShape = deselectShape;
window.deselectAllShapes = deselectAllShapes;
window.isShapeSelected = isShapeSelected;
window.getSelectedShapeCount = getSelectedShapeCount;
window.getSelectedShapeIds = getSelectedShapeIds;

// Shape selection help function
function showShapeSelectionHelp() {
    const helpText = `
🎯 Shape Selection Controls:

Mouse Controls:
• Click a shape to select it
• Ctrl+Click (Cmd+Click on Mac) to multi-select shapes
• Click on empty space to deselect all shapes

Keyboard Shortcuts:
• Delete/Backspace - Delete selected shape(s)
• Escape - Deselect all shapes
• Ctrl+A (Cmd+A) - Select all shapes
• Arrow Keys - Navigate between selected shapes (when multiple selected)

Visual Feedback:
• Green lines = Selected shapes
• Bright green = Last selected shape (in multi-selection)
• Red lines = Hovered shapes
• Blue lines = Default shapes

Selected shapes can be edited and deleted as a group.
    `.trim();

    alert(helpText);
}

// Make help function globally available
window.showShapeSelectionHelp = showShapeSelectionHelp;