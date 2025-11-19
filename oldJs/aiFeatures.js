async function fetchAndPopulateLocalOllamaModels() {
    try {
        const response = await fetch('/AI_Local_OLLAMA_Models');
        if (!response.ok) throw new Error(`Failed to fetch local models: ${response.status}`);
        const data = await response.json();
        window.localOllamaModelSelect.innerHTML = ''; // Assumes localOllamaModelSelect is global
        if (data.models && data.models.length > 0) {
            data.models.forEach(modelName => {
                const option = document.createElement('option');
                option.value = modelName;
                option.textContent = modelName;
                window.localOllamaModelSelect.appendChild(option);
            });
        } else {
            window.localOllamaModelSelect.innerHTML = '<option value="">No models found</option>';
        }
    } catch (error) {
        console.error("Error fetching local Ollama models:", error);
        window.localOllamaModelSelect.innerHTML = '<option value="">Error loading models</option>';
    }
}

function initializeAIFeatures() {
    window.aiSuggestionButton.addEventListener('click', async () => { // Assumes aiSuggestionButton is global
        if (!window.gd || !window.gd.layout || !window.gd.layout.xaxis) {
            alert("Chart not loaded yet or x-axis not defined.");
            window.aiSuggestionTextarea.value = "Error: Chart not ready."; // Assumes aiSuggestionTextarea is global
            return;
        }
        if (window.aiSuggestionButton.textContent.startsWith("STOP")) {
            if (aiSuggestionAbortController) { // Assumes aiSuggestionAbortController is global from state.js
                aiSuggestionAbortController.abort();
            } else {
                window.aiSuggestionButton.textContent = "Get AI Suggestion";
                window.aiSuggestionTextarea.value += "\n\n--- AI suggestion stop requested (no active process) ---";
            }
            return;
        }

        window.aiSuggestionTextarea.value = "Getting AI suggestion, please wait...";
        window.aiSuggestionButton.textContent = "STOP - Get AI Suggestion";

        try {
            const currentSymbol = window.symbolSelect.value;
            const currentResolution = window.resolutionSelect.value;
            const plotlyXRange = window.gd.layout.xaxis.range;
            let xAxisMin, xAxisMax;

            if (plotlyXRange && plotlyXRange.length === 2) {
                const convertToTimestamp = (value) => {
                    if (value instanceof Date) {
                        if (isNaN(value.getTime())) return null;
                        return Math.floor(value.getTime() / 1000);
                    } else if (typeof value === 'string') {
                        const parsedDate = new Date(value);
                        if (isNaN(parsedDate.getTime())) return null;
                        return Math.floor(parsedDate.getTime() / 1000);
                    } else if (typeof value === 'number') {
                        if (isNaN(value)) return null;
                        return Math.floor(value / 1000);
                    }
                    return null;
                };
                xAxisMin = convertToTimestamp(plotlyXRange[0]);
                xAxisMax = convertToTimestamp(plotlyXRange[1]);
                if (xAxisMin === null || xAxisMax === null || isNaN(xAxisMin) || isNaN(xAxisMax)) {
                    throw new Error("Could not parse valid time range from chart's x-axis.");
                }
            } else {
                throw new Error("Could not determine time range from chart's x-axis.");
            }

            const activeIndicatorCheckboxes = document.querySelectorAll('#indicator-checkbox-list input[type="checkbox"]:checked');
            const activeIndicatorIds = Array.from(activeIndicatorCheckboxes).map(cb => cb.value);

            const requestPayload = {
                symbol: currentSymbol, resolution: currentResolution, xAxisMin: xAxisMin, xAxisMax: xAxisMax,
                activeIndicatorIds: activeIndicatorIds, question: "Based on the provided market data, what is your trading suggestion (BUY, SELL, or HOLD) and why?",
                use_local_ollama: window.useLocalOllamaCheckbox.checked, // Assumes useLocalOllamaCheckbox is global
                local_ollama_model_name: window.useLocalOllamaCheckbox.checked ? window.localOllamaModelSelect.value : null
            };

            aiSuggestionAbortController = new AbortController();
            const response = await fetch('/AI', {
                method: 'POST', headers: { 'Content-Type': 'application/json'},
                body: JSON.stringify(requestPayload), signal: aiSuggestionAbortController.signal
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`AI suggestion failed: ${response.status} ${errorText}`);
            }

            if (window.useLocalOllamaCheckbox.checked && response.body) {
                window.aiSuggestionTextarea.value = "";
                const reader = response.body.getReader();
                const decoder = new TextDecoder();
                let buffer = '';

                function processLine(line) {
                    if (line.trim()) {
                        try {
                            const data = JSON.parse(line);
                            if (data.response) {
                                window.aiSuggestionTextarea.value += data.response;
                                window.aiSuggestionTextarea.scrollTop = window.aiSuggestionTextarea.scrollHeight; // Auto-scroll
                            }
                            // The 'done' field in individual stream objects indicates completion of that part.
                            // The overall stream ends when reader.read() returns done: true.
                        } catch (e) {
                            console.warn("Error parsing JSON line from stream:", e, "Line:", line);
                            // Optionally append raw line or an error message to the textarea
                            // window.aiSuggestionTextarea.value += `\n[Error processing line: ${line}]`;
                        }
                    }
                }

                while (true) {
                    if (aiSuggestionAbortController && aiSuggestionAbortController.signal.aborted) {
                        break;
                    }
                    const { value, done } = await reader.read();
                    if (done) {
                        processLine(buffer); // Process any remaining data in the buffer
                        break;
                    }
                    buffer += decoder.decode(value, { stream: true });
                    let newlineIndex;
                    while ((newlineIndex = buffer.indexOf('\n')) >= 0) {
                        const line = buffer.substring(0, newlineIndex);
                        buffer = buffer.substring(newlineIndex + 1);
                        processLine(line);
                    }
                }
            } else {
                const result = await response.json();
                window.aiSuggestionTextarea.value = JSON.stringify(result, null, 2);
            }
        } catch (error) {
            if (error.name === 'AbortError') {
                window.aiSuggestionTextarea.value += "\n\n--- AI suggestion stopped by user ---";
            } else {
                window.aiSuggestionTextarea.value = `Error: ${error.message}`;
            }
        } finally {
            window.aiSuggestionButton.textContent = "Get AI Suggestion";
            aiSuggestionAbortController = null;
        }
    });

    window.useLocalOllamaCheckbox.addEventListener('change', function() {
        window.localOllamaModelDiv.style.display = this.checked ? 'block' : 'none'; // Assumes localOllamaModelDiv is global
        if (this.checked) fetchAndPopulateLocalOllamaModels();
        saveSettingsInner(); // saveSettings might be called from main.js event listener
    });
}
