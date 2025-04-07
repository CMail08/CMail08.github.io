// script.js
// Handles simplified LLM interaction with loading spinner feedback.
// Adds Prism.js syntax highlighting for SQL display.
// Adds Vertical resizing for Explanation/SQL and Table sections.
// REMOVED: Vertical resizing for Input section.

// --- DOM Elements ---
const nlQueryTextarea = document.getElementById('nl-query');
const submitButton = document.getElementById('submit-button');
const statusMessageDiv = document.getElementById('status-message');
const resultsTableContainer = document.getElementById('results-table-container');
const resultsAnalysisDiv = document.getElementById('results-analysis');
const loadingSpinner = document.getElementById('loading-spinner');
const generatedSqlContainer = document.getElementById('generated-sql-container');
const generatedSqlDisplay = document.getElementById('generated-sql-display');
// References for resizable sections and their handles
// REMOVED inputAreaContent and resizeHandleInput
// const inputAreaContent = document.getElementById('input-area-content');
// const resizeHandleInput = document.getElementById('resize-handle-input');
// REMOVED explanationSqlGrid and resizeHandleES
// const explanationSqlGrid = document.getElementById('explanation-sql-grid');
// const resizeHandleES = document.getElementById('resize-handle-es');
// resultsTableContainer already defined above
// REMOVED resizeHandleTable
// const resizeHandleTable = document.getElementById('resize-handle-table');
// Details elements for opening/closing
const detailsExplanationSql = document.getElementById('details-explanation-sql');
const detailsResultsTable = document.getElementById('details-results-table');


// --- State Variables ---
let currentSql = "";
let currentAppState = "idle";

// --- Functions ---

/** Clears output areas and resets state */
function clearOutput(clearInput = false) {
    statusMessageDiv.textContent = ''; statusMessageDiv.className = 'mt-4 p-2 text-sm rounded';
    loadingSpinner.classList.add('hidden');
    const tableBody = resultsTableContainer?.querySelector('tbody');
    if (tableBody) tableBody.innerHTML = ''; else if(resultsTableContainer) resultsTableContainer.innerHTML = '';
    if(resultsAnalysisDiv) resultsAnalysisDiv.innerHTML = '';
    if (generatedSqlDisplay) generatedSqlDisplay.textContent = '';
    if (generatedSqlContainer) generatedSqlContainer.style.display = 'none';

    // Reset resizable heights
    // if (inputAreaContent) inputAreaContent.style.height = '120px'; // REMOVED
    // REMOVED reset for explanationSqlGrid height
    // if (explanationSqlGrid) explanationSqlGrid.style.height = '200px';
    // REMOVED reset for resultsTableContainer height
    // if (resultsTableContainer) resultsTableContainer.style.height = '300px';

    // Ensure details sections are closed
    if (detailsExplanationSql) detailsExplanationSql.open = false;
    if (detailsResultsTable) detailsResultsTable.open = false;

    currentSql = ""; setAppState("idle");
    if (clearInput) nlQueryTextarea.value = '';
}

/** Displays status messages and handles spinner */
function showStatus(message, isError = false, isLoading = false) {
    // (Function unchanged)
    statusMessageDiv.textContent = message; let classes = 'mt-4 p-2 text-sm rounded ';
    if (isError) classes += 'bg-red-100 text-red-700'; else if (isLoading) classes += 'bg-blue-100 text-blue-700'; else classes += 'bg-green-100 text-green-700';
    statusMessageDiv.className = classes; if (isLoading) loadingSpinner.classList.remove('hidden'); else loadingSpinner.classList.add('hidden');
}

/** Renders the data table */
function renderTable(columns, data) {
    // (Function unchanged)
    const table = document.createElement('table'); table.className = 'min-w-full divide-y divide-gray-200'; const thead = document.createElement('thead'); thead.className = 'bg-gray-50'; const headerRow = document.createElement('tr'); if (Array.isArray(columns)) { columns.forEach(colName => { const th = document.createElement('th'); th.scope = 'col'; th.className = 'px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider'; th.textContent = colName; headerRow.appendChild(th); }); } thead.appendChild(headerRow); table.appendChild(thead); const tbody = document.createElement('tbody'); tbody.className = 'bg-white divide-y divide-gray-200'; if (!data || data.length === 0) { const row = document.createElement('tr'); const cell = document.createElement('td'); cell.colSpan = columns?.length || 1; cell.textContent = 'Query returned no results.'; cell.className = 'px-6 py-4 text-sm text-gray-500 text-center'; row.appendChild(cell); tbody.appendChild(row); } else { data.forEach(rowData => { const row = document.createElement('tr'); if(Array.isArray(rowData)){ rowData.forEach(cellData => { const td = document.createElement('td'); td.className = 'px-6 py-4 whitespace-nowrap text-sm text-gray-900'; td.textContent = cellData === null ? 'NULL' : String(cellData); row.appendChild(td); }); } tbody.appendChild(row); }); } table.appendChild(tbody); resultsTableContainer.innerHTML = ''; resultsTableContainer.appendChild(table);
}

/** Sets the application state and updates button */
function setAppState(newState) {
    // (Function unchanged)
    currentAppState = newState; submitButton.disabled = false;
    switch (newState) {
        case "idle": submitButton.textContent = 'Ask AI Assistant'; submitButton.classList.replace('bg-green-500', 'bg-blue-500'); submitButton.classList.replace('hover:bg-green-700', 'hover:bg-blue-700'); submitButton.style.display = 'block'; loadingSpinner.classList.add('hidden'); break;
        case "processing_nl": submitButton.textContent = 'AI is Thinking...'; submitButton.disabled = true; submitButton.style.display = 'none'; showStatus('Asking AI Assistant for SQL and Explanation...', false, true); break;
        case "sql_ready": submitButton.textContent = 'Run Generated SQL'; submitButton.classList.replace('bg-blue-500', 'bg-green-500'); submitButton.classList.replace('hover:bg-blue-700', 'hover:bg-green-700'); submitButton.style.display = 'block'; loadingSpinner.classList.add('hidden'); break;
        case "executing_sql": submitButton.textContent = 'Executing SQL...'; submitButton.disabled = true; submitButton.style.display = 'none'; showStatus('Executing SQL query...', false, true); break;
        default: submitButton.textContent = 'Ask AI Assistant'; submitButton.classList.replace('bg-green-500', 'bg-blue-500'); submitButton.classList.replace('hover:bg-green-700', 'hover:bg-blue-700'); submitButton.style.display = 'block'; loadingSpinner.classList.add('hidden');
    }
}

/** Main handler for the submit button click */
async function handleSubmitClick() {
    // (Function unchanged)
    if (currentAppState === "idle") {
        const nlQuery = nlQueryTextarea.value.trim(); if (!nlQuery) { showStatus('Please enter a question.', true); return; }
        clearOutput(); setAppState("processing_nl");
        try {
            const response = await fetch('/process_nl_query', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ query: nlQuery }), });
            const result = await response.json(); if (!response.ok || result.error) throw new Error(result.error || `HTTP error ${response.status}`);
            currentSql = result.sql || "";
            const explanation = result.explanation || "<ul><li>(No explanation provided.)</li></ul>"; // Ensure fallback is also valid HTML
            // Use innerHTML to render the bulleted list
            if(resultsAnalysisDiv) resultsAnalysisDiv.innerHTML = explanation;
            // Reset SQL display (textContent is fine here)
            if (generatedSqlDisplay) generatedSqlDisplay.textContent = '';
            if (generatedSqlContainer) generatedSqlContainer.style.display = 'none';

            if (currentSql && generatedSqlDisplay && generatedSqlContainer) {
                generatedSqlDisplay.textContent = currentSql; // Still use textContent for SQL to prevent XSS from SQL
                generatedSqlContainer.style.display = 'block';
                // Re-highlight after setting content
                setTimeout(() => { if (window.Prism && Prism.languages.sql) { try { Prism.highlightElement(generatedSqlDisplay); console.info("Prism applied."); } catch (e) { console.error("Prism failed:", e); } } else { console.warn('Prism/SQL lang not loaded.'); } }, 0);
                if (detailsExplanationSql) detailsExplanationSql.open = true;
                setAppState("sql_ready"); showStatus('AI generated SQL. Click Run to execute.', false, false);
            } else { if (explanation && detailsExplanationSql) detailsExplanationSql.open = true; showStatus(currentSql ? 'Failed display SQL.' : 'Failed generate SQL.', true); setAppState("idle"); }
        } catch (error) { console.error('NL Error:', error); showStatus(`Failed: ${error.message}`, true); setAppState("idle"); }
        finally { if (currentAppState !== 'sql_ready') { setAppState("idle"); } }
    } else if (currentAppState === "sql_ready") {
        if (!currentSql) { showStatus('No SQL query.', true); return; }
        setAppState("executing_sql");
        try {
            const execResponse = await fetch('/query', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ sql: currentSql }), });
            const execResult = await execResponse.json(); if (!execResponse.ok || execResult.error) throw new Error(`SQL Failed: ${execResult.error || `HTTP error ${execResponse.status}`}\nSQL:\n${currentSql}`);
            showStatus(execResult.message || 'Success.', false); renderTable(execResult.columns, execResult.data);
            if (detailsResultsTable) detailsResultsTable.open = true;
            setAppState("idle");
        } catch (error) { console.error('SQL Exec Error:', error); showStatus(`Error: ${error.message}`, true); setAppState("idle"); }
        finally { setAppState("idle"); }
    }
}

// --- REUSABLE Vertical Resizing Logic ---
function makeResizable(handleId, targetId, minHeight = 50) {
    const handleElement = document.getElementById(handleId);
    const targetElement = document.getElementById(targetId);

    if (!handleElement || !targetElement) {
        // console.error(`Resize setup failed: Cannot find handle '${handleId}' or target '${targetId}'.`);
        return; // Exit silently if elements aren't found
    }
    // console.info(`Initializing resize for target: #${targetId} using handle: #${handleId}`);

    let isResizing = false;
    let startY, startHeight;

    const handleDrag = (event) => {
        if (!isResizing) return;
        const deltaY = event.clientY - startY;
        const newHeight = Math.max(minHeight, startHeight + deltaY);
        targetElement.style.height = `${newHeight}px`;
        // console.log(`Dragging ${targetId}: deltaY=${deltaY}, newHeight=${newHeight}px`);
    };

    const stopDrag = () => {
        if (!isResizing) return;
        isResizing = false;
        document.removeEventListener('mousemove', handleDrag);
        document.removeEventListener('mouseup', stopDrag);
        document.body.style.userSelect = '';
        document.body.style.cursor = '';
        // console.log(`Stopped resizing ${targetId}`);
    };

    handleElement.addEventListener('mousedown', (event) => {
        event.preventDefault();
        isResizing = true;
        startY = event.clientY;
        startHeight = targetElement.offsetHeight;
        document.addEventListener('mousemove', handleDrag);
        document.addEventListener('mouseup', stopDrag);
        document.body.style.userSelect = 'none';
        document.body.style.cursor = 'ns-resize';
        // console.log(`Started resizing ${targetId}: startY=${startY}, startHeight=${startHeight}`);
    });
}

// --- Event Listeners ---
submitButton.addEventListener('click', handleSubmitClick);

// Setup resizing for relevant sections after DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    // REMOVED call for input area handle
    // makeResizable('resize-handle-input', 'input-area-content', 80);
    // REMOVED call for explanation/SQL handle
    // makeResizable('resize-handle-es', 'explanation-sql-grid', 100);
    // REMOVED call for table handle
    // makeResizable('resize-handle-table', 'results-table-container', 150);
    console.info("Resizable sections initialized."); // Log successful init
});

// Initial clear
clearOutput();

// --- Auto-Resize Textarea Logic ---
function autoResizeTextarea(textarea) {
    if (!textarea) return;
    // Reset height to calculate scrollHeight accurately
    textarea.style.height = 'auto';
    // Set height based on content scroll height, respecting min-height from CSS
    textarea.style.height = `${textarea.scrollHeight}px`;
}

// Add event listener for input (typing, pasting)
if (nlQueryTextarea) {
    nlQueryTextarea.addEventListener('input', () => autoResizeTextarea(nlQueryTextarea));
    // Initial call in case textarea loads with content (e.g., back button)
    autoResizeTextarea(nlQueryTextarea);
}

