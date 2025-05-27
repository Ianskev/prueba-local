// SQL Query module
const SQLQuery = {
    editor: null,

    initEditor() {
        this.editor = CodeMirror.fromTextArea(document.getElementById('sql-editor'), {
            mode: 'text/x-sql',
            theme: 'default',
            lineNumbers: true,
            autofocus: true,
            indentWithTabs: true,
            tabSize: 2
        });

        this.editor.setValue('SELECT * FROM your_table LIMIT 10;');
    },

    async getDashboardData() {
        try {
            // Check if we have a token before making the request
            if (!Auth.token) {
                return { success: false, message: 'Authentication token not found. Please log in again.' };
            }

            const response = await fetch(`${API_URL}/sql/dashboard`, {
                method: 'GET',
                headers: Auth.getAuthHeaders()
            });
            
            const data = await response.json();
            
            if (response.ok) {
                return { success: true, data };
            } else {
                // Handle 401 errors specifically
                if (response.status === 401) {
                    Auth.logout(); // Clear invalid credentials
                    return { success: false, unauthorized: true, message: 'Session expired. Please log in again.' };
                }
                return { success: false, message: data.detail || 'Failed to fetch dashboard data' };
            }
        } catch (error) {
            console.error("Dashboard data error:", error);
            return { success: false, message: 'Network error. Please try again.' };
        }
    },

    async executeQuery(query, limit = 50, offset = 0) {
        try {
            const response = await fetch(`${API_URL}/sql/`, {
                method: 'POST',
                headers: Auth.getAuthHeaders(),
                body: JSON.stringify({ query, limit, offset })
            });
            
            const data = await response.json();
            
            return {
                success: response.ok,
                data: data.data,
                total: data.total,
                message: data.message,
                execution_time: data.execution_time
            };
        } catch (error) {
            return { success: false, message: 'Network error. Please try again.' };
        }
    },

    renderQueryResults(result) {
        const table = document.getElementById('query-results-table');
        const messageDiv = document.getElementById('query-message');
        const infoDiv = document.getElementById('query-info');
        
        table.innerHTML = '';
        messageDiv.innerHTML = '';
        
        if (result.execution_time) {
            infoDiv.textContent = `Execution time: ${result.execution_time.toFixed(4)}s | Total rows: ${result.total}`;
        } else {
            infoDiv.textContent = '';
        }
        
        if (result.message) {
            const messageClass = result.success ? 'success-message' : 'error-message';
            messageDiv.innerHTML = `<div class="${messageClass}">${result.message}</div>`;
        }
        
        if (result.success && result.data) {
            // Create header
            if (result.data.columns && result.data.columns.length > 0) {
                const thead = document.createElement('thead');
                const headerRow = document.createElement('tr');
                
                result.data.columns.forEach(column => {
                    const th = document.createElement('th');
                    th.textContent = column;
                    headerRow.appendChild(th);
                });
                
                thead.appendChild(headerRow);
                table.appendChild(thead);
                
                // Create body
                const tbody = document.createElement('tbody');
                
                if (result.data.records && result.data.records.length > 0) {
                    result.data.records.forEach(record => {
                        const tr = document.createElement('tr');
                        record.forEach(cell => {
                            const td = document.createElement('td');
                            td.textContent = cell === null ? 'NULL' : cell;
                            tr.appendChild(td);
                        });
                        tbody.appendChild(tr);
                    });
                } else {
                    const tr = document.createElement('tr');
                    const td = document.createElement('td');
                    td.colSpan = result.data.columns.length;
                    td.className = 'text-center';
                    td.textContent = 'No records found';
                    tr.appendChild(td);
                    tbody.appendChild(tr);
                }
                
                table.appendChild(tbody);
            } else {
                table.innerHTML = '<tr><td class="text-center">No data returned</td></tr>';
            }
        }
    },

    renderTables(tables) {
        const tablesList = document.getElementById('tables-list');
        tablesList.innerHTML = '';
        
        if (tables.length === 0) {
            tablesList.innerHTML = '<div class="text-muted text-center py-3">No tables found</div>';
            return;
        }
        
        tables.forEach(table => {
            const item = document.createElement('a');
            item.className = 'list-group-item list-group-item-action';
            item.textContent = table[0]; // The table name is in the first column of each record
            
            // Add an event to set an example query when clicking on a table
            item.addEventListener('click', () => {
                this.editor.setValue(`SELECT * FROM ${table[0]} LIMIT 10;`);
                document.getElementById('query-link').click(); // Switch to query tab
            });
            
            tablesList.appendChild(item);
        });
    }
};
