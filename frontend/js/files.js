// Files management module
const Files = {
    async listFiles() {
        try {
            const response = await fetch(`${API_URL}/files/list`, {
                method: 'GET',
                headers: Auth.getAuthHeaders()
            });
            
            const data = await response.json();
            
            if (response.ok) {
                return { success: true, files: data.files };
            } else {
                return { success: false, message: data.detail || 'Failed to fetch files' };
            }
        } catch (error) {
            return { success: false, message: 'Network error. Please try again.' };
        }
    },

    async uploadFile(file) {
        try {
            const formData = new FormData();
            formData.append('file', file);
            
            const response = await fetch(`${API_URL}/files/upload`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${Auth.token}`
                    // Don't set Content-Type here, let the browser set it with the boundary parameter
                },
                body: formData
            });
            
            const data = await response.json();
            
            if (response.ok) {
                return { success: true, file: data.file };
            } else {
                return { success: false, message: data.detail || 'Failed to upload file' };
            }
        } catch (error) {
            return { success: false, message: 'Network error. Please try again.' };
        }
    },

    async getFileContent(fileId) {
        try {
            const response = await fetch(`${API_URL}/files/content/${fileId}`, {
                method: 'GET',
                headers: Auth.getAuthHeaders()
            });
            
            const data = await response.json();
            
            if (response.ok) {
                return { 
                    success: true, 
                    filename: data.filename,
                    columns: data.columns, 
                    rows: data.rows 
                };
            } else {
                return { success: false, message: data.detail || 'Failed to fetch file content' };
            }
        } catch (error) {
            return { success: false, message: 'Network error. Please try again.' };
        }
    },

    renderFilesList(files, elementId) {
        const element = document.getElementById(elementId);
        element.innerHTML = '';

        if (files.length === 0) {
            element.innerHTML = '<div class="text-muted text-center py-3">No CSV files found</div>';
            return;
        }

        files.forEach(file => {
            const item = document.createElement('a');
            item.className = 'list-group-item list-group-item-action d-flex justify-content-between align-items-center';
            item.innerHTML = `
                <div>
                    <strong>${file.original_filename}</strong>
                    <small class="d-block text-muted">Uploaded: ${new Date(file.uploaded_at).toLocaleString()}</small>
                </div>
                <button class="btn btn-sm btn-outline-primary view-file" data-file-id="${file.id}">View</button>
            `;
            element.appendChild(item);
        });

        // Add event listeners to view buttons
        document.querySelectorAll('.view-file').forEach(button => {
            button.addEventListener('click', async (e) => {
                const fileId = e.target.getAttribute('data-file-id');
                await this.showFileContent(fileId);
            });
        });
    },

    async showFileContent(fileId) {
        const result = await this.getFileContent(fileId);
        
        if (result.success) {
            const table = document.getElementById('csv-content-table');
            table.innerHTML = '';
            
            // Create header
            const thead = document.createElement('thead');
            const headerRow = document.createElement('tr');
            result.columns.forEach(column => {
                const th = document.createElement('th');
                th.textContent = column;
                headerRow.appendChild(th);
            });
            thead.appendChild(headerRow);
            table.appendChild(thead);
            
            // Create body
            const tbody = document.createElement('tbody');
            result.rows.forEach(row => {
                const tr = document.createElement('tr');
                row.forEach(cell => {
                    const td = document.createElement('td');
                    td.textContent = cell;
                    tr.appendChild(td);
                });
                tbody.appendChild(tr);
            });
            table.appendChild(tbody);
            
            // Set modal title
            document.querySelector('#file-content-modal .modal-title').textContent = result.filename;
            
            // Show modal
            const modal = new bootstrap.Modal(document.getElementById('file-content-modal'));
            modal.show();
        } else {
            alert(`Error: ${result.message}`);
        }
    }
};
