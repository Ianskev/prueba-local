// Main application entry point

// API Base URL
const API_BASE_URL = 'http://localhost:8000';

// Application state
let currentUser = null;
let authToken = localStorage.getItem('authToken') || null;

// DOM Elements references
const loginForm = document.getElementById('loginForm');
const registerForm = document.getElementById('registerForm');
const dashboardSection = document.getElementById('dashboard');
const authSection = document.getElementById('authentication');
const querySection = document.getElementById('query-section');
const uploadSection = document.getElementById('upload-section');
const logoutBtn = document.getElementById('logoutBtn');
const queryInput = document.getElementById('queryInput');
const executeQueryBtn = document.getElementById('executeQueryBtn');
const resultTable = document.getElementById('resultTable');
const csvFileInput = document.getElementById('csvFileInput');
const uploadFileBtn = document.getElementById('uploadFileBtn');
const userFilesSelect = document.getElementById('userFiles');
const tableNameSelect = document.getElementById('tableName');
const importBtn = document.getElementById('importBtn');
const tablesList = document.getElementById('tablesList');
const filesList = document.getElementById('filesList');

// Event listeners setup
document.addEventListener('DOMContentLoaded', () => {
    setupEventListeners();
    checkAuthentication();
});

function setupEventListeners() {
    // Auth related events
    if (loginForm) loginForm.addEventListener('submit', handleLogin);
    if (registerForm) registerForm.addEventListener('submit', handleRegister);
    if (logoutBtn) logoutBtn.addEventListener('click', handleLogout);
    
    // Query related events
    if (executeQueryBtn) executeQueryBtn.addEventListener('click', executeQuery);
    
    // File upload related events
    if (uploadFileBtn) uploadFileBtn.addEventListener('click', uploadFile);
    if (importBtn) importBtn.addEventListener('click', importCSVToTable);
}

// Authentication check
function checkAuthentication() {
    if (authToken) {
        // Verify token with backend
        fetch(`${API_BASE_URL}/me`, {
            headers: {
                'Authorization': `Bearer ${authToken}`
            }
        })
        .then(response => {
            if (response.ok) {
                return response.json();
            } else {
                throw new Error('Authentication failed');
            }
        })
        .then(userData => {
            currentUser = userData;
            showDashboard();
        })
        .catch(error => {
            console.error('Authentication error:', error);
            localStorage.removeItem('authToken');
            authToken = null;
            showAuthForms();
        });
    } else {
        showAuthForms();
    }
}

// UI State Management
function showDashboard() {
    if (authSection) authSection.classList.add('hidden');
    if (dashboardSection) {
        dashboardSection.classList.remove('hidden');
        loadDashboardData();
    }
    if (querySection) querySection.classList.remove('hidden');
    if (uploadSection) uploadSection.classList.remove('hidden');
}

function showAuthForms() {
    if (authSection) authSection.classList.remove('hidden');
    if (dashboardSection) dashboardSection.classList.add('hidden');
    if (querySection) querySection.classList.add('hidden');
    if (uploadSection) uploadSection.classList.add('hidden');
}

// Load dashboard data
function loadDashboardData() {
    // Fetch and display dashboard data
    fetch(`${API_BASE_URL}/sql/dashboard`, {
        headers: {
            'Authorization': `Bearer ${authToken}`
        }
    })
    .then(response => response.json())
    .then(data => {
        // Update UI with tables and files
        updateTablesList(data.tables);
        updateFilesList(data.files);
        
        // Update dropdown for import function
        populateTablesDropdown(data.tables);
        loadUserFiles();
    })
    .catch(error => console.error('Error loading dashboard:', error));
}

function updateTablesList(tables) {
    if (!tablesList) return;
    
    if (tables.length === 0) {
        tablesList.innerHTML = '<li>No tables created yet</li>';
        return;
    }
    
    tablesList.innerHTML = tables.map(table => 
        `<li>${table.name} <button class="view-table" data-table="${table.name}">View</button></li>`
    ).join('');
    
    // Add click listeners to view buttons
    document.querySelectorAll('.view-table').forEach(btn => {
        btn.addEventListener('click', e => {
            const tableName = e.target.dataset.table;
            queryInput.value = `SELECT * FROM ${tableName} LIMIT 100;`;
            executeQuery();
        });
    });
}

function updateFilesList(files) {
    if (!filesList) return;
    
    if (files.length === 0) {
        filesList.innerHTML = '<li>No CSV files uploaded yet</li>';
        return;
    }
    
    filesList.innerHTML = files.map(file => 
        `<li>${file.filename} <button class="preview-file" data-fileid="${file.id}">Preview</button></li>`
    ).join('');
    
    // Add click listeners to preview buttons
    document.querySelectorAll('.preview-file').forEach(btn => {
        btn.addEventListener('click', e => {
            const fileId = e.target.dataset.fileid;
            previewCSVFile(fileId);
        });
    });
}

function populateTablesDropdown(tables) {
    if (!tableNameSelect) return;
    
    tableNameSelect.innerHTML = '<option value="">Select a table</option>';
    tables.forEach(table => {
        const option = document.createElement('option');
        option.value = table.name;
        option.textContent = table.name;
        tableNameSelect.appendChild(option);
    });
}

function loadUserFiles() {
    if (!userFilesSelect) return;
    
    fetch(`${API_BASE_URL}/files/`, {
        headers: {
            'Authorization': `Bearer ${authToken}`
        }
    })
    .then(response => response.json())
    .then(files => {
        userFilesSelect.innerHTML = '<option value="">Select a CSV file</option>';
        files.forEach(file => {
            const option = document.createElement('option');
            option.value = file.id;
            option.textContent = file.filename;
            userFilesSelect.appendChild(option);
        });
    })
    .catch(error => console.error('Error loading files:', error));
}

// Auth handlers
async function handleLogin(e) {
    e.preventDefault();
    const email = document.getElementById('loginEmail').value;
    const password = document.getElementById('loginPassword').value;
    
    try {
        const formData = new FormData();
        formData.append('username', email); // API expects username field but we use email
        formData.append('password', password);
        
        const response = await fetch(`${API_BASE_URL}/token`, {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) {
            throw new Error('Login failed');
        }
        
        const data = await response.json();
        authToken = data.access_token;
        localStorage.setItem('authToken', authToken);
        checkAuthentication();
    } catch (error) {
        console.error('Login error:', error);
        alert('Login failed: ' + error.message);
    }
}

async function handleRegister(e) {
    e.preventDefault();
    const email = document.getElementById('registerEmail').value;
    const username = document.getElementById('registerUsername').value;
    const password = document.getElementById('registerPassword').value;
    
    try {
        // For development only: Using XMLHttpRequest as a CORS fallback when fetch fails
        const registerUser = () => {
            return new Promise((resolve, reject) => {
                try {
                    const xhr = new XMLHttpRequest();
                    xhr.open('POST', `${API_BASE_URL}/register`, true);
                    xhr.setRequestHeader('Content-Type', 'application/json');
                    xhr.onreadystatechange = function () {
                        if (xhr.readyState === 4) {
                            if (xhr.status >= 200 && xhr.status < 300) {
                                resolve(JSON.parse(xhr.responseText));
                            } else {
                                try {
                                    const errorData = JSON.parse(xhr.responseText);
                                    reject(new Error(errorData.detail || 'Registration failed'));
                                } catch (e) {
                                    reject(new Error('Registration failed with status: ' + xhr.status));
                                }
                            }
                        }
                    };
                    xhr.send(JSON.stringify({ email, username, password }));
                } catch (e) {
                    reject(e);
                }
            });
        };

        // First try fetch, fall back to XMLHttpRequest
        try {
            const response = await fetch(`${API_BASE_URL}/register`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ email, username, password })
            });
            
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Registration failed');
            }
            
            alert('Registration successful! Please log in.');
            document.getElementById('loginEmail').value = email;
            document.getElementById('registerForm').reset();
        } catch (fetchError) {
            console.warn('Fetch failed, trying XMLHttpRequest:', fetchError);
            // Try with XMLHttpRequest as fallback
            await registerUser();
            alert('Registration successful! Please log in.');
            document.getElementById('loginEmail').value = email;
            document.getElementById('registerForm').reset();
        }
    } catch (error) {
        console.error('Registration error:', error);
        alert('Registration failed: ' + error.message);
    }
}

function handleLogout() {
    localStorage.removeItem('authToken');
    authToken = null;
    currentUser = null;
    showAuthForms();
}

// Query execution
async function executeQuery() {
    if (!queryInput || !resultTable) return;
    
    const query = queryInput.value.trim();
    if (!query) {
        alert('Please enter a SQL query');
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE_URL}/sql/`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${authToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ query })
        });
        
        const result = await response.json();
        
        if (!response.ok) {
            throw new Error(result.detail || 'Query execution failed');
        }
        
        displayQueryResult(result);
        
        // If it was a CREATE TABLE query, refresh dashboard
        if (query.toLowerCase().includes('create table') && result.message.includes('successful')) {
            loadDashboardData();
        }
        
    } catch (error) {
        console.error('Query error:', error);
        alert('Query execution failed: ' + error.message);
    }
}

function displayQueryResult(result) {
    const { data, message, execution_time } = result;
    
    // Display execution message
    const messageDiv = document.getElementById('queryMessage');
    if (messageDiv) {
        messageDiv.textContent = `${message} (${execution_time.toFixed(3)}s)`;
    }
    
    // Clear previous results
    resultTable.innerHTML = '';
    
    if (!data.columns || data.columns.length === 0) {
        resultTable.innerHTML = '<p>No data returned</p>';
        return;
    }
    
    // Create table header
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    
    data.columns.forEach(column => {
        const th = document.createElement('th');
        th.textContent = column;
        headerRow.appendChild(th);
    });
    
    thead.appendChild(headerRow);
    resultTable.appendChild(thead);
    
    // Create table body
    const tbody = document.createElement('tbody');
    
    data.records.forEach(record => {
        const row = document.createElement('tr');
        
        record.forEach(cell => {
            const td = document.createElement('td');
            td.textContent = cell === null ? 'NULL' : cell;
            row.appendChild(td);
        });
        
        tbody.appendChild(row);
    });
    
    resultTable.appendChild(tbody);
}

// File upload handlers
async function uploadFile() {
    if (!csvFileInput) return;
    
    const file = csvFileInput.files[0];
    if (!file) {
        alert('Please select a CSV file');
        return;
    }
    
    if (!file.name.endsWith('.csv')) {
        alert('Only CSV files are allowed');
        return;
    }
    
    const formData = new FormData();
    formData.append('file', file);
    
    try {
        const response = await fetch(`${API_BASE_URL}/files/upload`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${authToken}`
            },
            body: formData
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Upload failed');
        }
        
        alert('File uploaded successfully!');
        csvFileInput.value = '';
        loadDashboardData(); // Refresh file list
        
    } catch (error) {
        console.error('Upload error:', error);
        alert('Upload failed: ' + error.message);
    }
}

async function previewCSVFile(fileId) {
    try {
        const response = await fetch(`${API_BASE_URL}/files/${fileId}/preview`, {
            headers: {
                'Authorization': `Bearer ${authToken}`
            }
        });
        
        if (!response.ok) {
            throw new Error('Failed to load preview');
        }
        
        const data = await response.json();
        
        // Display preview in result table
        resultTable.innerHTML = '';
        
        // Create table header
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        
        data.columns.forEach(column => {
            const th = document.createElement('th');
            th.textContent = column;
            headerRow.appendChild(th);
        });
        
        thead.appendChild(headerRow);
        resultTable.appendChild(thead);
        
        // Create table body
        const tbody = document.createElement('tbody');
        
        data.records.forEach(record => {
            const row = document.createElement('tr');
            
            record.forEach(cell => {
                const td = document.createElement('td');
                td.textContent = cell === null ? 'NULL' : cell;
                row.appendChild(td);
            });
            
            tbody.appendChild(row);
        });
        
        resultTable.appendChild(tbody);
        
    } catch (error) {
        console.error('Preview error:', error);
        alert('Could not preview file: ' + error.message);
    }
}

async function importCSVToTable() {
    const fileId = userFilesSelect.value;
    const tableName = tableNameSelect.value;
    
    if (!fileId || !tableName) {
        alert('Please select both a CSV file and a table');
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE_URL}/files/${fileId}/import/${tableName}`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${authToken}`
            }
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Import failed');
        }
        
        const result = await response.json();
        alert(`Import complete: ${result.success_count} rows imported successfully, ${result.error_count} errors`);
        
        // Execute a SELECT query to show the imported data
        queryInput.value = `SELECT * FROM ${tableName} LIMIT 100;`;
        executeQuery();
        
    } catch (error) {
        console.error('Import error:', error);
        alert('Import failed: ' + error.message);
    }
}
