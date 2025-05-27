// Main application module

document.addEventListener('DOMContentLoaded', () => {
    // Initialize the application
    initApp();
    
    // Set up event listeners
    setupEventListeners();
});

function initApp() {
    // Check if user is already logged in
    if (Auth.init()) {
        showAppContainer();
        loadDashboard();
    } else {
        showAuthContainer();
    }
}

function setupEventListeners() {
    // Auth tabs
    document.getElementById('login-tab').addEventListener('click', function(e) {
        e.preventDefault();
        document.getElementById('login-form').style.display = 'block';
        document.getElementById('register-form').style.display = 'none';
        document.getElementById('login-tab').classList.add('active');
        document.getElementById('register-tab').classList.remove('active');
    });
    
    document.getElementById('register-tab').addEventListener('click', function(e) {
        e.preventDefault();
        document.getElementById('login-form').style.display = 'none';
        document.getElementById('register-form').style.display = 'block';
        document.getElementById('login-tab').classList.remove('active');
        document.getElementById('register-tab').classList.add('active');
    });
    
    // Login form
    document.getElementById('login').addEventListener('submit', async function(e) {
        e.preventDefault();
        const email = document.getElementById('login-email').value;
        const password = document.getElementById('login-password').value;
        
        // Show loading message
        document.getElementById('auth-message').innerHTML = `
            <div class="alert alert-info">Logging in...</div>
        `;
        
        const result = await Auth.login(email, password);
        
        if (result.success) {
            document.getElementById('auth-message').innerHTML = '';
            showAppContainer();
            loadDashboard();
        } else {
            console.error("Login failed:", result.message);
            document.getElementById('auth-message').innerHTML = `
                <div class="alert alert-danger">${result.message}</div>
            `;
        }
    });
    
    // Register form
    document.getElementById('register').addEventListener('submit', async function(e) {
        e.preventDefault();
        const username = document.getElementById('register-username').value;
        const email = document.getElementById('register-email').value;
        const password = document.getElementById('register-password').value;
        
        const result = await Auth.register(username, email, password);
        
        if (result.success) {
            document.getElementById('auth-message').innerHTML = `
                <div class="alert alert-success">Registration successful! You can now log in.</div>
            `;
            document.getElementById('login-tab').click();
        } else {
            document.getElementById('auth-message').innerHTML = `
                <div class="alert alert-danger">${result.message}</div>
            `;
        }
    });
    
    // Logout button
    document.getElementById('logout-btn').addEventListener('click', function() {
        Auth.logout();
        showAuthContainer();
    });
    
    // Navigation links
    document.getElementById('dashboard-link').addEventListener('click', function(e) {
        e.preventDefault();
        showSection('dashboard');
        loadDashboard();
    });
    
    document.getElementById('files-link').addEventListener('click', function(e) {
        e.preventDefault();
        showSection('files');
        loadFiles();
    });
    
    document.getElementById('query-link').addEventListener('click', function(e) {
        e.preventDefault();
        showSection('query');
        if (!SQLQuery.editor) {
            SQLQuery.initEditor();
        }
    });
    
    // File upload form
    document.getElementById('upload-form').addEventListener('submit', async function(e) {
        e.preventDefault();
        const fileInput = document.getElementById('csv-file');
        
        if (fileInput.files.length === 0) {
            document.getElementById('upload-message').innerHTML = `
                <div class="alert alert-danger">Please select a file</div>
            `;
            return;
        }
        
        const file = fileInput.files[0];
        document.getElementById('upload-message').innerHTML = `
            <div class="alert alert-info">Uploading file...</div>
        `;
        
        const result = await Files.uploadFile(file);
        
        if (result.success) {
            document.getElementById('upload-message').innerHTML = `
                <div class="alert alert-success">File uploaded successfully</div>
            `;
            document.getElementById('upload-form').reset();
            loadFiles();
        } else {
            document.getElementById('upload-message').innerHTML = `
                <div class="alert alert-danger">${result.message}</div>
            `;
        }
    });
    
    // Run SQL query button
    document.getElementById('run-query').addEventListener('click', async function() {
        const query = SQLQuery.editor.getValue();
        
        if (!query.trim()) {
            document.getElementById('query-message').innerHTML = `
                <div class="error-message">Please enter a SQL query</div>
            `;
            return;
        }
        
        document.getElementById('query-message').innerHTML = `
            <div class="alert alert-info">Running query...</div>
        `;
        
        const result = await SQLQuery.executeQuery(query);
        document.getElementById('query-message').innerHTML = '';
        SQLQuery.renderQueryResults(result);
    });
}

function showAppContainer() {
    document.getElementById('auth-container').style.display = 'none';
    document.getElementById('app-container').style.display = 'block';
    
    // Set username in the navbar
    if (Auth.user) {
        document.getElementById('username-display').textContent = Auth.user.username;
    }
}

function showAuthContainer() {
    document.getElementById('auth-container').style.display = 'block';
    document.getElementById('app-container').style.display = 'none';
}

function showSection(section) {
    // Hide all sections
    document.getElementById('dashboard-section').style.display = 'none';
    document.getElementById('files-section').style.display = 'none';
    document.getElementById('query-section').style.display = 'none';
    
    // Remove active class from all links
    document.querySelectorAll('.navbar-nav .nav-link').forEach(link => {
        link.classList.remove('active');
    });
    
    // Show the selected section and mark its link as active
    document.getElementById(`${section}-section`).style.display = 'block';
    document.getElementById(`${section}-link`).classList.add('active');
}

async function loadDashboard() {
    // Get dashboard data
    const result = await SQLQuery.getDashboardData();
    
    if (result.success) {
        // Display tables
        SQLQuery.renderTables(result.data.tables);
        
        // Display CSV files
        Files.renderFilesList(result.data.csv_files, 'csv-files-list');
    } else {
        // If unauthorized, redirect to login
        if (result.unauthorized) {
            showAuthContainer();
            document.getElementById('auth-message').innerHTML = `
                <div class="alert alert-warning">${result.message}</div>
            `;
            return;
        }
        
        // Show error messages
        document.getElementById('tables-list').innerHTML = `
            <div class="alert alert-danger">${result.message}</div>
        `;
        document.getElementById('csv-files-list').innerHTML = `
            <div class="alert alert-danger">${result.message}</div>
        `;
    }
}

async function loadFiles() {
    // Get the list of files
    const result = await Files.listFiles();
    
    if (result.success) {
        Files.renderFilesList(result.files, 'files-list');
    } else {
        document.getElementById('files-list').innerHTML = `
            <div class="error-message">${result.message}</div>
        `;
    }
}
