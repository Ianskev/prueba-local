import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import './Query.css';

// API Base URL - igual que en app.js
const API_BASE_URL = 'http://localhost:8000';

// IMPORTANTE: Acceder directamente al token como variable global, igual que en app.js
let authToken = localStorage.getItem('authToken') || null;

const QueryExecutor = () => {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [executionTime, setExecutionTime] = useState(null);
  
  // Estados para Import CSV to Table
  const [files, setFiles] = useState([]);
  const [tables, setTables] = useState([]);
  const [selectedFileId, setSelectedFileId] = useState('');
  const [selectedTableName, setSelectedTableName] = useState('');
  const [importing, setImporting] = useState(false);
  const [showImportSection, setShowImportSection] = useState(false);
  
  const navigate = useNavigate();
  const location = useLocation();

  // Verificar autenticación al montar (similar a app.js)
  useEffect(() => {
    checkAuthentication();
  }, []);

  // Función que verifica la autenticación igual que en app.js
  const checkAuthentication = () => {
    // Actualizar authToken global (exactamente como en app.js)
    authToken = localStorage.getItem('authToken') || null;

    if (authToken) {
      fetch(`${API_BASE_URL}/me`, {
        headers: {
          'Authorization': `Bearer ${authToken}`
        }
      })
      .then(response => {
        if (!response.ok) {
          throw new Error('Authentication failed');
        }
        return response.json();
      })
      .then(userData => {
        // Autenticación exitosa, cargar datos
        loadTables();
        loadUserFiles();
      })
      .catch(error => {
        console.error('Authentication error:', error);
        // Token inválido, limpiar y redirigir
        localStorage.removeItem('authToken');
        authToken = null;
        navigate('/login');
      });
    } else {
      navigate('/login');
    }
  };

  // Obtener token como en app.js (consistencia con tus otros componentes)
  const getAuthToken = () => {
    // Siempre obtenemos el token más reciente del localStorage
    return localStorage.getItem('authToken');
  };

  // executeQuery() - usando la variable global authToken como en app.js
  const executeQuery = async () => {
    const trimmedQuery = query.trim();
    if (!trimmedQuery) {
      setError('Please enter a SQL query');
      return;
    }

    if (!authToken) {
      setError('Authentication token is missing. Please log in again.');
      navigate('/login');
      return;
    }

    setLoading(true);
    setError('');
    setMessage('');
    setResults(null);
    setExecutionTime(null);

    try {
      console.log('Executing query with token:', authToken);
      const response = await fetch(`${API_BASE_URL}/sql/`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${authToken}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query: trimmedQuery })
      });

      const result = await response.json();

      if (!response.ok) {
        if (response.status === 401) {
          setError('Your session has expired. Please log in again.');
          localStorage.removeItem('authToken');
          authToken = null;
          setTimeout(() => navigate('/login'), 2000);
          return;
        }
        throw new Error(result.detail || 'Query execution failed');
      }

      // Mostrar resultado exactamente como en app.js
      setMessage(`${result.message} (${result.execution_time.toFixed(3)}s)`);
      setExecutionTime(result.execution_time);
      setResults(result.data);

      // Si fue CREATE TABLE, refresh los dropdowns
      if (trimmedQuery.toLowerCase().includes('create table') && result.message.includes('successful')) {
        loadTables();
      }
      
      // Añadido: Si fue DROP TABLE, también refresh los dropdowns
      if (trimmedQuery.toLowerCase().includes('drop table') && result.message.includes('successful')) {
        loadTables();
        // Si el dropdown del table name está seleccionado con la tabla que acabamos de borrar, lo limpiamos
        if (selectedTableName && trimmedQuery.toLowerCase().includes(selectedTableName.toLowerCase())) {
          setSelectedTableName('');
        }
      }

    } catch (error) {
      console.error('Query error:', error);
      setError('Query execution failed: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  // loadUserFiles() - usando la variable global authToken como en app.js
  const loadUserFiles = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/files/`, {
        headers: {
          'Authorization': `Bearer ${authToken}`
        }
      });

      if (!response.ok) {
        if (response.status === 401) {
          checkAuthentication();
          return;
        }
        throw new Error('Failed to load files');
      }

      const data = await response.json();
      setFiles(data);
    } catch (error) {
      console.error('Error loading files:', error);
    }
  };

  // loadTables() - basado en loadDashboardData() de app.js
  const loadTables = async () => {
    try {
      const authToken = getAuthToken();
      const response = await fetch(`${API_BASE_URL}/sql/dashboard`, {
        headers: {
          'Authorization': `Bearer ${authToken}`
        }
      });

      if (!response.ok) throw new Error('Failed to load tables');

      const data = await response.json();
      setTables(data.tables || []);
    } catch (error) {
      console.error('Error loading tables:', error);
    }
  };

  // importCSVToTable() - lógica exacta de app.js
  const importCSVToTable = async () => {
    if (!selectedFileId || !selectedTableName) {
      setError('Please select both a CSV file and a table');
      return;
    }

    const authToken = getAuthToken();
    setImporting(true);
    setError('');

    try {
      const response = await fetch(`${API_BASE_URL}/files/${selectedFileId}/import/${selectedTableName}`, {
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
      setMessage(`Import complete: ${result.success_count} rows imported successfully, ${result.error_count} errors`);

      // Ejecutar SELECT query para mostrar datos importados - como en app.js
      setQuery(`SELECT * FROM ${selectedTableName} LIMIT 100;`);
      setTimeout(() => {
        executeQuery();
      }, 1000);

    } catch (error) {
      console.error('Import error:', error);
      setError('Import failed: ' + error.message);
    } finally {
      setImporting(false);
    }
  };

  // handleSubmit para el form
  const handleSubmit = async (e) => {
    e.preventDefault();
    await executeQuery();
  };

  // displayQueryResult() - lógica exacta de app.js
  const renderResults = () => {
    if (!results || !results.columns || results.columns.length === 0) {
      return null;
    }

    return (
      <div className="query-results-container">
        <div className="query-results-header">
          <h4 className="query-results-title">📊 Query Results</h4>
          <div className="query-results-stats">
            <span className="query-stat-badge">
              📄 {results.records?.length || 0} rows
            </span>
            {executionTime && (
              <span className="query-stat-badge">
                ⚡ {executionTime.toFixed(3)}s
              </span>
            )}
          </div>
        </div>

        <div className="query-table-container">
          <div className="query-table-scroll">
            <table className="query-table">
              <thead className="query-table-header">
                <tr>
                  {results.columns.map((column, index) => (
                    <th key={index} className="query-table-th">{column}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="query-table-body">
                {results.records?.map((record, rowIndex) => (
                  <tr key={rowIndex} className="query-table-row">
                    {record.map((cell, cellIndex) => (
                      <td key={cellIndex} className="query-table-td">
                        <div className="query-cell-content">
                          {cell === null ? <span className="query-null">NULL</span> : String(cell)}
                        </div>
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    );
  };

  // Cargar datos al montar y verificar URL params
  useEffect(() => {
    loadUserFiles();
    loadTables();

    // Verificar si viene con query preestablecida desde URL
    const params = new URLSearchParams(location.search);
    const tableParam = params.get('table');
    if (tableParam) {
      setQuery(`SELECT * FROM ${tableParam} LIMIT 100;`);
    }
  }, [location.search]);

  return (
    <div className="query-container">
      <div className="query-wrapper">
        {/* Header */}
        <div className="query-header">
          <div className="query-title-section">
            <h1 className="query-main-title">🔍 SQL Query Console</h1>
            <p className="query-subtitle">Execute SQL queries and import CSV data</p>
          </div>
          
          {/* Toggle Import Section */}
          <button
            onClick={() => setShowImportSection(!showImportSection)}
            className="query-toggle-import-btn"
          >
            {showImportSection ? '🔍 Show Query Only' : '📥 Show Import CSV'}
          </button>
        </div>

        {/* Import CSV Section */}
        {showImportSection && (
          <div className="query-import-card">
            <div className="query-card-header">
              <div className="query-card-icon">📥</div>
              <h3 className="query-card-title">Import CSV to Table</h3>
            </div>

            <div className="query-import-form">
              <div className="query-import-row">
                <div className="query-import-field">
                  <label className="query-label">Select CSV File</label>
                  <select 
                    value={selectedFileId}
                    onChange={(e) => setSelectedFileId(e.target.value)}
                    className="query-select"
                  >
                    <option value="">Select a CSV file</option>
                    {files.map(file => (
                      <option key={file.id} value={file.id}>
                        {file.filename}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="query-import-field">
                  <label className="query-label">Select Table</label>
                  <select 
                    value={selectedTableName}
                    onChange={(e) => setSelectedTableName(e.target.value)}
                    className="query-select"
                  >
                    <option value="">Select a table</option>
                    {tables.map(table => (
                      <option key={table.name} value={table.name}>
                        {table.name}
                      </option>
                    ))}
                  </select>
                </div>

                <button
                  onClick={importCSVToTable}
                  disabled={!selectedFileId || !selectedTableName || importing}
                  className={`query-import-btn ${!selectedFileId || !selectedTableName || importing ? 'query-import-disabled' : ''}`}
                >
                  {importing ? (
                    <>
                      <div className="query-spinner"></div>
                      Importing...
                    </>
                  ) : (
                    '📥 Import'
                  )}
                </button>
              </div>

              {selectedFileId && selectedTableName && (
                <div className="query-import-info">
                  <p className="query-import-warning">
                    ⚠️ This will import data from <strong>{files.find(f => f.id.toString() === selectedFileId)?.filename}</strong> into table <strong>{selectedTableName}</strong>
                  </p>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Main Query Card */}
        <div className="query-card">
          <div className="query-card-header">
            <div className="query-card-icon">⚡</div>
            <h3 className="query-card-title">SQL Query Executor</h3>
          </div>

          <form onSubmit={handleSubmit} className="query-form">
            {/* Query Input Section */}
            <div className="query-section">
              <label className="query-label">SQL Query</label>
              <textarea
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                className="query-textarea"
                rows={8}
                placeholder="-- Enter your SQL query here
SELECT * FROM table_name;
-- Examples:
-- SELECT COUNT(*) FROM products;
-- CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
-- UPDATE users SET status = 'active' WHERE id = 1;
-- INSERT INTO categories (name) VALUES ('Electronics');"
              />
              <div className="query-input-footer">
                <span className="query-input-hint">💡 Use standard SQL syntax. Results limited to 1000 rows.</span>
              </div>
            </div>

            {/* Action Button */}
            <div className="query-actions">
              <button
                type="submit"
                disabled={loading || !query.trim()}
                className={`query-submit-btn ${loading || !query.trim() ? 'query-submit-disabled' : ''}`}
              >
                {loading ? (
                  <>
                    <div className="query-spinner"></div>
                    Executing Query...
                  </>
                ) : (
                  <>
                    ⚡ Execute Query
                  </>
                )}
              </button>
            </div>
          </form>
        </div>

        {/* Message Alert - como en app.js */}
        {message && (
          <div className="query-success-alert">
            ✅ {message}
          </div>
        )}

        {/* Error Alert */}
        {error && (
          <div className="query-error-alert">
            ❌ {error}
          </div>
        )}

        {/* Results */}
        {renderResults()}
      </div>
    </div>
  );
};

export default QueryExecutor;