import React, { useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import './Tables.css';

// API Base URL - igual que en app.js
const API_BASE_URL = 'http://localhost:8000';

// IMPORTANTE: Acceder directamente al token como variable global, igual que en app.js
let authToken = localStorage.getItem('authToken') || null;

const CreateTable = () => {
  const navigate = useNavigate();
  const [tableName, setTableName] = useState('');
  const [columns, setColumns] = useState([{ name: '', data_type: 'INT', is_indexed: false }]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [selectedFile, setSelectedFile] = useState(null);
  const [hasHeaders, setHasHeaders] = useState(true);
  const [sqlPreview, setSqlPreview] = useState('');
  const [files, setFiles] = useState([]);

  const dataTypes = ['INT', 'FLOAT', 'VARCHAR', 'TEXT', 'BOOLEAN', 'DATE', 'TIMESTAMP'];
  const indexTypes = [
    { value: '', label: 'No Index' },
    { value: 'INDEX', label: 'Standard Index' },
    { value: 'UNIQUE', label: 'Unique Index' },
    { value: 'PRIMARY KEY', label: 'Primary Key' }
  ];

  useEffect(() => {
    // Actualizar token desde localStorage como en app.js
    authToken = localStorage.getItem('authToken') || null;

    if (!authToken) {
      navigate('/login');
      return;
    }

    // Cargar archivos del usuario para importación después de crear tabla
    loadUserFiles();
    
    // Generar preview de SQL cada vez que cambian las columnas o nombre de tabla
    generateSqlPreview();
  }, [tableName, columns]);

  const loadUserFiles = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/files/`, {
        headers: {
          'Authorization': `Bearer ${authToken}`
        }
      });

      if (!response.ok) {
        if (response.status === 401) {
          localStorage.removeItem('authToken');
          authToken = null;
          navigate('/login');
          return;
        }
        throw new Error('Failed to load files');
      }

      const data = await response.json();
      setFiles(data || []);
    } catch (error) {
      console.error('Error loading files:', error);
    }
  };

  const handleColumnChange = (index, field, value) => {
    const newColumns = [...columns];
    newColumns[index][field] = value;
    setColumns(newColumns);
  };

  const addColumn = () => {
    setColumns([...columns, { name: '', data_type: 'INT', is_indexed: false }]);
  };

  const removeColumn = (index) => {
    if (columns.length > 1) {
      const newColumns = columns.filter((_, i) => i !== index);
      setColumns(newColumns);
    }
  };

  // Generar sentencia SQL CREATE TABLE
  const generateSqlPreview = () => {
    if (!tableName) {
      setSqlPreview('');
      return;
    }

    let sql = `CREATE TABLE ${tableName} (\n`;
    
    const columnDefinitions = columns.map((col, index) => {
      if (!col.name) return null;
      
      let definition = `  ${col.name} ${col.data_type}`;
      
      if (col.data_type === 'VARCHAR') {
        const size = col.size || 255;
        definition += `(${size})`;
      }
      
      if (col.is_indexed && col.index_type === 'PRIMARY KEY') {
        definition += ' PRIMARY KEY';
      } else if (col.is_indexed && col.index_type === 'UNIQUE') {
        definition += ' UNIQUE';
      }
      
      return definition;
    }).filter(Boolean);
    
    sql += columnDefinitions.join(',\n');
    
    // Agregar índices separados
    const separateIndexes = columns
      .filter(col => col.is_indexed && col.name && col.index_type === 'INDEX')
      .map(col => `  INDEX (${col.name})`);
    
    if (separateIndexes.length > 0) {
      sql += ',\n' + separateIndexes.join(',\n');
    }
    
    sql += '\n);';
    
    setSqlPreview(sql);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!tableName.trim()) {
      setError('Table name is required');
      return;
    }

    if (!columns.some(col => col.name.trim())) {
      setError('At least one column with a name is required');
      return;
    }

    setLoading(true);
    setError('');

    try {
      // 1. Ejecutar CREATE TABLE exactamente como en app.js executeQuery()
      const response = await fetch(`${API_BASE_URL}/sql/`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${authToken}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query: sqlPreview })
      });
      
      const result = await response.json();
      
      if (!response.ok) {
        throw new Error(result.detail || 'Failed to create table');
      }
      
      // 2. Si también hay un archivo seleccionado, importarlo a la tabla recién creada
      if (selectedFile) {
        // Similar a uploadFile() de app.js
        const formData = new FormData();
        formData.append('file', selectedFile);
        
        const uploadResponse = await fetch(`${API_BASE_URL}/files/upload`, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${authToken}`
          },
          body: formData
        });
        
        if (!uploadResponse.ok) {
          throw new Error('Failed to upload file');
        }
        
        const fileData = await uploadResponse.json();
        const fileId = fileData.id;
        
        // Similar a importCSVToTable() de app.js
        const importResponse = await fetch(`${API_BASE_URL}/files/${fileId}/import/${tableName}`, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${authToken}`
          }
        });
        
        if (!importResponse.ok) {
          throw new Error('Failed to import data to table');
        }
        
        const importResult = await importResponse.json();
        alert(`Table created and data imported: ${importResult.success_count} rows imported successfully, ${importResult.error_count} errors`);
      } else {
        alert('Table created successfully!');
      }
      
      // Navegar a la tabla creada
      navigate(`/table/${tableName}`);
      
    } catch (err) {
      console.error('Error creating table:', err);
      setError(err.message || 'Failed to create table');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="create-table-container">
      <div className="create-table-wrapper">
        {/* Header */}
        <div className="create-table-header">
          <Link to="/tables" className="create-table-back-btn">
            ← Back
          </Link>
          <div className="create-table-title-section">
            <h1 className="create-table-main-title">🏗️ Create Table</h1>
            <p className="create-table-subtitle">Define your SQL table structure and optionally import data</p>
          </div>
        </div>

        {/* Main Create Card */}
        <div className="create-table-card">
          {/* Alerts */}
          {error && (
            <div className="create-table-error-alert">
              ❌ {error}
            </div>
          )}

          <form onSubmit={handleSubmit} className="create-table-form">
            {/* Table Name Section */}
            <div className="create-table-section">
              <label className="create-table-label">Table Name</label>
              <input
                type="text"
                value={tableName}
                onChange={(e) => setTableName(e.target.value)}
                className="create-table-input"
                placeholder="Enter table name (e.g., products, customers)"
                required
              />
            </div>

            {/* Columns Section */}
            <div className="create-table-section">
              <div className="create-table-columns-header">
                <label className="create-table-label">Column Definitions</label>
                <button
                  type="button"
                  onClick={addColumn}
                  className="create-table-add-column-btn"
                >
                  ➕ Add Column
                </button>
              </div>

              <div className="create-table-columns-list">
                {columns.map((column, index) => (
                  <div key={index} className="create-table-column-row">
                    <div className="create-table-column-inputs">
                      <input
                        type="text"
                        value={column.name}
                        onChange={(e) => handleColumnChange(index, 'name', e.target.value)}
                        className="create-table-column-input"
                        placeholder="Column name..."
                        required
                      />
                      
                      <select
                        value={column.data_type}
                        onChange={(e) => handleColumnChange(index, 'data_type', e.target.value)}
                        className="create-table-column-select"
                        required
                      >
                        {dataTypes.map(type => (
                          <option key={type} value={type}>{type}</option>
                        ))}
                      </select>

                      {column.data_type === 'VARCHAR' && (
                        <input
                          type="number"
                          value={column.size || 255}
                          onChange={(e) => handleColumnChange(index, 'size', parseInt(e.target.value) || 255)}
                          className="create-table-column-input"
                          placeholder="Size"
                          style={{ width: '80px' }}
                        />
                      )}

                      <label className="create-table-checkbox-label">
                        <input
                          type="checkbox"
                          checked={column.is_indexed}
                          onChange={(e) => handleColumnChange(index, 'is_indexed', e.target.checked)}
                          className="create-table-checkbox"
                        />
                        <span className="create-table-checkbox-text">Index</span>
                      </label>

                      {column.is_indexed && (
                        <select
                          value={column.index_type || ''}
                          onChange={e => handleColumnChange(index, 'index_type', e.target.value)}
                          className="create-table-column-select"
                        >
                          {indexTypes.map(opt => (
                            <option key={opt.value} value={opt.value}>{opt.label}</option>
                          ))}
                        </select>
                      )}
                    </div>

                    {columns.length > 1 && (
                      <button
                        type="button"
                        onClick={() => removeColumn(index)}
                        className="create-table-remove-column-btn"
                      >
                        ❌
                      </button>
                    )}
                  </div>
                ))}
              </div>
            </div>

            {/* SQL Preview Section */}
            <div className="create-table-section">
              <label className="create-table-label">SQL Preview</label>
              <div className="create-table-sql-preview">
                <pre>{sqlPreview || "-- SQL will appear here as you define your table --"}</pre>
              </div>
            </div>

            {/* Data Import Section (opcional) */}
            <div className="create-table-section">
              <label className="create-table-label">Data Import (Optional)</label>
              <input
                type="file"
                onChange={(e) => setSelectedFile(e.target.files[0])}
                className="create-table-input"
                accept=".csv"
              />
              <p style={{ color: '#64748b', fontSize: '0.9rem', margin: '0.5rem 0 0 0' }}>
                Optional: Upload a CSV to import data after table creation
              </p>
              
              <label className="create-table-checkbox-label" style={{ marginTop: '0.5rem' }}>
                <input
                  type="checkbox"
                  checked={hasHeaders}
                  onChange={(e) => setHasHeaders(e.target.checked)}
                  className="create-table-checkbox"
                />
                <span className="create-table-checkbox-text">CSV file has headers in first row</span>
              </label>
            </div>

            {/* Action Buttons */}
            <div className="create-table-actions">
              <Link to="/tables" className="create-table-cancel-btn">
                ← Cancel
              </Link>

              <button
                type="submit"
                disabled={loading || !sqlPreview}
                className={`create-table-submit-btn ${(loading || !sqlPreview) ? 'create-table-submit-disabled' : ''}`}
              >
                {loading ? (
                  <>
                    <div className="create-table-spinner"></div>
                    Creating...
                  </>
                ) : (
                  <>
                    🏗️ Create Table
                  </>
                )}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
};

export default CreateTable;