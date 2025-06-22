import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import './Tables.css';

// API Base URL - igual que en app.js
const API_BASE_URL = 'http://localhost:8000';

// IMPORTANTE: Acceder directamente al token como variable global, igual que en app.js
let authToken = localStorage.getItem('authToken') || null;

const TableList = () => {
  const [tables, setTables] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  // loadDashboardData() - lógica exacta de app.js
  const fetchTables = async () => {
    try {
      setLoading(true);
      
      // Actualizar token desde localStorage como en app.js
      authToken = localStorage.getItem('authToken') || null;
      
      if (!authToken) {
        setError('Authentication token is missing. Please log in again.');
        return;
      }
      
      const response = await fetch(`${API_BASE_URL}/sql/dashboard`, {
        headers: {
          'Authorization': `Bearer ${authToken}`
        }
      });
      
      if (!response.ok) {
        if (response.status === 401) {
          setError('Your session has expired. Please log in again.');
          localStorage.removeItem('authToken');
          authToken = null;
          return;
        }
        throw new Error('Failed to load tables');
      }
      
      const data = await response.json();
      setTables(data.tables || []);
      setError('');
      
    } catch (err) {
      setError('Error loading tables. Please try again.');
      console.error('Error fetching tables:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTables();
  }, []);

  const handleDelete = async (tableName) => {
    if (window.confirm(`Are you sure you want to delete table ${tableName}? This action cannot be undone.`)) {
      try {
        // Usar fetch directo como en app.js
        const response = await fetch(`${API_BASE_URL}/sql/`, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${authToken}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ query: `DROP TABLE ${tableName}` })
        });
        
        if (!response.ok) {
          const result = await response.json();
          throw new Error(result.detail || 'Failed to delete table');
        }
        
        // Refresh tables locally
        setTables(prevTables => prevTables.filter(table => table.name !== tableName));
        
        // Opcional: Propagar el cambio a todas las partes de la aplicación
        // mediante un evento personalizado que otros componentes pueden escuchar
        const dropTableEvent = new CustomEvent('tableDropped', { 
          detail: { tableName } 
        });
        window.dispatchEvent(dropTableEvent);
        
        fetchTables(); // También refrescamos del servidor como respaldo
      } catch (err) {
        setError('Error deleting table: ' + err.message);
        console.error(err);
      }
    }
  };

  if (loading) return <div className="tables-loading-container">
    <div className="tables-loading-spinner"></div>
    <p className="tables-loading-text">Loading tables...</p>
  </div>;

  return (
    <div className="tables-container">
      <div className="tables-wrapper">
        {/* Header */}
        <div className="tables-header">
          <div className="tables-header-content">
            <div className="tables-title-section">
              <h1 className="tables-main-title">🗃️ Database Tables</h1>
              <p className="tables-subtitle">Manage your database tables and structures</p>
            </div>
            <Link to="/tables/create" className="tables-create-btn">
              ➕ Create New Table
            </Link>
          </div>

          {/* Stats Cards */}
          <div className="tables-stats-grid">
            <div className="tables-stat-card">
              <div className="tables-stat-icon">📊</div>
              <div className="tables-stat-info">
                <p className="tables-stat-label">Total Tables</p>
                <p className="tables-stat-value">{tables.length}</p>
              </div>
            </div>
            
            <div className="tables-stat-card">
              <div className="tables-stat-icon">🔗</div>
              <div className="tables-stat-info">
                <p className="tables-stat-label">Active Connections</p>
                <p className="tables-stat-value">1</p>
              </div>
            </div>

            <div className="tables-stat-card">
              <div className="tables-stat-icon">⚡</div>
              <div className="tables-stat-info">
                <p className="tables-stat-label">Status</p>
                <p className="tables-stat-value">Online</p>
              </div>
            </div>
          </div>
        </div>

        {/* Error Alert */}
        {error && (
          <div className="tables-error-alert">
            ❌ {error}
          </div>
        )}

        {/* Loading State */}
        {loading ? (
          <div className="tables-loading-container">
            <div className="tables-loading-spinner"></div>
            <p className="tables-loading-text">Loading your tables...</p>
          </div>
        ) : tables.length === 0 ? (
          /* Empty State */
          <div className="tables-empty-state">
            <div className="tables-empty-icon">🗃️</div>
            <h3 className="tables-empty-title">No tables found</h3>
            <p className="tables-empty-subtitle">Create your first database table to get started</p>
            <Link to="/tables/create" className="tables-empty-create-btn">
              ➕ Create Your First Table
            </Link>
          </div>
        ) : (
          /* Tables Grid */
          <div className="tables-grid">
            {tables.map((table, index) => (
              <div
                key={table.name}
                className="tables-card"
                style={{ animationDelay: `${index * 0.1}s` }}
              >
                <div className="tables-card-header">
                  <div className="tables-card-icon">🗃️</div>
                  <button
                    onClick={() => handleDelete(table.name)}
                    className="tables-delete-btn"
                  >
                    🗑️
                  </button>
                </div>
                
                <h3 className="tables-card-title">{table.name}</h3>
                
                <div className="tables-card-details">
                  <div className="tables-card-detail">
                    <span>Columns:</span>
                    <span>{table.columns?.length || 0}</span>
                  </div>
                  <div className="tables-card-detail">
                    <span>Rows:</span>
                    <span>{table.row_count?.toLocaleString() || 0}</span>
                  </div>
                  <div className="tables-card-detail">
                    <span>Created:</span>
                    <span>{new Date(table.created_at).toLocaleDateString()}</span>
                  </div>
                </div>

                <div className="tables-card-actions">
                  <Link to={`/tables/${table.name}`} className="tables-view-btn">
                    👁️ View Data
                  </Link>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default TableList;