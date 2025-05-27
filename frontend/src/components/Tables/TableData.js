import React, { useState, useEffect } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import './Tables.css';

// API Base URL - igual que en app.js
const API_BASE_URL = 'http://localhost:8000';

// IMPORTANTE: Acceder directamente al token como variable global, igual que en app.js
let authToken = localStorage.getItem('authToken') || null;

const TableData = () => {
  const { tableName } = useParams();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [executionTime, setExecutionTime] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [rowsPerPage] = useState(100); // Constante como en app.js (LIMIT 100)
  const navigate = useNavigate();

  // executeQuery() - exactly like in app.js
  const executeQuery = async (query) => {
    try {
      setLoading(true);
      setError('');
      setMessage('');
      
      // Actualizar token cada vez como en app.js
      authToken = localStorage.getItem('authToken') || null;
      
      if (!authToken) {
        setError('Authentication token is missing. Please log in again.');
        navigate('/login');
        return;
      }

      console.log('Executing query:', query);
      
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
        if (response.status === 401) {
          setError('Your session has expired. Please log in again.');
          localStorage.removeItem('authToken');
          authToken = null;
          setTimeout(() => navigate('/login'), 2000);
          return;
        }
        throw new Error(result.detail || 'Query execution failed');
      }
      
      console.log('Query result:', result);
      
      // Exactamente como displayQueryResult() en app.js
      const { data, message, execution_time } = result;
      setMessage(`${message} (${execution_time.toFixed(3)}s)`);
      setExecutionTime(execution_time);
      setData(data);
      
      // Calcular paginación (aproximada)
      // Usamos rowCount si está disponible
      if (data?.rowCount) {
        setTotalPages(Math.ceil(data.rowCount / rowsPerPage));
      }
      
    } catch (err) {
      console.error('Query error:', err);
      setError(`Query execution failed: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (tableName) {
      // Usar exactamente la misma consulta que app.js
      const query = `SELECT * FROM ${tableName} LIMIT ${rowsPerPage};`;
      executeQuery(query);
      
      // También ejecutar un COUNT para obtener el total de filas
      const countQuery = `SELECT COUNT(*) as total FROM ${tableName};`;
      fetch(`${API_BASE_URL}/sql/`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${authToken}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query: countQuery })
      })
      .then(response => response.json())
      .then(result => {
        if (result.data?.records?.length > 0) {
          const total = result.data.records[0][0];
          setTotalPages(Math.ceil(total / rowsPerPage));
        }
      })
      .catch(console.error);
    }
  }, [tableName]); // Quité currentPage y rowsPerPage de las dependencias

  const handlePageChange = (newPage) => {
    if (newPage >= 1 && newPage <= totalPages) {
      setCurrentPage(newPage);
    }
  };

  // Implementa la visualización como displayQueryResult() en app.js
  const renderTable = () => {
    if (!data || !data.columns || data.columns.length === 0) {
      return <p className="table-no-data">No data returned</p>;
    }

    return (
      <div className="table-results-container">
        <table className="table-data-table">
          <thead>
            <tr>
              {data.columns.map((column, idx) => (
                <th key={idx}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.records.map((record, rowIdx) => (
              <tr key={rowIdx}>
                {record.map((cell, cellIdx) => (
                  <td key={cellIdx}>
                    {cell === null ? <span className="table-null-value">NULL</span> : String(cell)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  return (
    <div className="table-data-container">
      <div className="table-data-wrapper">
        {/* Header */}
        <div className="table-data-header">
          <Link to="/tables" className="table-data-back-btn">
            ← Back to Tables
          </Link>
          <div className="table-data-title-section">
            <h1 className="table-data-title">
              🗃️ {tableName}
              <span className="table-data-subtitle">Table Data</span>
            </h1>
          </div>
        </div>

        {/* Table Stats & Controls */}
        <div className="table-data-controls">
          <div className="table-data-info">
            {data && (
              <>
                <span className="table-data-count">
                  {data.records?.length || 0} rows
                  {totalPages > 1 && ` (page ${currentPage} of ${totalPages})`}
                </span>
                <span className="table-data-time">
                  Query time: {executionTime?.toFixed(3) || 0}s
                </span>
              </>
            )}
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="table-data-pagination">
              <button 
                onClick={() => handlePageChange(1)}
                disabled={currentPage === 1}
                className="table-data-page-btn"
              >
                ⏮️ First
              </button>
              <button 
                onClick={() => handlePageChange(currentPage - 1)}
                disabled={currentPage === 1}
                className="table-data-page-btn"
              >
                ◀️ Prev
              </button>
              <span className="table-data-page-info">
                Page {currentPage} of {totalPages}
              </span>
              <button 
                onClick={() => handlePageChange(currentPage + 1)}
                disabled={currentPage === totalPages}
                className="table-data-page-btn"
              >
                Next ▶️
              </button>
              <button 
                onClick={() => handlePageChange(totalPages)}
                disabled={currentPage === totalPages}
                className="table-data-page-btn"
              >
                Last ⏭️
              </button>
            </div>
          )}
        </div>

        {/* Message */}
        {message && (
          <div className="table-data-message">
            ✅ {message}
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="table-data-error">
            ❌ {error}
          </div>
        )}

        {/* Loading Indicator */}
        {loading ? (
          <div className="table-data-loading">
            <div className="table-data-spinner"></div>
            <span>Loading table data...</span>
          </div>
        ) : (
          renderTable()
        )}
      </div>
    </div>
  );
};

export default TableData;