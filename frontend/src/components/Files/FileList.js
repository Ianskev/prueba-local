import React, { useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import './Files.css';
import { fileService } from '../../services/api';

// API Base URL - igual que en app.js
const API_BASE_URL = 'http://localhost:8000';

const FileList = () => {
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [previewData, setPreviewData] = useState(null);
  const navigate = useNavigate();

  // Obtener token como en app.js
  const getAuthToken = () => {
    return localStorage.getItem('authToken'); // Cambiado de 'token' a 'authToken'
  };

  // loadUserFiles() - lógica exacta de app.js
  const loadUserFiles = async () => {
    try {
      setLoading(true);
      const authToken = getAuthToken();
      
      if (!authToken) {
        setError('Authentication token is missing. Please log in again.');
        navigate('/login');
        return;
      }

      const response = await fetch(`${API_BASE_URL}/files/`, {
        headers: {
          'Authorization': `Bearer ${authToken}`
        }
      });

      if (!response.ok) {
        if (response.status === 401) {
          setError('Your session has expired. Please log in again.');
          localStorage.removeItem('authToken');
          setTimeout(() => navigate('/login'), 2000);
          return;
        }
        throw new Error('Failed to load files');
      }

      const data = await response.json();
      setFiles(data);
      setError('');
    } catch (error) {
      console.error('Error loading files:', error);
      setError('Error loading files. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  // previewCSVFile() - lógica exacta de app.js
  const previewCSVFile = async (fileId) => {
    try {
      const authToken = getAuthToken();
      
      const response = await fetch(`${API_BASE_URL}/files/${fileId}/preview`, {
        headers: {
          'Authorization': `Bearer ${authToken}`
        }
      });
      
      if (!response.ok) {
        throw new Error('Failed to load preview');
      }
      
      const data = await response.json();
      setPreviewData(data);
      
      // Scroll to preview section
      document.getElementById('preview-section')?.scrollIntoView({ behavior: 'smooth' });
      
    } catch (error) {
      console.error('Preview error:', error);
      alert('Could not preview file: ' + error.message);
    }
  };

  // Función para eliminar un archivo
  const handleDelete = async (fileId, fileName) => {
    if (window.confirm(`Are you sure you want to delete file "${fileName}"? This action cannot be undone.`)) {
      try {
        await fileService.deleteFile(fileId);
        
        // Actualizar la lista de archivos localmente
        setFiles(prevFiles => prevFiles.filter(file => file.id !== fileId));
        setError('');
        
      } catch (err) {
        console.error('Error deleting file:', err);
        setError('Error deleting file: ' + (err.response?.data?.detail || err.message));
      }
    }
  };

  useEffect(() => {
    loadUserFiles();
  }, []);

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  // Componente para renderizar la tabla de preview (como en app.js)
  const PreviewTable = ({ data }) => {
    if (!data || !data.columns || data.columns.length === 0) {
      return <p>No data returned</p>;
    }

    return (
      <div className="files-preview-table-container">
        <table className="files-preview-table">
          <thead>
            <tr>
              {data.columns.map((column, index) => (
                <th key={index}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.records.map((record, rowIndex) => (
              <tr key={rowIndex}>
                {record.map((cell, cellIndex) => (
                  <td key={cellIndex}>
                    {cell === null ? 'NULL' : cell}
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
    <div className="files-container">
      <div className="files-wrapper">
        {/* Header */}
        <div className="files-header">
          <div className="files-header-content">
            <div className="files-title-section">
              <h1 className="files-main-title">📁 My Files</h1>
              <p className="files-subtitle">Manage your uploaded CSV files</p>
            </div>
            <Link to="/files/upload" className="files-upload-btn">
              📤 Upload New File
            </Link>
          </div>

          {/* Stats Cards */}
          <div className="files-stats-grid">
            <div className="files-stat-card">
              <div className="files-stat-icon">📄</div>
              <div className="files-stat-info">
                <p className="files-stat-label">Total Files</p>
                <p className="files-stat-value">{files.length}</p>
              </div>
            </div>
            
            <div className="files-stat-card">
              <div className="files-stat-icon">💾</div>
              <div className="files-stat-info">
                <p className="files-stat-label">Total Size</p>
                <p className="files-stat-value">
                  {formatFileSize(files.reduce((acc, file) => acc + (file.size || 0), 0))}
                </p>
              </div>
            </div>

            <div className="files-stat-card">
              <div className="files-stat-icon">📊</div>
              <div className="files-stat-info">
                <p className="files-stat-label">CSV Files</p>
                <p className="files-stat-value">{files.filter(f => f.filename?.endsWith('.csv')).length}</p>
              </div>
            </div>
          </div>
        </div>

        {/* Error Alert */}
        {error && (
          <div className="files-error-alert">
            ❌ {error}
          </div>
        )}

        {/* Loading State */}
        {loading ? (
          <div className="files-loading-container">
            <div className="files-loading-spinner"></div>
            <p className="files-loading-text">Loading your files...</p>
          </div>
        ) : files.length === 0 ? (
          /* Empty State */
          <div className="files-empty-state">
            <div className="files-empty-icon">📂</div>
            <h3 className="files-empty-title">No CSV files uploaded yet</h3>
            <p className="files-empty-subtitle">Upload your first CSV file to get started with data analysis</p>
            <Link to="/files/upload" className="files-empty-upload-btn">
              📤 Upload Your First File
            </Link>
          </div>
        ) : (
          /* Files Grid */
          <div className="files-grid">
            {files.map((file, index) => (
              <div
                key={file.id}
                className="files-card"
                style={{ animationDelay: `${index * 0.1}s` }}
              >
                <div className="files-card-header">
                  <div className="files-card-icon">📄</div>
                  <button
                    onClick={() => handleDelete(file.id, file.filename)}
                    className="files-delete-btn"
                  >
                    🗑️
                  </button>
                </div>
                
                <h3 className="files-card-title">{file.filename}</h3>
                
                <div className="files-card-details">
                  <div className="files-card-detail">
                    <span>Size:</span>
                    <span>{formatFileSize(file.size || 0)}</span>
                  </div>
                  <div className="files-card-detail">
                    <span>Type:</span>
                    <span>CSV</span>
                  </div>
                  <div className="files-card-detail">
                    <span>Uploaded:</span>
                    <span>{file.created_at ? new Date(file.created_at).toLocaleDateString() : 'Unknown'}</span>
                  </div>
                </div>

                {/* Actions - como en app.js */}
                <div className="files-card-actions">
                  <button 
                    onClick={() => previewCSVFile(file.id)}
                    className="preview-file"
                    title="Preview file content"
                  >
                    Preview
                  </button>
                </div>

                <div className="files-card-progress">
                  <div className="files-progress-bar"></div>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Preview Section - como en app.js */}
        {previewData && (
          <div id="preview-section" className="files-preview-section">
            <div className="files-preview-header">
              <h3>File Preview</h3>
              <button 
                onClick={() => setPreviewData(null)}
                className="files-preview-close"
              >
                ✕
              </button>
            </div>
            <PreviewTable data={previewData} />
          </div>
        )}
      </div>
    </div>
  );
};

export default FileList;