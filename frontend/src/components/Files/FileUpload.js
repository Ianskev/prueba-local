import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import './Files.css';

// API Base URL - igual que en app.js
const API_BASE_URL = 'http://localhost:8000';

const FileUpload = () => {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [dragActive, setDragActive] = useState(false);
  const navigate = useNavigate();

  // Obtener token como en app.js
  const getAuthToken = () => {
    return localStorage.getItem('authToken'); // Cambiado de 'token' a 'authToken'
  };

  const handleFileSelect = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile) {
      // Validación como en app.js
      if (!selectedFile.name.endsWith('.csv')) {
        setError('Only CSV files are allowed');
        return;
      }
      setFile(selectedFile);
      setError('');
    }
  };

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      const droppedFile = e.dataTransfer.files[0];
      // Validación como en app.js
      if (!droppedFile.name.endsWith('.csv')) {
        setError('Only CSV files are allowed');
        return;
      }
      setFile(droppedFile);
      setError('');
    }
  };

  // uploadFile() - lógica exacta de app.js
  const uploadFile = async () => {
    if (!file) {
      setError('Please select a CSV file');
      return;
    }
    
    if (!file.name.endsWith('.csv')) {
      setError('Only CSV files are allowed');
      return;
    }
    
    const authToken = getAuthToken();
    if (!authToken) {
      setError('Authentication token is missing. Please log in again.');
      navigate('/login');
      return;
    }

    const formData = new FormData();
    formData.append('file', file);
    
    setUploading(true);
    setError('');
    setSuccess('');
    
    try {
      const response = await fetch(`${API_BASE_URL}/files/upload`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${authToken}`
        },
        body: formData
      });
      
      if (!response.ok) {
        if (response.status === 401) {
          setError('Your session has expired. Please log in again.');
          localStorage.removeItem('authToken');
          setTimeout(() => navigate('/login'), 2000);
          return;
        }
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Upload failed');
      }
      
      setSuccess('File uploaded successfully!');
      setFile(null); // Reset file input como en app.js
      
      // Redirigir después de un delay como el patrón de app.js
      setTimeout(() => {
        navigate('/files');
      }, 1500);
      
    } catch (error) {
      console.error('Upload error:', error);
      setError('Upload failed: ' + error.message);
    } finally {
      setUploading(false);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    await uploadFile();
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <div className="upload-container">
      <div className="upload-wrapper">
        {/* Header */}
        <div className="upload-header">
          <Link to="/files" className="upload-back-btn">
            ← Back to Files
          </Link>
          <div className="upload-title-section">
            <h1 className="upload-main-title">📤 Upload CSV File</h1>
            <p className="upload-subtitle">Upload your CSV files for data analysis</p>
          </div>
        </div>

        {/* Main Upload Card */}
        <div className="upload-card">
          {/* Alerts */}
          {error && (
            <div className="upload-error-alert">
              ❌ {error}
            </div>
          )}
          
          {success && (
            <div className="upload-success-alert">
              ✅ {success}
            </div>
          )}

          <form onSubmit={handleSubmit} className="upload-form">
            {/* Drag & Drop Area */}
            <div className="upload-section">
              <label className="upload-label">Select CSV File</label>
              
              <div
                className={`upload-drop-zone ${dragActive ? 'upload-drop-active' : ''} ${file ? 'upload-drop-success' : ''}`}
                onDragEnter={handleDrag}
                onDragLeave={handleDrag}
                onDragOver={handleDrag}
                onDrop={handleDrop}
                onClick={() => document.getElementById('file-input').click()}
              >
                <input
                  id="file-input"
                  type="file"
                  onChange={handleFileSelect}
                  accept=".csv"
                  className="upload-file-input"
                />

                {file ? (
                  <div className="upload-file-selected">
                    <div className="upload-file-icon">📄</div>
                    <h3 className="upload-file-name">{file.name}</h3>
                    <p className="upload-file-size">{formatFileSize(file.size)}</p>
                    <p className="upload-file-status">✓ CSV file ready for upload</p>
                  </div>
                ) : (
                  <div className="upload-file-empty">
                    <div className="upload-cloud-icon">☁️</div>
                    <h3 className="upload-empty-title">
                      Drop your CSV file here or click to browse
                    </h3>
                    <p className="upload-empty-subtitle">
                      Only CSV files are supported
                    </p>
                    <div className="upload-file-types">
                      <span className="upload-file-type">.CSV</span>
                    </div>
                  </div>
                )}

                {dragActive && (
                  <div className="upload-drag-overlay">
                    <p className="upload-drag-text">Drop CSV file here!</p>
                  </div>
                )}
              </div>

              {file && (
                <button
                  type="button"
                  onClick={() => {
                    setFile(null);
                    setError('');
                    setSuccess('');
                  }}
                  className="upload-remove-btn"
                >
                  ❌ Remove file
                </button>
              )}
            </div>

            {/* Action Buttons */}
            <div className="upload-actions">
              <Link to="/files" className="upload-cancel-btn">
                ← Back to Files
              </Link>

              <button
                type="submit"
                disabled={!file || uploading}
                className={`upload-submit-btn ${!file || uploading ? 'upload-submit-disabled' : ''}`}
              >
                {uploading ? (
                  <>
                    <div className="upload-spinner"></div>
                    Uploading...
                  </>
                ) : (
                  <>
                    📤 Upload CSV File
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

export default FileUpload;