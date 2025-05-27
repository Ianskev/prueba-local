import React, { useState } from 'react';
import './Auth.css';
import WaveSketch from './WaveSketch';
import logo from '../../assets/logo.svg';
import { Link, useNavigate } from 'react-router-dom';

// API Base URL - igual que en app.js
const API_BASE_URL = 'http://localhost:8000';

const Login = ({ onLogin }) => {
  const [credentials, setCredentials] = useState({ username: '', password: '' });
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  const handleChange = (e) => {
    const { name, value } = e.target;
    setCredentials({ ...credentials, [name]: value });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    
    try {
      // Lógica exacta de app.js para login
      const formData = new FormData();
      formData.append('username', credentials.username); // API expects username field
      formData.append('password', credentials.password);
      
      const response = await fetch(`${API_BASE_URL}/token`, {
        method: 'POST',
        body: formData
      });
      
      if (!response.ok) {
        throw new Error('Login failed');
      }
      
      const data = await response.json();
      const authToken = data.access_token;
      
      // Guardar token como en app.js
      localStorage.setItem('authToken', authToken);
      
      // Añade log para depuración
      console.log('Login exitoso, token guardado:', authToken);
      
      if (typeof onLogin === 'function') {
        onLogin();
      } else {
        console.error("Error: onLogin no es una función");
      }
      
      // Retrasa un poco la navegación para asegurar que el estado se actualice
      setTimeout(() => {
        navigate('/files');
      }, 100);
      
    } catch (error) {
      console.error('Login error:', error);
      setError('Login failed: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="auth-container">
      <WaveSketch />

      <div className="auth-navbar">
        <div
          className="auth-logo"
          style={{ cursor: 'pointer' }}
          onClick={() => window.location.href = '/'}
        >
          <img src={logo} alt="Smart Stock Logo" />
        </div>
        <div className="auth-nav-links">
          <Link to="/login" className="auth-nav-link login-active">Login</Link>
          <Link to="/register" className="auth-nav-link">Register</Link>
        </div>
      </div>

      <div className="auth-content">
        <div className="auth-card">
          <h1 className="auth-title">Welcome!</h1>
          {error && (
            <div className="mb-3" style={{
              background: 'rgba(255,0,0,0.08)',
              color: '#b91c1c',
              borderRadius: '12px',
              padding: '12px 18px',
              marginBottom: '1.5rem',
              border: '1px solid #fca5a5'
            }}>
              {error}
            </div>
          )}
          <form onSubmit={handleSubmit} className="auth-form">
            <div className="form-group">
              <label htmlFor="username" className="form-label">Username</label>
              <input
                type="text"
                id="username"
                name="username"
                value={credentials.username}
                onChange={handleChange}
                className="form-input"
                required
                autoComplete="username"
                placeholder="Enter your username"
              />
            </div>

            <div className="form-group">
              <label htmlFor="password" className="form-label">Password</label>
              <input
                type="password"
                id="password"
                name="password"
                value={credentials.password}
                onChange={handleChange}
                className="form-input"
                required
                autoComplete="current-password"
                placeholder="Enter your password"
              />
            </div>

            <button type="submit" className="auth-button" disabled={loading}>
              {loading ? 'Signing in...' : 'Login'}
            </button>
          </form>
          <div style={{ textAlign: 'center', marginTop: '1.5rem', color: '#fff' }}>
            Don't have an account?{' '}
            <Link to="/register" style={{ color: '#FFD700', fontWeight: 'bold', textDecoration: 'underline' }}>
              Create Account
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Login;