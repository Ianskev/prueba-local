import React, { useState } from 'react';
import './Auth.css';
import WaveSketch from './WaveSketch';
import logo from '../../assets/logo.svg';
import { Link, useNavigate } from 'react-router-dom';

// API Base URL - igual que en app.js
const API_BASE_URL = 'http://localhost:8000';

const Register = ({ onLogin }) => {
  const [userData, setUserData] = useState({ username: '', email: '', password: '' });
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  const handleChange = (e) => {
    const { name, value } = e.target;
    setUserData({ ...userData, [name]: value });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    
    try {
      // Lógica exacta de app.js para register - con fallback XMLHttpRequest
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
            xhr.send(JSON.stringify({ 
              email: userData.email, 
              username: userData.username, 
              password: userData.password 
            }));
          } catch (e) {
            reject(e);
          }
        });
      };

      // First try fetch, fall back to XMLHttpRequest (igual que app.js)
      try {
        const response = await fetch(`${API_BASE_URL}/register`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ 
            email: userData.email, 
            username: userData.username, 
            password: userData.password 
          })
        });
        
        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.detail || 'Registration failed');
        }
        
        // Registration successful
        setError('Registration successful! Please log in.');
        setTimeout(() => {
          navigate('/login');
        }, 2000);
        
      } catch (fetchError) {
        console.warn('Fetch failed, trying XMLHttpRequest:', fetchError);
        // Try with XMLHttpRequest as fallback
        await registerUser();
        
        // Registration successful
        setError('Registration successful! Please log in.');
        setTimeout(() => {
          navigate('/login');
        }, 2000);
      }
      
    } catch (error) {
      console.error('Registration error:', error);
      setError('Registration failed: ' + error.message);
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
          <Link to="/login" className="auth-nav-link">Login</Link>
          <Link to="/register" className="auth-nav-link register-active">Register</Link>
        </div>
      </div>

      <div className="auth-content">
        <div className="auth-card register-card">
          <div className="register-layout">
            <div className="register-left">
              <h2 className="register-slogan">
                It's Time<br />
                to Take<br />
                Charge of<br />
                Your<br />
                Stock
              </h2>
            </div>

            <div className="register-divider"></div>

            <div className="register-right">
              <h1 className="auth-title">Create An Account</h1>
              {error && (
                <div className="mb-3" style={{
                  background: error.includes('successful') ? 'rgba(0,255,0,0.08)' : 'rgba(255,0,0,0.08)',
                  color: error.includes('successful') ? '#16a34a' : '#b91c1c',
                  borderRadius: '12px',
                  padding: '12px 18px',
                  marginBottom: '1.5rem',
                  border: error.includes('successful') ? '1px solid #86efac' : '1px solid #fca5a5'
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
                    value={userData.username}
                    onChange={handleChange}
                    className="form-input"
                    required
                  />
                </div>

                <div className="form-group">
                  <label htmlFor="email" className="form-label">Email</label>
                  <input
                    type="email"
                    id="email"
                    name="email"
                    value={userData.email}
                    onChange={handleChange}
                    className="form-input"
                    required
                  />
                </div>

                <div className="form-group">
                  <label htmlFor="password" className="form-label">Password</label>
                  <input
                    type="password"
                    id="password"
                    name="password"
                    value={userData.password}
                    onChange={handleChange}
                    className="form-input"
                    required
                  />
                </div>

                <button type="submit" className="auth-button" disabled={loading}>
                  {loading ? 'Creating account...' : 'Register'}
                </button>
              </form>
              <div style={{ textAlign: 'center', marginTop: '1.5rem', color: '#fff' }}>
                Already have an account?{' '}
                <Link to="/login" style={{ color: '#FFD700', fontWeight: 'bold', textDecoration: 'underline' }}>
                  Sign In
                </Link>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Register;