import React from 'react';
import './Landing.css';
import WaveSketch from './WaveSketch';
import logo from '../../assets/logo.svg';
import boxLogo from '../../assets/box-logo.svg';
import { Link } from 'react-router-dom';

export default function Landing() {
  return (
    <div className="landing">
      <WaveSketch />
      <div className="navbar">
        <div className="logo">
          <img src={logo} alt="Smart Stock Logo" />
        </div>
        <div className="nav-links">
          <Link to="/login" className="nav-button login-btn">Login</Link>
          <Link to="/register" className="nav-button register-btn">Register</Link>
        </div>
      </div>
      
      <div className="content">
        <div className="hero-section">
         <div className="box-logo">  <img src={boxLogo} alt="Box Logo" />
                </div>
          <div className="hero-text">
            <h1>Smart Stock</h1>
            <h2>Turning Stock into Strategy.</h2>
            
            <div className="features">
              <div className="feature-item">
                <span className="checkmark">✓</span>
                <span className="feature-text">
                  Real-Time Tracking Know – </span>
                <span className="feature-description">
                   exactly what's in stock, 24/7.
                </span>
              </div>
            </div>
            
            <div className="cta-text">
              <p>P.S. Why wait? Your competitors aren't sleeping...</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}