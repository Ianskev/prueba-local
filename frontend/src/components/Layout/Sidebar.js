import React, { useState } from 'react';
import { Nav, Button } from 'react-bootstrap';
import { Link, useLocation } from 'react-router-dom';
import { Menu, X, Home, Search, Settings, BarChart3, Upload, Database } from 'lucide-react';
import logo from '../../assets/logo.svg';
import './Sidebar.css';

const Sidebar = ({ onLogout }) => {
  const [isCollapsed, setIsCollapsed] = useState(false);
  const location = useLocation();

  const handleLogout = () => {
    if (onLogout) {
      onLogout();
    }
  };

  const navItems = [
    { name: 'Files', icon: Upload, path: '/files' },
    { name: 'Tables', icon: Database, path: '/tables' },
    { name: 'Query Executor', icon: Search, path: '/query' },
    { name: 'Metrics', icon: BarChart3, path: '/metrics' }
  ];

  const toggleNavbar = () => {
    setIsCollapsed(!isCollapsed);
  };

  const isActiveRoute = (path) => {
    return location.pathname === path || location.pathname.startsWith(path + '/');
  };

  return (
    <>
      {/* Toggle button for mobile/collapsed state */}
      {isCollapsed && (
        <button 
          onClick={toggleNavbar}
          className="navbar-toggle-btn"
        >
          <Menu size={20} />
        </button>
      )}

      <nav className={`navbar-bar ${isCollapsed ? 'navbar-collapsed' : ''}`}>
        
        {/* Header with logo and collapse button */}
        <div>
          <div className="navbar-header">
            <div className="navbar-logo">
              <img src={logo} alt="Smart Stock" className="navbar-logo-img" />
              <div className="navbar-title">SMART STOCK</div>
            </div>
            <button
              onClick={toggleNavbar}
              className="navbar-close-btn"
            >
              <X size={18} />
            </button>
          </div>

          {/* Navigation Links */}
          <ul className="navbar-links">
            {navItems.map((item) => {
              const IconComponent = item.icon;
              const isActive = isActiveRoute(item.path);
              
              return (
                <li key={item.name}>
                  <Link
                    to={item.path}
                    className={`navbar-link-btn ${isActive ? 'active' : ''}`}
                  >
                    <IconComponent size={18} />
                    <span>{item.name}</span>
                  </Link>
                </li>
              );
            })}
          </ul>
        </div>

        {/* Account section with logout */}
        <div className="navbar-account">
          <div className="navbar-account-info">
            <div className="navbar-account-avatar">
              <Settings size={16} />
            </div>
            <div className="navbar-account-details">
              <div className="navbar-account-label">Account</div>
              <div className="navbar-account-email">Smart Stock User</div>
            </div>
          </div>
          <Button 
            variant="outline-light" 
            size="sm" 
            onClick={handleLogout}
            style={{
              width: '100%', 
              marginTop: '10px',
              border: '1px solid rgba(255,255,255,0.3)',
              color: 'rgba(255,255,255,0.9)'
            }}
          >
            🚪 Logout
          </Button>
        </div>
      </nav>

      {/* Overlay for mobile */}
      {!isCollapsed && (
        <div className="navbar-overlay" onClick={toggleNavbar}></div>
      )}
    </>
  );
};

export default Sidebar;