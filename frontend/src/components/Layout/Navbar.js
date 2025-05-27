import React from 'react';
import { Navbar, Nav, Container, Button } from 'react-bootstrap';
import { Link } from 'react-router-dom';

const AppNavbar = ({ isAuthenticated, onLogout }) => {
  const handleLogout = () => {
    if (onLogout) {
      onLogout();
    }
  };

  return (
    <Navbar expand="lg" className="navbar">
      <Container>
        <Navbar.Brand href="/" style={{display: 'flex', alignItems: 'center', gap: '10px'}}>
          <span style={{fontSize: '1.5rem'}}>💼</span>
          <strong>Smart Stock</strong>
        </Navbar.Brand>
        
        <Navbar.Toggle aria-controls="basic-navbar-nav" />
        <Navbar.Collapse id="basic-navbar-nav">
          <Nav className="me-auto">
            {isAuthenticated && (
              <>
                <Nav.Link as={Link} to="/files" style={{fontWeight: '500'}}>
                  📁 Files
                </Nav.Link>
                <Nav.Link as={Link} to="/tables" style={{fontWeight: '500'}}>
                  📊 Tables
                </Nav.Link>
                <Nav.Link as={Link} to="/query" style={{fontWeight: '500'}}>
                  🔍 Query
                </Nav.Link>
              </>
            )}
          </Nav>
          
          <Nav>
            {isAuthenticated ? (
              <Button variant="outline-primary" onClick={handleLogout} size="sm">
                🚪 Logout
              </Button>
            ) : (
              <>
                <Nav.Link as={Link} to="/login" style={{fontWeight: '500'}}>
                  🔐 Wa
                </Nav.Link>
                <Nav.Link as={Link} to="/register" style={{fontWeight: '500'}}>
                  📝 Register
                </Nav.Link>
              </>
            )}
          </Nav>
        </Navbar.Collapse>
      </Container>
    </Navbar>
  );
};

export default AppNavbar;