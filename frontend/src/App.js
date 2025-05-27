import React, { useState, useEffect } from 'react';
import { Routes, Route, Navigate, useNavigate } from 'react-router-dom';
import Sidebar from './components/Layout/Sidebar';
import Login from './components/Auth/Login';
import Register from './components/Auth/Register';
import FileList from './components/Files/FileList';
import FileUpload from './components/Files/FileUpload';
import TableList from './components/Tables/TableList';
import CreateTable from './components/Tables/CreateTable';
import TableData from './components/Tables/TableData';
import QueryExecutor from './components/Query/QueryExecutor';
import MetricsDisplay from './components/Metrics/MetricsDisplay';
import Landing from './components/Landing/Landing';
import { Container, Row, Col } from 'react-bootstrap';

function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    const checkAuth = () => {
      const token = localStorage.getItem('token');
      setIsAuthenticated(!!token);
    };
    checkAuth();
    window.addEventListener('storage', checkAuth);
    return () => {
      window.removeEventListener('storage', checkAuth);
    };
  }, []);

  const ProtectedRoute = ({ children }) => {
    const token = localStorage.getItem('token');
    if (!token) {
      return <Navigate to="/login" replace />;
    }
    return children;
  };

  const handleLogin = () => {
    setIsAuthenticated(true);
  };

  const handleLogout = () => {
    localStorage.removeItem('token');
    window.location.href = '/login';
  };

  return (
    <div className="App">
      <Container fluid>
        <Routes>
          {/* Rutas públicas SIN layout */}
          <Route path="/" element={<Landing />} />
          <Route path="/login" element={<Login onLogin={handleLogin} />} />
          <Route path="/register" element={<Register onLogin={handleLogin} />} />

          {/* Rutas privadas CON layout */}
          <Route
            path="/*"
            element={
              <ProtectedRoute>
                <Row>
                  <Col md={3} lg={2} className="sidebar">
                    <Sidebar onLogout={handleLogout} />
                  </Col>
                  <Col md={9} lg={10} className="content-area">
                    <Routes>
                      <Route path="/files" element={<FileList />} />
                      <Route path="/files/upload" element={<FileUpload />} />
                      <Route path="/tables" element={<TableList />} />
                      <Route path="/tables/create" element={<CreateTable />} />
                      <Route path="/tables/:tableName" element={<TableData />} />
                      <Route path="/query" element={<QueryExecutor />} />
                      <Route path="/metrics" element={<MetricsDisplay />} />
                      <Route path="*" element={<Navigate to="/" />} />
                    </Routes>
                  </Col>
                </Row>
              </ProtectedRoute>
            }
          />
          {/* Catch-all para rutas no encontradas */}
          <Route path="*" element={<Navigate to="/" />} />
        </Routes>
      </Container>
    </div>
  );
}

export default App;