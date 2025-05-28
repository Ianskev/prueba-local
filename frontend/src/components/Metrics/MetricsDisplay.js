import React, { useState, useEffect } from 'react';
import { Card, Row, Col, Alert, Spinner, Nav, Tab } from 'react-bootstrap';
import axios from 'axios';
import IndexComparison from './IndexComparison';

const MetricCard = ({ title, value, unit = '', icon }) => (
  <Card className="mb-4 shadow-sm">
    <Card.Body>
      <div className="d-flex justify-content-between align-items-center">
        <div>
          <h6 className="text-muted">{title}</h6>
          <h4 className="mb-0">
            {value} {unit}
          </h4>
        </div>
        {icon && <div className="fs-1 text-muted">{icon}</div>}
      </div>
    </Card.Body>
  </Card>
);

const MetricsDisplay = () => {
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [activeTab, setActiveTab] = useState('general');

  useEffect(() => {
    const fetchMetrics = async () => {
      try {
        setLoading(true);
        const token = localStorage.getItem('token');
        const response = await axios.get('/metrics', {
          headers: {
            Authorization: `Bearer ${token}`
          }
        });
        
        setMetrics(response.data);
        setError('');
      } catch (err) {
        console.error('Error fetching metrics:', err);
        setError('Failed to load metrics. Please try again later.');
      } finally {
        setLoading(false);
      }
    };

    if (activeTab === 'general') {
      fetchMetrics();
    }
  }, [activeTab]);

  // Renderiza las métricas generales
  const renderGeneralMetrics = () => {
    if (loading) {
      return (
        <div className="d-flex justify-content-center align-items-center" style={{ minHeight: '300px' }}>
          <Spinner animation="border" role="status">
            <span className="visually-hidden">Loading...</span>
          </Spinner>
        </div>
      );
    }

    if (error) {
      return <Alert variant="danger">{error}</Alert>;
    }

    if (!metrics) {
      return <Alert variant="info">No metrics data available.</Alert>;
    }

    return (
      <div className="mt-4">
        <h2 className="mb-4">Sistema de Base de Datos</h2>
        
        <Row>
          <Col md={6} lg={4}>
            <MetricCard 
              title="Total Tablas" 
              value={metrics.table_count || 0} 
              icon="📊" 
            />
          </Col>
          <Col md={6} lg={4}>
            <MetricCard 
              title="Total Índices" 
              value={metrics.index_count || 0} 
              icon="🔍" 
            />
          </Col>
          <Col md={6} lg={4}>
            <MetricCard 
              title="Tamaño Total de Datos" 
              value={Math.round((metrics.total_data_size || 0) / 1024)} 
              unit="KB"
              icon="💾" 
            />
          </Col>
        </Row>

        <h4 className="mt-4 mb-3">Rendimiento de Consultas</h4>
        <Row>
          <Col md={6} lg={4}>
            <MetricCard 
              title="Tiempo Promedio de Consulta" 
              value={metrics.avg_query_time ? metrics.avg_query_time.toFixed(2) : '0.00'} 
              unit="ms"
              icon="⏱️" 
            />
          </Col>
          <Col md={6} lg={4}>
            <MetricCard 
              title="Total de Consultas" 
              value={metrics.query_count || 0} 
              icon="🔢" 
            />
          </Col>
          <Col md={6} lg={4}>
            <MetricCard 
              title="Tasa de Aciertos de Cache" 
              value={(metrics.cache_hit_rate ? metrics.cache_hit_rate * 100 : 0).toFixed(1)} 
              unit="%"
              icon="📈" 
            />
          </Col>
        </Row>

        {metrics.most_used_tables && metrics.most_used_tables.length > 0 && (
          <>
            <h4 className="mt-4 mb-3">Tablas Más Usadas</h4>
            <Row>
              {metrics.most_used_tables.map((table, index) => (
                <Col md={6} lg={4} key={index}>
                  <Card className="mb-4 shadow-sm">
                    <Card.Body>
                      <h5>{table.name}</h5>
                      <p className="mb-0">Conteo de accesos: {table.access_count}</p>
                    </Card.Body>
                  </Card>
                </Col>
              ))}
            </Row>
          </>
        )}
      </div>
    );
  };

  return (
    <div className="container py-4">
      <h1 className="mb-4">🔍 Métricas y Rendimiento</h1>
      
      <Nav variant="tabs" className="mb-4">
        <Nav.Item>
          <Nav.Link 
            active={activeTab === 'general'} 
            onClick={() => setActiveTab('general')}
          >
            Métricas Generales
          </Nav.Link>
        </Nav.Item>
        <Nav.Item>
          <Nav.Link 
            active={activeTab === 'index-comparison'} 
            onClick={() => setActiveTab('index-comparison')}
          >
            Comparación de Índices
          </Nav.Link>
        </Nav.Item>
      </Nav>

      {activeTab === 'general' ? renderGeneralMetrics() : <IndexComparison />}
    </div>
  );
};

export default MetricsDisplay;
