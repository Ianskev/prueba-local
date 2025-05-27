import React, { useState, useEffect } from 'react';
import { Card, Row, Col, Alert, Spinner } from 'react-bootstrap';
import axios from 'axios';

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

    fetchMetrics();
  }, []);

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
      <h2 className="mb-4">System Metrics</h2>
      
      <Row>
        <Col md={6} lg={4}>
          <MetricCard 
            title="Total Tables" 
            value={metrics.table_count || 0} 
            icon="📊" 
          />
        </Col>
        <Col md={6} lg={4}>
          <MetricCard 
            title="Total Indices" 
            value={metrics.index_count || 0} 
            icon="🔍" 
          />
        </Col>
        <Col md={6} lg={4}>
          <MetricCard 
            title="Total Data Size" 
            value={Math.round((metrics.total_data_size || 0) / 1024)} 
            unit="KB"
            icon="💾" 
          />
        </Col>
      </Row>

      <h4 className="mt-4 mb-3">Query Performance</h4>
      <Row>
        <Col md={6} lg={4}>
          <MetricCard 
            title="Average Query Time" 
            value={metrics.avg_query_time ? metrics.avg_query_time.toFixed(2) : '0.00'} 
            unit="ms"
            icon="⏱️" 
          />
        </Col>
        <Col md={6} lg={4}>
          <MetricCard 
            title="Total Queries" 
            value={metrics.query_count || 0} 
            icon="🔢" 
          />
        </Col>
        <Col md={6} lg={4}>
          <MetricCard 
            title="Cache Hit Rate" 
            value={(metrics.cache_hit_rate ? metrics.cache_hit_rate * 100 : 0).toFixed(1)} 
            unit="%"
            icon="📈" 
          />
        </Col>
      </Row>

      {metrics.most_used_tables && metrics.most_used_tables.length > 0 && (
        <>
          <h4 className="mt-4 mb-3">Most Used Tables</h4>
          <Row>
            {metrics.most_used_tables.map((table, index) => (
              <Col md={6} lg={4} key={index}>
                <Card className="mb-4 shadow-sm">
                  <Card.Body>
                    <h5>{table.name}</h5>
                    <p className="mb-0">Access count: {table.access_count}</p>
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

export default MetricsDisplay;
