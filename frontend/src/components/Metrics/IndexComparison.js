import React, { useState } from 'react';
import { metricsService, queryService } from '../../services/api';
import './IndexComparison.css';

const IndexComparison = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [results, setResults] = useState(null);
  const [baseTableName, setBaseTableName] = useState('test_index');
  const [dataSize, setDataSize] = useState(500);
  const [selectedIndices, setSelectedIndices] = useState([
    'BTREE', 'HASH', 'AVL', 'ISAM'
  ]);
  
  // Lista de índices disponibles para probar
  const availableIndices = [
    { value: 'BTREE', label: 'B+Tree' },
    { value: 'HASH', label: 'Hash' },
    { value: 'AVL', label: 'AVL Tree' },
    { value: 'ISAM', label: 'ISAM' },
    { value: 'RTREE', label: 'R-Tree' }
  ];
  
  // Función para ejecutar la comparación
  const runComparison = async () => {
    if (selectedIndices.length === 0) {
      setError('Seleccione al menos un tipo de índice');
      return;
    }
    
    setLoading(true);
    setError('');
    
    try {
      const results = await metricsService.compareIndices({
        baseTableName,
        dataSize,
        indexTypes: selectedIndices
      });
      
      setResults(results);
    } catch (err) {
      console.error('Error al ejecutar comparación:', err);
      setError('Error al ejecutar la comparación: ' + err.message);
    } finally {
      setLoading(false);
    }
  };
  
  // Función para limpiar las tablas de prueba
  const cleanupTables = async () => {
    setLoading(true);
    try {
      for (const indexType of selectedIndices) {
        const tableName = `${baseTableName}_${indexType.toLowerCase()}`;
        try {
          await queryService.executeQuery(`DROP TABLE IF EXISTS ${tableName}`);
        } catch (error) {
          console.error(`Error al eliminar tabla ${tableName}:`, error);
        }
      }
      setError('');
      setResults(null);
    } catch (err) {
      setError('Error al limpiar tablas: ' + err.message);
    } finally {
      setLoading(false);
    }
  };
  
  // Manejar cambio en selección de índices
  const handleIndexChange = (indexValue) => {
    setSelectedIndices(
      selectedIndices.includes(indexValue)
        ? selectedIndices.filter(i => i !== indexValue)
        : [...selectedIndices, indexValue]
    );
  };
  
  return (
    <div className="index-comparison-container">
      <h2 className="index-comparison-title">🔍 Comparación de Rendimiento de Índices</h2>
      <p className="index-comparison-subtitle">Compare el rendimiento de diferentes tipos de índices</p>
      
      <div className="index-comparison-card">
        <h3 className="index-comparison-card-title">Configuración de la Comparación</h3>
        
        <div className="index-comparison-form">
          <div className="index-comparison-form-row">
            <div className="index-comparison-form-group">
              <label htmlFor="baseTableName">Nombre Base de Tabla</label>
              <input
                type="text"
                id="baseTableName"
                value={baseTableName}
                onChange={(e) => setBaseTableName(e.target.value)}
                disabled={loading}
                className="index-comparison-input"
              />
              <small className="index-comparison-help-text">
                Las tablas se nombrarán como {baseTableName}_btree, {baseTableName}_hash, etc.
              </small>
            </div>
            
            <div className="index-comparison-form-group">
              <label htmlFor="dataSize">Tamaño de Datos (Número de Filas)</label>
              <input
                type="number"
                id="dataSize"
                value={dataSize}
                onChange={(e) => setDataSize(parseInt(e.target.value, 10) || 100)}
                min="10"
                max="1000"
                disabled={loading}
                className="index-comparison-input"
              />
              <small className="index-comparison-help-text">
                Valores grandes tomarán más tiempo de procesamiento
              </small>
            </div>
          </div>
          
          <div className="index-comparison-form-group">
            <label>Índices a Comparar</label>
            <div className="index-comparison-checkboxes">
              {availableIndices.map(index => (
                <label key={index.value} className="index-comparison-checkbox-label">
                  <input
                    type="checkbox"
                    checked={selectedIndices.includes(index.value)}
                    onChange={() => handleIndexChange(index.value)}
                    disabled={loading}
                    className="index-comparison-checkbox"
                  />
                  {index.label}
                </label>
              ))}
            </div>
          </div>
          
          <div className="index-comparison-buttons">
            <button
              onClick={runComparison}
              disabled={loading || selectedIndices.length === 0}
              className="index-comparison-button index-comparison-primary-button"
            >
              {loading ? 'Ejecutando Comparación...' : 'Iniciar Comparación'}
            </button>
            
            <button
              onClick={cleanupTables}
              disabled={loading}
              className="index-comparison-button index-comparison-secondary-button"
            >
              Eliminar Tablas de Prueba
            </button>
          </div>
        </div>
      </div>
      
      {error && (
        <div className="index-comparison-error">
          ❌ {error}
        </div>
      )}
      
      {results && results.indices && results.indices.length > 0 && (
        <div className="index-comparison-results-card">
          <h3 className="index-comparison-card-title">Resultados de la Comparación</h3>
          
          <div className="index-comparison-chart">
            <h4>Gráfico de Rendimiento</h4>
            <div className="index-comparison-chart-bars">
              {results.indices.map((index, i) => (
                <div key={index} className="index-comparison-chart-bar-group">
                  <div className="index-comparison-chart-label">{index}</div>
                  <div className="index-comparison-chart-bars-container">
                    <div 
                      className="index-comparison-chart-bar index-comparison-creation-bar"
                      style={{ height: `${Math.min(200, results.executionTimes[i].createTime / 10)}px` }}
                      title={`Tiempo de creación: ${results.executionTimes[i].createTime.toFixed(2)} ms`}
                    >
                      <span className="index-comparison-bar-value">
                        {results.executionTimes[i].createTime.toFixed(0)} ms
                      </span>
                    </div>
                    <div 
                      className="index-comparison-chart-bar index-comparison-query-bar"
                      style={{ height: `${Math.min(200, results.executionTimes[i].queryTime / 10)}px` }}
                      title={`Tiempo de consulta: ${results.executionTimes[i].queryTime.toFixed(2)} ms`}
                    >
                      <span className="index-comparison-bar-value">
                        {results.executionTimes[i].queryTime.toFixed(0)} ms
                      </span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
            <div className="index-comparison-chart-legend">
              <div className="index-comparison-legend-item">
                <div className="index-comparison-legend-color index-comparison-creation-color"></div>
                <span>Tiempo de Creación</span>
              </div>
              <div className="index-comparison-legend-item">
                <div className="index-comparison-legend-color index-comparison-query-color"></div>
                <span>Tiempo de Consulta</span>
              </div>
            </div>
          </div>
          
          <table className="index-comparison-table">
            <thead>
              <tr>
                <th>Tipo de Índice</th>
                <th>Tiempo de Creación (ms)</th>
                <th>Tiempo de Consulta (ms)</th>
                <th>Tiempo Total (ms)</th>
                <th>Operaciones de Disco</th>
              </tr>
            </thead>
            <tbody>
              {results.indices.map((index, i) => (
                <tr key={index}>
                  <td>{index}</td>
                  <td>{results.executionTimes[i].createTime.toFixed(2)}</td>
                  <td>{results.executionTimes[i].queryTime.toFixed(2)}</td>
                  <td>{results.executionTimes[i].totalTime.toFixed(2)}</td>
                  <td>{results.diskOperations[i]}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

export default IndexComparison;
