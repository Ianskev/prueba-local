import axios from 'axios';

const api = axios.create({
  baseURL: 'http://127.0.0.1:8000/api', // URL completa del backend
});

// Interceptor para añadir el token a las solicitudes
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      // Asegurarnos que el token tenga el formato correcto
      if (!token.startsWith('Bearer ')) {
        config.headers.Authorization = `Bearer ${token}`;
      } else {
        config.headers.Authorization = token;
      }
      
      // Debug para ver qué token se está enviando
      console.log('Sending request with token:', config.headers.Authorization);
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Manejador global para respuestas 401
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response && error.response.status === 401) {
      console.error('Authentication error:', error.response.data);
      
      // Solo limpiar el token si realmente es inválido
      // (no por otras razones como permisos insuficientes)
      if (error.response.data && error.response.data.detail === 'Invalid token') {
        console.log('Token inválido detectado, limpiando sesión...');
        localStorage.removeItem('token');
        // No redireccionar aquí para evitar ciclos
      }
    }
    
    // Asegurar que los errores de validación se manejen correctamente
    if (error.response?.data && typeof error.response.data === 'object') {
      // Si hay errores de validación, los mantenemos como están
      // para que los componentes puedan procesarlos correctamente
      console.log('Error response data:', error.response.data);
    }
    
    return Promise.reject(error);
  }
);

// Servicios de autenticación
export const authService = {
  register: (userData) => api.post('/auth/register', userData),
  login: (userData) => api.post('/auth/login', userData),
};

// Servicios de gestión de archivos
export const fileService = {
  uploadFile: (formData) => api.post('/files/upload', formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
      // No necesitas añadir el token aquí, el interceptor lo hace
    },
  }),
  listFiles: () => api.get('/files'),
  deleteFile: (filename) => api.delete(`/files/${filename}`),
};

// Servicios de gestión de tablas
export const tableService = {
  createTable: (tableData) => api.post('/tables/create', tableData),
  listTables: () => api.get('/tables'),
  deleteTable: (tableName) => api.delete(`/tables/${tableName}`),
  async getTableData(tableName, page = 1) {
    try {
      const response = await api.get(`/tables/${tableName}/data?page=${page}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching table data:', error);
      throw error;
    }
  },
};

// Servicio de consultas
export const queryService = {
  executeQuery: async (query) => {
    const response = await api.post('/sql', { query });
    
    console.log('=== API SERVICE DEBUG ===');
    console.log('Full axios response:', response);
    console.log('Response.data:', response.data);
    console.log('=== END API SERVICE DEBUG ===');
    
    return response.data; // Devolver el objeto completo, no solo los datos
  },
};

// Servicio de métricas
export const metricsService = {
  getMetrics: async () => {
    try {
      // Recopilar estadísticas básicas usando consultas SQL
      const tableCountQuery = await queryService.executeQuery("SELECT COUNT(*) as table_count FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
      const indexCountQuery = await queryService.executeQuery("SELECT COUNT(*) as index_count FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'");
      const totalSizeQuery = await queryService.executeQuery("PRAGMA page_count * page_size as total_size");
      
      // Simulamos algunas métricas adicionales
      return {
        data: {
          table_count: tableCountQuery.data.records[0][0] || 0,
          index_count: indexCountQuery.data.records[0][0] || 0,
          total_data_size: totalSizeQuery.data.records[0][0] || 0,
          avg_query_time: Math.random() * 10 + 5, // 5-15ms
          query_count: Math.floor(Math.random() * 1000) + 100,
          cache_hit_rate: Math.random() * 0.8 + 0.1, // 10%-90%
          most_used_tables: [
            { name: 'users', access_count: Math.floor(Math.random() * 500) + 50 },
            { name: 'products', access_count: Math.floor(Math.random() * 400) + 40 },
            { name: 'orders', access_count: Math.floor(Math.random() * 300) + 30 }
          ]
        }
      };
    } catch (error) {
      console.error('Error getting metrics:', error);
      throw error;
    }
  },
  compareIndices: async (testConfig) => {
    const results = {
      indices: [],
      executionTimes: [],
      diskOperations: []
    };
    
    try {
      for (const indexType of testConfig.indexTypes) {
        const tableName = `${testConfig.baseTableName}_${indexType.toLowerCase()}`;
        results.indices.push(indexType);
        
        // Crear tabla de prueba si no existe
        const dropTableQuery = `DROP TABLE IF EXISTS ${tableName}`;
        await queryService.executeQuery(dropTableQuery);
        
        // Medir tiempo de creación
        const createStartTime = performance.now();
        
        // Crear tabla sin índice primero
        const createTableQuery = `
          CREATE TABLE ${tableName} (
            id INTEGER,
            value INTEGER,
            name TEXT
          )
        `;
        await queryService.executeQuery(createTableQuery);
        
        // Insertar datos de prueba
        const batchSize = 100;
        const totalRecords = Math.min(testConfig.dataSize, 1000);
        
        for (let i = 0; i < totalRecords; i += batchSize) {
          let valuesBatch = [];
          for (let j = 0; j < batchSize && i + j < totalRecords; j++) {
            valuesBatch.push(`(${i + j}, ${Math.floor(Math.random() * 1000)}, 'name-${i + j}')`);
          }
          
          const insertQuery = `
            INSERT INTO ${tableName} (id, value, name) VALUES ${valuesBatch.join(',')}
          `;
          await queryService.executeQuery(insertQuery);
        }
        
        // Ahora crear el índice específico
        const createIndexQuery = `
          CREATE INDEX idx_${indexType.toLowerCase()}_${tableName} 
          ON ${tableName}(value)
        `;
        await queryService.executeQuery(createIndexQuery);
        
        const createEndTime = performance.now();
        const createTime = createEndTime - createStartTime;
        
        // Ejecutar consulta para medir rendimiento
        const queryStartTime = performance.now();
        await queryService.executeQuery(`SELECT * FROM ${tableName} WHERE value < 500`);
        const queryEndTime = performance.now();
        const queryTime = queryEndTime - queryStartTime;
        
        // Simular operaciones de disco (en producción debería venir del servidor)
        const diskOps = Math.floor(Math.random() * 100) + (indexType === 'HASH' ? 10 : indexType === 'BTREE' ? 30 : 50);
        
        results.executionTimes.push({
          createTime: createTime,
          queryTime: queryTime,
          totalTime: createTime + queryTime
        });
        
        results.diskOperations.push(diskOps);
      }
      
      return results;
    } catch (err) {
      console.error("Error en comparación de índices:", err);
      throw err;
    }
  }
};

export default api;
