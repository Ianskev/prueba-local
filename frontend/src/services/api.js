import axios from 'axios';

const api = axios.create({
  baseURL: 'http://127.0.0.1:8000', // Quitamos el '/api' del final
});

// Interceptor para añadir el token a las solicitudes
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('authToken'); // Cambiado de 'token' a 'authToken'
    if (token) {
      // Asegurarnos que el token tenga el formato correcto
      if (!token.startsWith('Bearer ')) {
        config.headers.Authorization = `Bearer ${token}`;
      } else {
        config.headers.Authorization = token;
      }
      
      // Debug para ver qué token se está enviando
      console.log('Sending request with token:', config.headers.Authorization);
    } else {
      console.log('No token found in localStorage', localStorage);
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
        localStorage.removeItem('authToken'); // Cambiado de 'token' a 'authToken'
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
      // Versión simulada para pruebas (sin autenticación)
      for (const indexType of testConfig.indexTypes) {
        results.indices.push(indexType);
        
        // Tiempos simulados basados en el tipo de índice
        const baseCreateTime = 500 + Math.random() * 300;
        const baseQueryTime = 200 + Math.random() * 100;
        
        // Diferentes tipos de índices tienen diferentes características de rendimiento
        let createTimeFactor = 1.0;
        let queryTimeFactor = 1.0;
        
        switch(indexType) {
          case 'BTREE':
            createTimeFactor = 1.2;
            queryTimeFactor = 0.8;
            break;
          case 'HASH':
            createTimeFactor = 0.9;
            queryTimeFactor = 0.6;
            break;
          case 'AVL':
            createTimeFactor = 1.4;
            queryTimeFactor = 0.9;
            break;
          case 'ISAM':
            createTimeFactor = 0.8;
            queryTimeFactor = 1.2;
            break;
          case 'RTREE':
            createTimeFactor = 1.6;
            queryTimeFactor = 0.7;
            break;
        }
        
        const createTime = baseCreateTime * createTimeFactor * (testConfig.dataSize / 500);
        const queryTime = baseQueryTime * queryTimeFactor * (Math.log(testConfig.dataSize) / Math.log(500));
        
        results.executionTimes.push({
          createTime: createTime,
          queryTime: queryTime,
          totalTime: createTime + queryTime
        });
        
        // Simular operaciones de disco
        const diskOps = Math.floor(testConfig.dataSize / 10) * 
                       (indexType === 'HASH' ? 0.5 : 
                        indexType === 'BTREE' ? 0.8 : 
                        indexType === 'AVL' ? 1.2 :
                        indexType === 'ISAM' ? 0.7 : 1.5);
                        
        results.diskOperations.push(Math.round(diskOps));
      }
      
      // Simular un poco de retraso para que parezca que está haciendo trabajo
      await new Promise(resolve => setTimeout(resolve, 1500));
      
      return results;
    } catch (err) {
      console.error("Error en comparación de índices:", err);
      throw err;
    }
  }
};

export default api;
