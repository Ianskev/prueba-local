/**
 * Utilidad para depurar problemas con JWT
 */

// Verifica si el token es un JWT válido
export const isValidJWT = (token) => {
  if (!token) return false;
  
  // Eliminar 'Bearer ' si está presente
  if (token.startsWith('Bearer ')) {
    token = token.substring(7);
  }
  
  // Verificar estructura básica (tres partes separadas por puntos)
  const parts = token.split('.');
  if (parts.length !== 3) return false;
  
  try {
    // Verificar que las primeras dos partes sean JSON válido en base64
    atob(parts[0]);
    atob(parts[1]);
    return true;
  } catch (e) {
    return false;
  }
};

// Decodifica un JWT para ver su contenido
export const decodeJWT = (token) => {
  if (!token) return null;
  
  // Eliminar 'Bearer ' si está presente
  if (token.startsWith('Bearer ')) {
    token = token.substring(7);
  }
  
  try {
    const base64Url = token.split('.')[1];
    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
    return JSON.parse(window.atob(base64));
  } catch (e) {
    console.error('Error decodificando token:', e);
    return null;
  }
};

// Imprime información útil sobre el token actual
export const debugToken = () => {
  const token = localStorage.getItem('token');
  console.log('Token en localStorage:', token);
  
  if (token) {
    console.log('¿Es un JWT válido?', isValidJWT(token));
    const decoded = decodeJWT(token);
    console.log('Contenido decodificado:', decoded);
    
    if (decoded && decoded.exp) {
      const expDate = new Date(decoded.exp * 1000);
      const now = new Date();
      console.log('Expira:', expDate);
      console.log('¿Expirado?', expDate < now);
    }
  }
};
