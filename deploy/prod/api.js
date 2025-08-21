// Production API configuration
// This file replaces src/config/api.js during production build

// Production backend configuration (configurable)
const PROD_BACKEND_HOST = process.env.REACT_APP_BACKEND_HOST || 'localhost';
const PROD_BACKEND_PORT = process.env.REACT_APP_BACKEND_PORT || '9998';

// Production backend URL - dynamically determined at runtime
const getApiBaseUrl = () => {
  // First check for runtime configuration override
  if (typeof window !== 'undefined' && window.STICAL_CONFIG && window.STICAL_CONFIG.BACKEND_URL) {
    console.log('Using runtime config URL:', window.STICAL_CONFIG.BACKEND_URL);
    return window.STICAL_CONFIG.BACKEND_URL;
  }

  // Then check for explicit environment variable
  if (process.env.REACT_APP_BACKEND_URL) {
    console.log('Using env variable URL:', process.env.REACT_APP_BACKEND_URL);
    return process.env.REACT_APP_BACKEND_URL;
  }

  // Use build-time configuration
  const buildTimeUrl = `http://${PROD_BACKEND_HOST}:${PROD_BACKEND_PORT}`;
  console.log('Using build-time URL:', buildTimeUrl);
  
  // Only determine dynamically if we're in a browser environment and no explicit config
  if (typeof window !== 'undefined' && window.location && PROD_BACKEND_HOST === 'localhost') {
    const protocol = window.location.protocol;
    const hostname = window.location.hostname;
    
    console.log('Browser environment detected:', { protocol, hostname });
    
    // Use the browser's hostname if we're using default localhost
    if (hostname === 'localhost' || hostname === '127.0.0.1') {
      const dynamicUrl = `${protocol}//${hostname}:${PROD_BACKEND_PORT}`;
      console.log('Using dynamic localhost URL:', dynamicUrl);
      return dynamicUrl;
    }
    
    // For production servers, use the same hostname
    const dynamicUrl = `${protocol}//${hostname}:${PROD_BACKEND_PORT}`;
    console.log('Using dynamic server URL:', dynamicUrl);
    return dynamicUrl;
  }
  
  // Fallback to build-time configuration
  console.log('Using fallback URL:', buildTimeUrl);
  return buildTimeUrl;
};

const API_BASE_URL = getApiBaseUrl();

console.log('=== API Configuration Debug ===');
console.log('PROD_BACKEND_HOST:', PROD_BACKEND_HOST);
console.log('PROD_BACKEND_PORT:', PROD_BACKEND_PORT);
console.log('Final API_BASE_URL:', API_BASE_URL);
console.log('process.env.REACT_APP_BACKEND_URL:', process.env.REACT_APP_BACKEND_URL);
console.log('process.env.REACT_APP_BACKEND_HOST:', process.env.REACT_APP_BACKEND_HOST);
console.log('window.STICAL_CONFIG:', typeof window !== 'undefined' ? window.STICAL_CONFIG : 'not available');
console.log('===============================');

export { API_BASE_URL };
