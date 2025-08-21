// src/config/api.js
// API configuration for production deployment

// Check if we have a custom backend URL from environment
const REACT_APP_BACKEND_URL = process.env.REACT_APP_BACKEND_URL;

let API_BASE_URL;

if (REACT_APP_BACKEND_URL) {
  // Use explicitly set backend URL
  API_BASE_URL = REACT_APP_BACKEND_URL;
} else if (process.env.NODE_ENV === 'production') {
  // In production, try to use the current hostname with port 9998
  if (typeof window !== 'undefined' && window.location) {
    API_BASE_URL = `${window.location.protocol}//${window.location.hostname}:9998`;
  } else {
    // Fallback for server-side rendering or build time
    API_BASE_URL = 'http://localhost:9998';
  }
} else {
  // Development environment
  API_BASE_URL = 'http://localhost:8000';
}

export { API_BASE_URL };
