// src/utils/auth.js

/**
 * Utility function to make authenticated API calls
 * This is a standalone version that doesn't rely on React context
 */
export const fetchWithAuth = async (url, options = {}) => {
  const token = localStorage.getItem('authToken');
  
  const defaultOptions = {
    headers: {
      'Content-Type': 'application/json',
      ...(token && { Authorization: `Bearer ${token}` }),
      ...options.headers,
    },
    ...options,
  };

  try {
    const response = await fetch(url, defaultOptions);
    
    if (response.status === 401) {
      // Token is invalid or expired
      localStorage.removeItem('authToken');
      // Don't automatically redirect in development - just log the error
      console.warn('Authentication expired or missing. You may need to login.');
      // window.location.href = '/login'; // Commented out for development
    }
    
    // If response is not ok, try to get error details
    if (!response.ok) {
      let errorMessage = `HTTP ${response.status}: ${response.statusText}`;
      try {
        const errorData = await response.json();
        if (errorData.detail) {
          errorMessage = `${errorMessage}\n\nDetails: ${errorData.detail}`;
        }
        if (errorData.traceback) {
          errorMessage = `${errorMessage}\n\nTraceback:\n${errorData.traceback}`;
        }
      } catch (jsonError) {
        // If we can't parse JSON, just use the status text
        console.warn('Could not parse error response as JSON:', jsonError);
      }
      throw new Error(errorMessage);
    }
    
    return response;
  } catch (error) {
    if (error.message.includes('Authentication expired')) {
      localStorage.removeItem('authToken');
      // Don't redirect in development
      console.warn('Authentication error:', error.message);
    }
    throw error;
  }
};

/**
 * Get the current auth token from localStorage
 */
export const getAuthToken = () => {
  return localStorage.getItem('authToken');
};

/**
 * Check if user is authenticated
 */
export const isAuthenticated = () => {
  return !!localStorage.getItem('authToken');
};

/**
 * Login helper
 */
export const login = (token) => {
  localStorage.setItem('authToken', token);
};

/**
 * Logout helper
 */
export const logout = () => {
  localStorage.removeItem('authToken');
  window.location.href = '/login';
};
