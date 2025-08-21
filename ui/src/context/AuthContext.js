// src/context/AuthContext.js

import React, { createContext, useContext, useState, useEffect } from 'react';

const AuthContext = createContext();

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};

export const AuthProvider = ({ children }) => {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [token, setToken] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Check if there's a token in localStorage on app load
    const savedToken = localStorage.getItem('authToken');
    if (savedToken) {
      setToken(savedToken);
      setIsAuthenticated(true);
    }
    setLoading(false);
  }, []);

  const login = (authToken) => {
    setToken(authToken);
    setIsAuthenticated(true);
    localStorage.setItem('authToken', authToken);
  };

  const logout = () => {
    setToken(null);
    setIsAuthenticated(false);
    localStorage.removeItem('authToken');
  };

  // Function to make authenticated API calls
  const fetchWithAuth = async (url, options = {}) => {
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
        logout();
        throw new Error('Authentication expired. Please login again.');
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
        logout();
      }
      throw error;
    }
  };

  const value = {
    isAuthenticated,
    token,
    login,
    logout,
    fetchWithAuth,
    loading,
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
};
