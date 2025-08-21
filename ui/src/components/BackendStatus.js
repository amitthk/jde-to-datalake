// src/components/BackendStatus.js

import React, { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config/api';

function BackendStatus() {
  const [status, setStatus] = useState({ healthy: false, message: '', testing: true });

  useEffect(() => {
    const checkBackend = async () => {
      console.log('=== Backend Health Check Debug ===');
      console.log('API_BASE_URL:', API_BASE_URL);
      console.log('Full health check URL:', `${API_BASE_URL}/health`);
      console.log('Window location:', window.location.href);
      console.log('User agent:', navigator.userAgent);
      
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
        
        console.log('Making fetch request...');
        const response = await fetch(`${API_BASE_URL}/health`, {
          signal: controller.signal,
          method: 'GET',
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
          }
        });
        
        clearTimeout(timeoutId);
        console.log('Response received:', {
          status: response.status,
          statusText: response.statusText,
          headers: Object.fromEntries(response.headers.entries())
        });
        
        if (response.ok) {
          const data = await response.json();
          console.log('Response data:', data);
          setStatus({ healthy: true, message: data.message, testing: false });
        } else {
          const errorText = await response.text();
          console.log('Error response text:', errorText);
          setStatus({ healthy: false, message: `Backend error: ${response.status} - ${response.statusText}`, testing: false });
        }
      } catch (error) {
        console.error('Fetch error details:', {
          name: error.name,
          message: error.message,
          stack: error.stack
        });
        
        let errorMessage = `Connection error: ${error.message}`;
        if (error.name === 'AbortError') {
          errorMessage = 'Request timeout (10s) - Backend may be slow or unreachable';
        } else if (error.message.includes('NetworkError') || error.message.includes('Failed to fetch')) {
          errorMessage = 'Network error - Check if backend is running and accessible';
        }
        
        setStatus({ healthy: false, message: errorMessage, testing: false });
      }
      console.log('=== Health Check Complete ===');
    };

    checkBackend();
  }, []);

  if (status.testing) {
    return (
      <div className="alert alert-info">
        <div className="d-flex align-items-center">
          <div className="spinner-border spinner-border-sm me-2" role="status">
            <span className="visually-hidden">Loading...</span>
          </div>
          Checking backend status...
        </div>
      </div>
    );
  }

  return (
    <div className={`alert ${status.healthy ? 'alert-success' : 'alert-danger'}`}>
      <strong>Backend Status:</strong> {status.message}
      {!status.healthy && (
        <div className="mt-2">
          <small>
            <strong>Backend URL:</strong> {API_BASE_URL}<br/>
            <strong>Health Check:</strong> {API_BASE_URL}/health<br/>
            <strong>Debugging:</strong> Open browser dev tools (F12) → Console tab for detailed logs
          </small>
        </div>
      )}
    </div>
  );
}

export default BackendStatus;
