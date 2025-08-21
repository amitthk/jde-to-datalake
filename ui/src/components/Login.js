// src/components/Login.js

import React, { useState } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import { API_BASE_URL } from '../config/api';
import BackendStatus from './BackendStatus';

function Login({ onLogin }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    try {
      const response = await fetch(`${API_BASE_URL}/token`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          username: username,
          password: password,
        }),
      });

      if (response.ok) {
        const data = await response.json();
        localStorage.setItem('authToken', data.access_token);
        onLogin(data.access_token);
      } else {
        let errorMessage = `Login failed (${response.status}: ${response.statusText})`;
        try {
          const errorData = await response.json();
          if (errorData.detail) {
            errorMessage = `${errorMessage}\n\nDetails: ${errorData.detail}`;
          }
          if (errorData.traceback) {
            errorMessage = `${errorMessage}\n\nTraceback:\n${errorData.traceback}`;
          }
        } catch (jsonError) {
          console.warn('Could not parse error response as JSON:', jsonError);
        }
        setError(errorMessage);
      }
    } catch (error) {
      let errorMessage = 'Network error. Please try again.';
      if (error.message) {
        errorMessage = `${errorMessage}\n\nError: ${error.message}`;
      }
      setError(errorMessage);
      console.error('Login error:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="container-fluid d-flex justify-content-center align-items-center min-vh-100 bg-light">
      <div className="card shadow" style={{ width: '400px' }}>
        <div className="card-header bg-success text-white text-center">
          <h4 className="mb-0">
            <img src="/logo.png" alt="Logo" style={{ width: '120px', marginRight: '10px' }} />
          </h4>
          <h5>JDE ↔ Bakery-System Inventory</h5>
        </div>
        <div className="card-body">
          <BackendStatus />
          
          <form onSubmit={handleSubmit}>
            {error && (
              <div className="alert alert-danger" role="alert" style={{ whiteSpace: 'pre-line' }}>
                {error}
              </div>
            )}
            
            <div className="mb-3">
              <label htmlFor="username" className="form-label">Username</label>
              <input
                type="text"
                className="form-control"
                id="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                required
                disabled={loading}
              />
            </div>
            
            <div className="mb-3">
              <label htmlFor="password" className="form-label">Password</label>
              <input
                type="password"
                className="form-control"
                id="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                disabled={loading}
              />
            </div>
            
            <button
              type="submit"
              className="btn btn-success w-100"
              disabled={loading}
            >
              {loading ? (
                <>
                  <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                  Signing in...
                </>
              ) : (
                'Sign In'
              )}
            </button>
          </form>
        </div>
        <div className="card-footer text-muted text-center">
          <small>Please use your domain credentials</small>
        </div>
      </div>
    </div>
  );
}

export default Login;
