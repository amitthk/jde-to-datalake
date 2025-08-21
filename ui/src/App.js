// src/App.js

import React from 'react';
import { BrowserRouter as Router, Route, Routes, Link } from 'react-router-dom';
import './App.css';
import LiveDataComparison from './components/LiveDataComparison';
import JdeItemMasterReview from './components/JdeItemMasterReview';
import BakeryOpsToJde from './components/BakeryOpsToJde';
import BakeryOpsData from './components/BakeryOpsData';
import S3DataManager from './components/S3DataManager';
import BatchReview from './components/BatchReview';
import AdvancedPatchForm from './components/AdvancedPatchForm';
import Login from './components/Login';
import { AuthProvider, useAuth } from './context/AuthContext';

function AuthenticatedApp() {
  const { isAuthenticated, logout, loading } = useAuth();

  if (loading) {
    return (
      <div className="d-flex justify-content-center align-items-center min-vh-100">
        <div className="spinner-border" role="status">
          <span className="visually-hidden">Loading...</span>
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return <Login onLogin={() => window.location.reload()} />;
  }

  return (
    <Router>
      <div className="App">
        <header className="bg-success text-white p-3">
          <div className="d-flex justify-content-between align-items-center">
            <h1>
              <img src="/logo.png" alt="Logo" style={{ width: '180px', marginRight: '10px' }} />
              JDE Data Ingestion - Bakery Operations
            </h1>
            <button className="btn btn-outline-light" onClick={logout}>
              Logout
            </button>
          </div>
        </header>

        <nav className="navbar navbar-expand-lg navbar-light bg-light mt-3">
          <div className="container-fluid">
            <Link to="/" className="navbar-brand">Dashboard</Link>
            <button className="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav" aria-controls="navbarNav" aria-expanded="false" aria-label="Toggle navigation">
              <span className="navbar-toggler-icon"></span>
            </button>
            <div className="collapse navbar-collapse" id="navbarNav">
              <ul className="navbar-nav">
                <li className="nav-item">
                  <Link to="/live-comparison" className="nav-link">JDE Cardex Review</Link>
                </li>
                <li className="nav-item">
                  <Link to="/item-master-review" className="nav-link">JDE Item Master Review</Link>
                </li>
                <li className="nav-item">
                  <Link to="/bakery-ops-to-jde" className="nav-link">Bakery Ops to JDE</Link>
                </li>
                <li className="nav-item">
                  <Link to="/bakery-ops-data" className="nav-link">Bakery Ops Data</Link>
                </li>
                <li className="nav-item">
                  <Link to="/s3-data-manager" className="nav-link">S3 Data Lake</Link>
                </li>
                <li className="nav-item">
                  <Link to="/advanced-patch" className="nav-link">Advanced Patching</Link>
                </li>
              </ul>
            </div>
          </div>
        </nav>

        <Routes>
          <Route path="/" element={<LiveDataComparison />} />
          <Route path="/live-comparison" element={<LiveDataComparison />} />
          <Route path="/item-master-review" element={<JdeItemMasterReview />} />
          <Route path="/bakery-ops-to-jde" element={<BakeryOpsToJde />} />
          <Route path="/bakery-ops-data" element={<BakeryOpsData />} />
          <Route path="/s3-data-manager" element={<S3DataManager />} />
          <Route path="/batch-review/:sessionId" element={<BatchReview />} />
          <Route path="/advanced-patch" element={<AdvancedPatchForm />} />
        </Routes>
      </div>
    </Router>
  );
}

function App() {
  return (
    <AuthProvider>
      <AuthenticatedApp />
    </AuthProvider>
  );
}

export default App;
