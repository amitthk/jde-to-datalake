// src/components/S3DataManager.js
import React, { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config/api';
import { fetchWithAuth } from '../utils/auth';

const S3DataManager = () => {
    const [dispatches, setDispatches] = useState([]);
    const [schemas, setSchemas] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [activeTab, setActiveTab] = useState('dispatches');
    const [filters, setFilters] = useState({
        dispatchType: '',
        startDate: '',
        endDate: ''
    });

    useEffect(() => {
        if (activeTab === 'dispatches') {
            fetchDispatches();
        } else if (activeTab === 'schemas') {
            fetchSchemas();
        }
    }, [activeTab, filters]);

    const fetchDispatches = async () => {
        setLoading(true);
        setError(null);
        
        try {
            const params = new URLSearchParams();
            if (filters.dispatchType) params.append('dispatch_type', filters.dispatchType);
            if (filters.startDate) params.append('start_date', filters.startDate);
            if (filters.endDate) params.append('end_date', filters.endDate);
            
            const response = await fetchWithAuth(`${API_BASE_URL}/s3/dispatches?${params}`);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            
            const result = await response.json();
            setDispatches(result.dispatches || []);
        } catch (err) {
            console.error('Error fetching S3 dispatches:', err);
            setError(`Failed to fetch dispatches: ${err.message}`);
            setDispatches([]);
        } finally {
            setLoading(false);
        }
    };

    const fetchSchemas = async () => {
        setLoading(true);
        setError(null);
        
        try {
            const response = await fetchWithAuth(`${API_BASE_URL}/s3/schemas`);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            
            const result = await response.json();
            setSchemas(result.schemas || []);
        } catch (err) {
            console.error('Error fetching schemas:', err);
            setError(`Failed to fetch schemas: ${err.message}`);
            setSchemas([]);
        } finally {
            setLoading(false);
        }
    };

    const downloadDispatch = async (s3Key) => {
        try {
            const response = await fetchWithAuth(`${API_BASE_URL}/s3/download/${encodeURIComponent(s3Key)}`);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = s3Key.split('/').pop();
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (err) {
            console.error('Error downloading dispatch:', err);
            alert(`Failed to download: ${err.message}`);
        }
    };

    const formatFileSize = (bytes) => {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    };

    const formatDate = (dateStr) => {
        try {
            return new Date(dateStr).toLocaleDateString('en-US', {
                year: 'numeric',
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        } catch {
            return dateStr;
        }
    };

    const getDispatchTypeColor = (type) => {
        const colors = {
            'to_bakery_ops': 'primary',
            'from_bakery_ops': 'success',
            'cardex_changes': 'info',
            'bakery_ops_products': 'warning',
            'bakery_ops_movements': 'secondary'
        };
        return colors[type] || 'dark';
    };

    return (
        <div className="container-fluid mt-4">
            <div className="row">
                <div className="col-12">
                    <div className="card shadow">
                        <div className="card-header bg-dark text-white">
                            <h3 className="card-title mb-0">
                                <i className="fas fa-cloud me-2"></i>
                                S3 Data Lake Manager
                            </h3>
                            <p className="card-text mt-2 mb-0">
                                Monitor and manage data stored in the S3 data lake, including dispatches and schema versions
                            </p>
                        </div>

                        <div className="card-body">
                            {/* Navigation Tabs */}
                            <ul className="nav nav-tabs mb-4">
                                <li className="nav-item">
                                    <button 
                                        className={`nav-link ${activeTab === 'dispatches' ? 'active' : ''}`}
                                        onClick={() => setActiveTab('dispatches')}
                                    >
                                        <i className="fas fa-database me-1"></i>
                                        Data Dispatches
                                    </button>
                                </li>
                                <li className="nav-item">
                                    <button 
                                        className={`nav-link ${activeTab === 'schemas' ? 'active' : ''}`}
                                        onClick={() => setActiveTab('schemas')}
                                    >
                                        <i className="fas fa-code me-1"></i>
                                        Schema Versions
                                    </button>
                                </li>
                                <li className="nav-item">
                                    <button 
                                        className={`nav-link ${activeTab === 'analytics' ? 'active' : ''}`}
                                        onClick={() => setActiveTab('analytics')}
                                    >
                                        <i className="fas fa-chart-bar me-1"></i>
                                        Analytics
                                    </button>
                                </li>
                            </ul>

                            {/* Dispatches Tab */}
                            {activeTab === 'dispatches' && (
                                <>
                                    {/* Filters */}
                                    <div className="row mb-4">
                                        <div className="col-md-12">
                                            <div className="card">
                                                <div className="card-header">
                                                    <h6>Filters</h6>
                                                </div>
                                                <div className="card-body">
                                                    <div className="row">
                                                        <div className="col-md-4">
                                                            <label className="form-label">Dispatch Type</label>
                                                            <select 
                                                                className="form-select"
                                                                value={filters.dispatchType}
                                                                onChange={(e) => setFilters({...filters, dispatchType: e.target.value})}
                                                            >
                                                                <option value="">All Types</option>
                                                                <option value="to_bakery_ops">To Bakery Ops</option>
                                                                <option value="from_bakery_ops">From Bakery Ops</option>
                                                                <option value="cardex_changes">Cardex Changes</option>
                                                                <option value="bakery_ops_products">Product Data</option>
                                                                <option value="bakery_ops_movements">Movement Data</option>
                                                            </select>
                                                        </div>
                                                        <div className="col-md-3">
                                                            <label className="form-label">Start Date</label>
                                                            <input 
                                                                type="date"
                                                                className="form-control"
                                                                value={filters.startDate}
                                                                onChange={(e) => setFilters({...filters, startDate: e.target.value})}
                                                            />
                                                        </div>
                                                        <div className="col-md-3">
                                                            <label className="form-label">End Date</label>
                                                            <input 
                                                                type="date"
                                                                className="form-control"
                                                                value={filters.endDate}
                                                                onChange={(e) => setFilters({...filters, endDate: e.target.value})}
                                                            />
                                                        </div>
                                                        <div className="col-md-2 d-flex align-items-end">
                                                            <button 
                                                                className="btn btn-primary w-100"
                                                                onClick={fetchDispatches}
                                                                disabled={loading}
                                                            >
                                                                <i className="fas fa-search me-1"></i>
                                                                Search
                                                            </button>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    </div>

                                    {loading ? (
                                        <div className="text-center py-5">
                                            <div className="spinner-border text-primary" role="status">
                                                <span className="visually-hidden">Loading...</span>
                                            </div>
                                            <p className="mt-2">Loading dispatch data...</p>
                                        </div>
                                    ) : error ? (
                                        <div className="alert alert-danger">
                                            <h6>Error Loading Data</h6>
                                            <p>{error}</p>
                                        </div>
                                    ) : (
                                        <div className="table-responsive">
                                            <table className="table table-striped table-hover">
                                                <thead className="table-dark">
                                                    <tr>
                                                        <th>S3 Key</th>
                                                        <th>Type</th>
                                                        <th>Date</th>
                                                        <th>Size</th>
                                                        <th>Last Modified</th>
                                                        <th>Actions</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {dispatches.length > 0 ? (
                                                        dispatches.map((dispatch, index) => (
                                                            <tr key={dispatch.key || index}>
                                                                <td>
                                                                    <code className="small">{dispatch.key}</code>
                                                                </td>
                                                                <td>
                                                                    <span className={`badge bg-${getDispatchTypeColor(dispatch.dispatch_type)}`}>
                                                                        {dispatch.dispatch_type?.replace(/_/g, ' ').toUpperCase()}
                                                                    </span>
                                                                </td>
                                                                <td>{dispatch.date}</td>
                                                                <td>{formatFileSize(dispatch.size)}</td>
                                                                <td>{formatDate(dispatch.last_modified)}</td>
                                                                <td>
                                                                    <div className="btn-group" role="group">
                                                                        <button 
                                                                            className="btn btn-sm btn-outline-primary"
                                                                            onClick={() => downloadDispatch(dispatch.key)}
                                                                            title="Download"
                                                                        >
                                                                            <i className="fas fa-download"></i>
                                                                        </button>
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                        ))
                                                    ) : (
                                                        <tr>
                                                            <td colSpan="6" className="text-center text-muted py-4">
                                                                <i className="fas fa-info-circle me-2"></i>
                                                                No dispatches found with current filters
                                                            </td>
                                                        </tr>
                                                    )}
                                                </tbody>
                                            </table>
                                        </div>
                                    )}
                                </>
                            )}

                            {/* Schemas Tab */}
                            {activeTab === 'schemas' && (
                                <>
                                    {loading ? (
                                        <div className="text-center py-5">
                                            <div className="spinner-border text-primary" role="status">
                                                <span className="visually-hidden">Loading...</span>
                                            </div>
                                            <p className="mt-2">Loading schema data...</p>
                                        </div>
                                    ) : error ? (
                                        <div className="alert alert-danger">
                                            <h6>Error Loading Schemas</h6>
                                            <p>{error}</p>
                                        </div>
                                    ) : (
                                        <div className="row">
                                            {schemas.length > 0 ? (
                                                schemas.map((schema, index) => (
                                                    <div key={index} className="col-md-6 mb-4">
                                                        <div className="card">
                                                            <div className="card-header">
                                                                <h6>{schema.table_name}</h6>
                                                                <small className="text-muted">Version {schema.version}</small>
                                                            </div>
                                                            <div className="card-body">
                                                                <p><strong>Created:</strong> {formatDate(schema.created_at)}</p>
                                                                <p><strong>Fields:</strong> {Object.keys(schema.schema.fields || {}).length}</p>
                                                                <details className="small">
                                                                    <summary>Schema Details</summary>
                                                                    <pre className="mt-2 bg-light p-2 rounded">
                                                                        {JSON.stringify(schema.schema, null, 2)}
                                                                    </pre>
                                                                </details>
                                                            </div>
                                                        </div>
                                                    </div>
                                                ))
                                            ) : (
                                                <div className="col-12">
                                                    <div className="text-center text-muted py-5">
                                                        <i className="fas fa-info-circle me-2"></i>
                                                        No schemas found
                                                    </div>
                                                </div>
                                            )}
                                        </div>
                                    )}
                                </>
                            )}

                            {/* Analytics Tab */}
                            {activeTab === 'analytics' && (
                                <div className="row">
                                    <div className="col-12">
                                        <div className="alert alert-info">
                                            <h6>Analytics Dashboard</h6>
                                            <p>Coming soon: Data volume trends, dispatch success rates, and schema evolution analytics.</p>
                                        </div>
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default S3DataManager;
