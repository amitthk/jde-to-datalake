// src/components/BakeryOpsToJde.js
import React, { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config/api';
import { fetchWithAuth } from '../utils/auth';

const BakeryOpsToJde = () => {
    const [actionData, setActionData] = useState([]);
    const [filteredData, setFilteredData] = useState([]);
    const [daysBack, setDaysBack] = useState(2);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [selectedBatches, setSelectedBatches] = useState([]);
    const [selectAll, setSelectAll] = useState(false);
    const [currentBatchForReview, setCurrentBatchForReview] = useState(null);
    const [jdePayload, setJdePayload] = useState(null);
    const [showModal, setShowModal] = useState(false);
    const [isEditing, setIsEditing] = useState(false);

    useEffect(() => {
        fetchActionData();
    }, [daysBack]);

    const fetchActionData = async () => {
        setLoading(true);
        setError(null);
        
        try {
            const response = await fetchWithAuth(`${API_BASE_URL}/data/bakery_ops_to_jde_actions?days_back=${daysBack}`);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const result = await response.json();
            
            if (result.data && Array.isArray(result.data)) {
                setActionData(result.data);
                setFilteredData(result.data);
            } else {
                setActionData([]);
                setFilteredData([]);
                console.warn('No valid data received:', result);
            }
        } catch (err) {
            console.error('Error fetching bakery ops data:', err);
            setError(`Failed to fetch data: ${err.message}`);
            setActionData([]);
            setFilteredData([]);
        } finally {
            setLoading(false);
        }
    };

    const handleBatchSelection = (batchKey) => {
        setSelectedBatches(prev => {
            if (prev.includes(batchKey)) {
                return prev.filter(key => key !== batchKey);
            } else {
                return [...prev, batchKey];
            }
        });
    };

    const handleSelectAll = () => {
        if (selectAll) {
            setSelectedBatches([]);
        } else {
            const availableBatches = filteredData
                .filter(batch => !batch.already_dispatched)
                .map(batch => batch.key);
            setSelectedBatches(availableBatches);
        }
        setSelectAll(!selectAll);
    };

    const handleShowBatchReview = async (batch) => {
        try {
            const response = await fetchWithAuth(`${API_BASE_URL}/bakery_ops_to_jde/prepare_payload`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify([batch.key])
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const result = await response.json();
            if (result.prepared_batches && result.prepared_batches.length > 0) {
                setCurrentBatchForReview({...batch, ...result.prepared_batches[0]});
                setJdePayload(result.prepared_batches[0]);
                setShowModal(true);
            }
        } catch (error) {
            console.error('Error preparing batch for review:', error);
        }
    };

    const handleDispatchSelected = async () => {
        if (selectedBatches.length === 0) {
            alert('Please select at least one batch to dispatch.');
            return;
        }

        if (!window.confirm(`Are you sure you want to dispatch ${selectedBatches.length} selected batch(es) to JDE?`)) {
            return;
        }

        try {
            const response = await fetchWithAuth(`${API_BASE_URL}/bakery_ops_to_jde/dispatch`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(selectedBatches)
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const result = await response.json();
            
            let successMessage = `✅ Dispatch Results:\n`;
            successMessage += `• Total Processed: ${result.total_processed || 0}\n`;
            successMessage += `• Successful: ${result.successful || 0}\n`;
            successMessage += `• Failed: ${result.failed || 0}\n`;
            
            if (result.results && Array.isArray(result.results)) {
                const successfulDispatches = result.results.filter(r => r.success);
                if (successfulDispatches.length > 0) {
                    successMessage += `\n📋 Successfully Dispatched Batches:\n`;
                    successfulDispatches.forEach(r => {
                        successMessage += `• Batch ${r.batch_id}: ${r.jde_transaction_id || 'Success'}\n`;
                    });
                }
                
                const failedDispatches = result.results.filter(r => !r.success);
                if (failedDispatches.length > 0) {
                    successMessage += `\n❌ Failed Dispatches:\n`;
                    failedDispatches.forEach(r => {
                        successMessage += `• Batch ${r.batch_id}: ${r.error || 'Unknown error'}\n`;
                    });
                }
            }
            
            alert(successMessage);
            
            // Clear selections and refresh data
            setSelectedBatches([]);
            setSelectAll(false);
            await fetchActionData();
            
        } catch (error) {
            console.error('Error dispatching batches:', error);
            alert(`❌ Error dispatching batches: ${error.message}`);
        }
    };

    const formatDate = (dateStr) => {
        if (!dateStr) return 'N/A';
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

    const formatQuantity = (quantity) => {
        if (quantity === null || quantity === undefined) return 'N/A';
        return parseFloat(quantity).toLocaleString('en-US', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 4
        });
    };

    if (loading) {
        return (
            <div className="container mt-4">
                <div className="text-center">
                    <div className="spinner-border text-primary" role="status">
                        <span className="visually-hidden">Loading...</span>
                    </div>
                    <p className="mt-2">Loading Bakery Operations data...</p>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="container mt-4">
                <div className="alert alert-danger" role="alert">
                    <h4 className="alert-heading">Error!</h4>
                    <p>{error}</p>
                    <hr />
                    <button className="btn btn-outline-danger" onClick={fetchActionData}>
                        Try Again
                    </button>
                </div>
            </div>
        );
    }

    return (
        <div className="container-fluid mt-4">
            <div className="row">
                <div className="col-12">
                    <div className="card shadow">
                        <div className="card-header bg-primary text-white">
                            <h3 className="card-title mb-0">
                                <i className="fas fa-exchange-alt me-2"></i>
                                Bakery Operations to JDE - Batch Dispatch
                            </h3>
                            <p className="card-text mt-2 mb-0">
                                Select and review individual batches from Bakery Operations actions. Click "Show Me" or the eye icon to review JDE payload values before dispatching. 
                            </p>
                        </div>
                        
                        <div className="card-body">
                            {/* Controls Section */}
                            <div className="row mb-4">
                                <div className="col-md-6">
                                    <div className="card">
                                        <div className="card-header">
                                            <h5>Data Controls</h5>
                                        </div>
                                        <div className="card-body">
                                            <div className="row">
                                                <div className="col-md-8">
                                                    <label className="form-label">
                                                        <strong>Days to Look Back for Bakery Operations Actions</strong>
                                                    </label>
                                                    <input
                                                        type="number"
                                                        className="form-control"
                                                        value={daysBack}
                                                        onChange={(e) => setDaysBack(parseInt(e.target.value) || 1)}
                                                        min="1"
                                                        max="365"
                                                    />
                                                </div>
                                                <div className="col-md-4 d-flex align-items-end">
                                                    <button 
                                                        className="btn btn-outline-primary w-100"
                                                        onClick={fetchActionData}
                                                        disabled={loading}
                                                    >
                                                        <i className="fas fa-sync me-1"></i>
                                                        Refresh
                                                    </button>
                                                </div>
                                            </div>
                                            <small className="text-muted">
                                                Number of days back to fetch Bakery Operations actions (1-365 days)
                                            </small>
                                        </div>
                                    </div>
                                </div>
                                
                                <div className="col-md-6">
                                    <div className="card">
                                        <div className="card-header">
                                            <h5>Batch Selection</h5>
                                        </div>
                                        <div className="card-body">
                                            <div className="d-flex flex-column gap-2">
                                                <div className="form-check">
                                                    <input 
                                                        className="form-check-input" 
                                                        type="checkbox" 
                                                        id="selectAll"
                                                        checked={selectAll}
                                                        onChange={handleSelectAll}
                                                    />
                                                    <label className="form-check-label fw-bold" htmlFor="selectAll">
                                                        Select All Available Batches
                                                    </label>
                                                </div>
                                                <div className="d-flex gap-2">
                                                    <span className="badge bg-info">
                                                        Selected: {selectedBatches.length}
                                                    </span>
                                                    <span className="badge bg-secondary">
                                                        Total Available: {filteredData.filter(batch => !batch.already_dispatched).length}
                                                    </span>
                                                </div>
                                                <button 
                                                    className="btn btn-success"
                                                    onClick={handleDispatchSelected}
                                                    disabled={selectedBatches.length === 0}
                                                >
                                                    <i className="fas fa-paper-plane me-1"></i>
                                                    Dispatch Selected ({selectedBatches.length})
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            {/* Data Table */}
                            <div className="table-responsive">
                                <table className="table table-sm table-striped table-hover">
                                    <thead className="table-dark">
                                        <tr>
                                            <th>Select</th>
                                            <th>Product Name</th>
                                            <th>Batch Number</th>
                                            <th>Lot Code</th>
                                            <th>Quantity</th>
                                            <th>Unit</th>
                                            <th>Vessel</th>
                                            <th>Action Date</th>
                                            <th>Status</th>
                                            <th>Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {filteredData.length > 0 ? (
                                            filteredData.map((batch, index) => (
                                                <tr key={batch.key || index}>
                                                    <td>
                                                        <input 
                                                            type="checkbox" 
                                                            className="form-check-input"
                                                            checked={selectedBatches.includes(batch.key)}
                                                            onChange={() => handleBatchSelection(batch.key)}
                                                            disabled={batch.already_dispatched}
                                                        />
                                                    </td>
                                                    <td>
                                                        <span className="fw-semibold">
                                                            {batch.product_name || 'N/A'}
                                                        </span>
                                                    </td>
                                                    <td>
                                                        <code className="text-primary">
                                                            {batch.batch_number || 'N/A'}
                                                        </code>
                                                    </td>
                                                    <td>
                                                        <code className="text-success">
                                                            {batch.lot_code || 'N/A'}
                                                        </code>
                                                    </td>
                                                    <td>
                                                        <span className="badge bg-secondary">
                                                            {formatQuantity(batch.quantity)}
                                                        </span>
                                                    </td>
                                                    <td>
                                                        <span className="badge bg-info">
                                                            {batch.unit || 'N/A'}
                                                        </span>
                                                    </td>
                                                    <td>
                                                        <small className="text-muted">
                                                            {batch.vessel_id || 'N/A'}
                                                        </small>
                                                    </td>
                                                    <td>
                                                        <small>
                                                            {formatDate(batch.action_date)}
                                                        </small>
                                                    </td>
                                                    <td>
                                                        {batch.already_dispatched ? (
                                                            <span className="badge bg-success">
                                                                <i className="fas fa-check me-1"></i>
                                                                Dispatched
                                                            </span>
                                                        ) : (
                                                            <span className="badge bg-warning">
                                                                <i className="fas fa-clock me-1"></i>
                                                                Pending
                                                            </span>
                                                        )}
                                                    </td>
                                                    <td>
                                                        <div className="btn-group" role="group">
                                                            <button 
                                                                className="btn btn-sm btn-outline-primary"
                                                                onClick={() => handleShowBatchReview(batch)}
                                                                title="Review JDE Payload"
                                                            >
                                                                <i className="fas fa-eye"></i>
                                                            </button>
                                                            {!batch.already_dispatched && (
                                                                <button 
                                                                    className="btn btn-sm btn-success"
                                                                    onClick={() => {
                                                                        setSelectedBatches([batch.key]);
                                                                        handleDispatchSelected();
                                                                    }}
                                                                    title="Dispatch This Batch"
                                                                >
                                                                    <i className="fas fa-paper-plane"></i>
                                                                </button>
                                                            )}
                                                        </div>
                                                    </td>
                                                </tr>
                                            ))
                                        ) : (
                                            <tr>
                                                <td colSpan="10" className="text-center text-muted py-4">
                                                    <i className="fas fa-info-circle me-2"></i>
                                                    No Bakery Operations data found for the last {daysBack} day(s)
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>

                            {/* Status Legend */}
                            <div className="mt-4">
                                <h6>Status Legend:</h6>
                                <div className="d-flex flex-wrap gap-3">
                                    <span className="badge bg-success ms-2">Dispatched</span> - Already processed to JDE
                                    <span className="badge bg-warning ms-2">Pending</span> - Ready for dispatch to JDE
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Batch Review Modal */}
            {showModal && currentBatchForReview && (
                <div className="modal show d-block" tabIndex="-1">
                    <div className="modal-dialog modal-xl">
                        <div className="modal-content">
                            <div className="modal-header">
                                <h5 className="modal-title">Batch Review & JDE Payload</h5>
                                <button type="button" className="btn-close" onClick={() => setShowModal(false)}></button>
                            </div>
                            <div className="modal-body">
                                <div className="row">
                                    <div className="col-md-6">
                                        <h6>Batch Information</h6>
                                        <table className="table table-sm">
                                            <tbody>
                                                <tr>
                                                    <td><strong>Product:</strong></td>
                                                    <td>{currentBatchForReview.product_name}</td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Batch Number:</strong></td>
                                                    <td>{currentBatchForReview.batch_number}</td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Lot Number:</strong></td>
                                                    <td>{currentBatchForReview.lot_number}</td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Vessel Code:</strong></td>
                                                    <td><code className="text-primary">{currentBatchForReview.vessel_code || 'N/A'}</code></td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Lot Code:</strong></td>
                                                    <td><code className="text-success">{currentBatchForReview.lot_code || 'N/A'}</code></td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Quantity:</strong></td>
                                                    <td>{formatQuantity(currentBatchForReview.quantity)} {currentBatchForReview.unit}</td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                    
                                    <div className="col-md-6">
                                        <h6>JDE Payload Preview</h6>
                                        {jdePayload && (
                                            <pre className="bg-light p-3 rounded small">
                                                {JSON.stringify(jdePayload.jde_payload, null, 2)}
                                            </pre>
                                        )}
                                    </div>
                                </div>
                            </div>
                            <div className="modal-footer">
                                <button type="button" className="btn btn-secondary" onClick={() => setShowModal(false)}>
                                    Close
                                </button>
                                {!currentBatchForReview.already_dispatched && (
                                    <button 
                                        type="button" 
                                        className="btn btn-success"
                                        onClick={() => {
                                            setSelectedBatches([currentBatchForReview.key]);
                                            setShowModal(false);
                                            handleDispatchSelected();
                                        }}
                                    >
                                        <i className="fas fa-paper-plane me-1"></i>
                                        Dispatch This Batch
                                    </button>
                                )}
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default BakeryOpsToJde;
