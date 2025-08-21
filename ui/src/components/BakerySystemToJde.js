import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import ErrorModal from './ErrorModal';
import BarChart from './BarChart';
import { fetchWithAuth } from '../utils/auth';
import { API_BASE_URL } from '../config/api';

const BakerySystemToJde = () => {
    const navigate = useNavigate();
    const [batches, setBatches] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [showErrorModal, setShowErrorModal] = useState(false);
    const [selectedBatches, setSelectedBatches] = useState(new Set());
    const [editingBatch, setEditingBatch] = useState(null);
    const [dispatchingBatches, setDispatchingBatches] = useState(new Set());
    const [filter, setFilter] = useState('all'); // all, pending, dispatched
    const [confirmDispatch, setConfirmDispatch] = useState(null); // For dispatch confirmation modal
    const [jdePayload, setJdePayload] = useState(null); // For editing JDE payload
    const [showPayloadReview, setShowPayloadReview] = useState(false); // For payload review modal
    const [currentBatchForReview, setCurrentBatchForReview] = useState(null); // Current batch being reviewed
    const [daysBack, setDaysBack] = useState(3); // Default to 3 days back
    const [dispatchResult, setDispatchResult] = useState(null); // Store dispatch response
    const [showDispatchPreview, setShowDispatchPreview] = useState(false); // Show JSON preview before dispatch

    const fetchBatchData = useCallback(async () => {
        setLoading(true);
        try {
            const response = await fetchWithAuth(`${API_BASE_URL}/data/bakery_system_to_jde_actions?days_back=${daysBack}`);
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const result = await response.json();
            
            if (result.success && result.data) {
                setBatches(result.data);
            } else {
                setBatches([]);
            }
        } catch (error) {
            console.error('Error fetching batch data:', error);
            setError(error.message);
            setShowErrorModal(true);
        } finally {
            setLoading(false);
        }
    }, [daysBack]);

    // Initial load only - don't refetch when daysBack changes
    useEffect(() => {
        fetchBatchData();
    }, []); // Remove fetchBatchData dependency

    // Manual update function
    const handleUpdateData = () => {
        fetchBatchData();
    };

    const handleBatchSelection = (uniqueTransactionId) => {
        const newSelected = new Set(selectedBatches);
        if (newSelected.has(uniqueTransactionId)) {
            newSelected.delete(uniqueTransactionId);
        } else {
            newSelected.add(uniqueTransactionId);
        }
        setSelectedBatches(newSelected);
    };

    const handleEditBatch = (batch) => {
        setEditingBatch({...batch});
    };

    const handleSaveEdit = () => {
        // Update the batch in local state
        setBatches(prev => 
            prev.map(batch => 
                batch.unique_transaction_id === editingBatch.unique_transaction_id ? editingBatch : batch
            )
        );
        setEditingBatch(null);
    };

    const handleDispatchBatch = async (batch) => {
        try {
            // First, get the prepared JDE payload for review
            const response = await fetchWithAuth(
                `${API_BASE_URL}/prepare_jde_payload`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(batch)
                }
            );
            
            const result = await response.json();
            
            if (result.success) {
                // Show the payload review modal instead of alert
                setJdePayload({
                    original_batch: batch,
                    jde_payload: result.payload,
                    meta_info: {
                        converted_unit: result.converted_unit,
                        determined_bu: result.determined_bu,
                        extracted_lot_number: result.extracted_lot_number
                    }
                });
                setCurrentBatchForReview(batch);
                setShowPayloadReview(true);
            } else {
                setError(result.error || 'Failed to prepare payload');
                setShowErrorModal(true);
            }
        } catch (error) {
            console.error('Error preparing JDE payload:', error);
            setError(`Error preparing dispatch: ${error.message}`);
            setShowErrorModal(true);
        }
    };

    const handleConfirmDispatch = async () => {
        if (!jdePayload || !confirmDispatch) return;
        
        const batchKey = confirmDispatch.unique_transaction_id;
        setDispatchingBatches(prev => new Set([...prev, batchKey]));
        
        try {
            const response = await fetchWithAuth(
                `${API_BASE_URL}/dispatch/prepared_payload_to_jde`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        jde_payload: jdePayload.jde_payload,
                        batch_data: jdePayload.original_batch
                    })
                }
            );
            
            const result = await response.json();
            
            if (result.success) {
                // Mark batch as dispatched in local state
                setBatches(prev =>
                    prev.map(b =>
                        b.unique_transaction_id === confirmDispatch.unique_transaction_id ? {...b, already_dispatched: true} : b
                    )
                );
                alert(`Successfully dispatched lot ${confirmDispatch.lot_number || confirmDispatch.batch_number || confirmDispatch.unique_transaction_id}`);
            } else {
                alert(`Failed to dispatch: ${result.error}`);
            }
        } catch (error) {
            console.error('Error dispatching batch:', error);
            alert(`Error dispatching batch: ${error.message}`);
        } finally {
            setDispatchingBatches(prev => {
                const newSet = new Set(prev);
                newSet.delete(batchKey);
                return newSet;
            });
            setConfirmDispatch(null);
            setJdePayload(null);
        }
    };

    const handleUpdateJdePayload = (field, value, gridIndex = null) => {
        setJdePayload(prev => {
            const updated = {...prev};
            
            if (gridIndex !== null) {
                // Updating GridData
                updated.jde_payload.GridData[gridIndex][field] = value;
            } else {
                // Updating top-level field
                updated.jde_payload[field] = value;
            }
            
            return updated;
        });
    };

    const handleBatchMultipleDispatch = async () => {
        if (selectedBatches.size === 0) {
            alert('Please select batches to review');
            return;
        }

        const batchesToReview = batches.filter(batch => 
            selectedBatches.has(batch.unique_transaction_id) && !batch.already_dispatched
        );

        if (batchesToReview.length === 0) {
            alert('No pending batches selected');
            return;
        }

        try {
            // Create a session for batch review
            const response = await fetchWithAuth(
                `${API_BASE_URL}/batch_review/create_session`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(batchesToReview)
                }
            );

            const result = await response.json();
            
            if (result.success) {
                // Navigate to batch review page with session ID
                navigate(`/batch-review/${result.session_id}`);
            } else {
                throw new Error(result.error || 'Failed to create review session');
            }
        } catch (error) {
            console.error('Error creating session:', error);
            alert(`Failed to create review session: ${error.message}`);
        }
    };

    const getFilteredBatches = () => {
        // First filter out batches with zero or null quantities
        const nonZeroBatches = batches.filter(batch => {
            const quantity = batch.quantity;
            return quantity !== null && quantity !== undefined && quantity !== 0 && 
                   (typeof quantity !== 'string' || (quantity.trim() !== '' && quantity.trim() !== '0' && quantity.trim() !== '0.0'));
        });
        
        switch (filter) {
            case 'pending':
                return nonZeroBatches.filter(batch => !batch.already_dispatched);
            case 'dispatched':
                return nonZeroBatches.filter(batch => batch.already_dispatched);
            default:
                return nonZeroBatches;
        }
    };

    const formatDate = (dateStr) => {
        if (!dateStr) return 'N/A';
        try {
            return new Date(dateStr).toLocaleDateString();
        } catch {
            return dateStr;
        }
    };

    const formatQuantity = (quantity) => {
        if (typeof quantity === 'number') {
            return quantity.toFixed(2);
        }
        return quantity || '0.00';
    };

    if (loading) {
        return (
            <div className="d-flex justify-content-center align-items-center" style={{ minHeight: '200px' }}>
                <div className="spinner-border text-primary" role="status">
                    <span className="visually-hidden">Loading...</span>
                </div>
            </div>
        );
    }

    const filteredBatches = getFilteredBatches();
    const pendingCount = batches.filter(b => !b.already_dispatched).length;
    const dispatchedCount = batches.filter(b => b.already_dispatched).length;
    
    // Count zero quantity batches
    const zeroQuantityCount = batches.filter(batch => {
        const quantity = batch.quantity;
        return quantity === null || quantity === undefined || quantity === 0 || 
               (typeof quantity === 'string' && (quantity.trim() === '' || quantity.trim() === '0' || quantity.trim() === '0.0'));
    }).length;

    return (
        <div className="container-fluid" style={{overflowX: 'auto', minWidth: '1200px'}}>
            {/* Header Section */}
            <div className="row mb-3">
                <div className="col">
                    <h2 className="text-dark">
                        <i className="fas fa-exchange-alt me-2"></i>
                        Bakery-System to JDE - Batch Dispatch
                    </h2>
                    <p className="text-muted">
                        Select and review individual batches from Bakery-System actions. Click "Show Me" or the eye icon to review JDE payload values before dispatching. 
                        <strong> Nothing is sent to JDE without your explicit confirmation.</strong>
                    </p>
                </div>
                <div className="col-auto">
                    <button className="btn btn-primary me-2" onClick={fetchBatchData}>
                        <i className="fas fa-sync-alt me-1"></i>
                        Refresh
                    </button>
                    <button 
                        className="btn btn-info"
                        onClick={handleBatchMultipleDispatch}
                        disabled={selectedBatches.size === 0}
                    >
                        <i className="fas fa-eye me-1"></i>
                        Show Me ({selectedBatches.size})
                    </button>
                </div>
            </div>

            {/* Data Controls */}
            <div className="row mb-3">
                <div className="col">
                    <div className="card">
                        <div className="card-header bg-secondary text-white">
                            <h5 className="mb-0">
                                <i className="fas fa-cog me-2"></i>
                                Data Controls
                            </h5>
                        </div>
                        <div className="card-body">
                            <div className="row align-items-center">
                                <div className="col-md-6">
                                    <label htmlFor="daysBack" className="form-label fw-bold">
                                        <i className="fas fa-calendar-alt me-2"></i>
                                        Days to Look Back for Bakery-System Actions
                                    </label>
                                    <div className="input-group" style={{maxWidth: '300px'}}>
                                        <input
                                            type="number"
                                            className="form-control"
                                            id="daysBack"
                                            value={daysBack}
                                            onChange={(e) => setDaysBack(parseInt(e.target.value) || 1)}
                                            min="1"
                                            max="365"
                                        />
                                        <span className="input-group-text">days</span>
                                        <button 
                                            className="btn btn-primary" 
                                            type="button" 
                                            onClick={handleUpdateData}
                                            disabled={loading}
                                        >
                                            {loading ? 'Loading...' : 'Update Data'}
                                        </button>
                                    </div>
                                    <small className="text-muted">
                                        Number of days back to fetch Bakery-System actions (1-365 days)
                                    </small>
                                </div>
                                <div className="col-md-6">
                                    <div className="text-center">
                                        <button 
                                            className="btn btn-secondary btn-lg" 
                                            onClick={fetchBatchData}
                                            disabled={loading}
                                        >
                                            {loading ? (
                                                <>
                                                    <div className="spinner-border spinner-border-sm me-2" role="status"></div>
                                                    Updating...
                                                </>
                                            ) : (
                                                <>
                                                    <i className="fas fa-sync-alt me-2"></i>
                                                    Update Data
                                                </>
                                            )}
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Summary Charts */}
            <div className="row mb-3">
                <div className="col-md-8">
                    <div className="card">
                        <div className="card-header bg-secondary text-white">
                            <h5 className="mb-0">
                                <i className="fas fa-chart-bar me-2"></i>
                                Batch Summary
                            </h5>
                        </div>
                        <div className="card-body">
                            <BarChart
                                data={[
                                    {
                                        label: 'Total Batches',
                                        value: batches.length,
                                        color: '#17a2b8'
                                    },
                                    {
                                        label: 'Pending Dispatch',
                                        value: pendingCount,
                                        color: '#ffc107'
                                    },
                                    {
                                        label: 'Dispatched',
                                        value: dispatchedCount,
                                        color: '#28a745'
                                    }
                                ]}
                                title="Batch Status Overview"
                                height={200}
                            />
                        </div>
                    </div>
                </div>
                <div className="col-md-4">
                    <div className="card h-100">
                        <div className="card-header bg-secondary text-white">
                            <h5 className="mb-0">
                                <i className="fas fa-info-circle me-2"></i>
                                Quick Stats
                            </h5>
                        </div>
                        <div className="card-body d-flex flex-column justify-content-center">
                            <div className="row text-center">
                                <div className="col-12 mb-3">
                                    <h3 className="text-info mb-0">{batches.length}</h3>
                                    <small className="text-muted">Total Batches</small>
                                </div>
                                <div className="col-12 mb-3">
                                    <h3 className="text-warning mb-0">{pendingCount}</h3>
                                    <small className="text-muted">Pending</small>
                                </div>
                                <div className="col-12">
                                    <h3 className="text-success mb-0">{dispatchedCount}</h3>
                                    <small className="text-muted">Dispatched</small>
                                </div>
                                {zeroQuantityCount > 0 && (
                                    <div className="col-12 mt-3">
                                        <h5 className="text-danger mb-0">{zeroQuantityCount}</h5>
                                        <small className="text-muted">Zero Quantity (Skipped)</small>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            
            {/* Zero Quantity Warning */}
            {zeroQuantityCount > 0 && (
                <div className="row mb-3">
                    <div className="col">
                        <div className="alert alert-warning">
                            <i className="fas fa-exclamation-triangle me-2"></i>
                            <strong>Notice:</strong> {zeroQuantityCount} batch(es) with zero or null quantities have been automatically skipped as they cannot be dispatched to JDE.
                        </div>
                    </div>
                </div>
            )}

            {/* Filters */}
            <div className="row mb-3">
                <div className="col">
                    <div className="btn-group" role="group">
                        <button 
                            className={`btn ${filter === 'all' ? 'btn-primary' : 'btn-outline-primary'}`}
                            onClick={() => setFilter('all')}
                        >
                            All ({batches.length})
                        </button>
                        <button 
                            className={`btn ${filter === 'pending' ? 'btn-warning' : 'btn-outline-warning'}`}
                            onClick={() => setFilter('pending')}
                        >
                            Pending ({pendingCount})
                        </button>
                        <button 
                            className={`btn ${filter === 'dispatched' ? 'btn-success' : 'btn-outline-success'}`}
                            onClick={() => setFilter('dispatched')}
                        >
                            Dispatched ({dispatchedCount})
                        </button>
                    </div>
                </div>
            </div>

            {/* Batch Table */}
            <div className="row">
                <div className="col">
                    <div className="card">
                        <div className="card-header">
                            <h5 className="mb-0">
                                <i className="fas fa-list me-2"></i>
                                Batch Details ({filteredBatches.length})
                            </h5>
                        </div>
                        <div className="card-body">
                            {filteredBatches.length === 0 ? (
                                <div className="text-center py-4">
                                    <i className="fas fa-inbox fa-3x text-muted mb-3"></i>
                                    <h5 className="text-muted">No batches found</h5>
                                    <p className="text-muted">No batches match the current filter.</p>
                                </div>
                            ) : (
                                <div className="table-responsive">
                                    <table className="table table-striped table-hover">
                                        <thead className="table-dark">
                                            <tr>
                                                <th width="40">
                                                    <input
                                                        type="checkbox"
                                                        onChange={(e) => {
                                                            if (e.target.checked) {
                                                                setSelectedBatches(new Set(
                                                                    filteredBatches
                                                                        .filter(b => !b.already_dispatched)
                                                                        .map(b => b.unique_transaction_id)
                                                                ));
                                                            } else {
                                                                setSelectedBatches(new Set());
                                                            }
                                                        }}
                                                    />
                                                </th>
                                                <th>Ingredient</th>
                                                <th>Batch Number</th>
                                                <th>Lot Number</th>
                                                <th>Vessel Code</th>
                                                <th>Lot Code</th>
                                                <th>Quantity</th>
                                                <th>Unit</th>
                                                <th>Vessel</th>
                                                <th>Date</th>
                                                <th>Status</th>
                                                <th>Review</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {filteredBatches.map((batch) => {
                                                const batchKey = batch.unique_transaction_id;
                                                const isDispatching = dispatchingBatches.has(batchKey);
                                                
                                                return (
                                                    <tr key={batch.unique_transaction_id} className={batch.already_dispatched ? 'table-success' : ''}>
                                                        <td>
                                                            <input
                                                                type="checkbox"
                                                                checked={selectedBatches.has(batch.unique_transaction_id)}
                                                                onChange={() => handleBatchSelection(batch.unique_transaction_id)}
                                                                disabled={batch.already_dispatched}
                                                            />
                                                        </td>
                                                        <td>
                                                            <strong className="text-primary">
                                                                {batch.ingredient_name}
                                                            </strong>
                                                            <br />
                                                            <small className="text-muted">ID: {batch.ingredient_id}</small>
                                                        </td>
                                                        <td>
                                                            <code className="text-info">
                                                                {batch.batch_number || batch.unique_transaction_id}
                                                            </code>
                                                        </td>
                                                        <td>
                                                            <code className="text-warning">
                                                                {batch.lot_number || 'N/A'}
                                                            </code>
                                                        </td>
                                                        <td>
                                                            <code className="text-primary">
                                                                {batch.vessel_code || 'N/A'}
                                                            </code>
                                                        </td>
                                                        <td>
                                                            <code className="text-success">
                                                                {batch.bakery_system_lot_code || 'N/A'}
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
                                                            <div className="btn-group btn-group-sm">
                                                                <button
                                                                    className="btn btn-outline-secondary"
                                                                    onClick={() => handleEditBatch(batch)}
                                                                    disabled={batch.already_dispatched}
                                                                >
                                                                    <i className="fas fa-edit"></i>
                                                                </button>
                                                                <button
                                                                    className="btn btn-info"
                                                                    onClick={() => handleDispatchBatch(batch)}
                                                                    disabled={batch.already_dispatched || isDispatching}
                                                                    title="Review JDE payload before dispatch"
                                                                >
                                                                    {isDispatching ? (
                                                                        <i className="fas fa-spinner fa-spin"></i>
                                                                    ) : (
                                                                        <i className="fas fa-eye"></i>
                                                                    )}
                                                                </button>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                );
                                            })}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            </div>

            {/* Edit Batch Modal */}
            {editingBatch && (
                <div className="modal show d-block" style={{backgroundColor: 'rgba(0,0,0,0.5)'}}>
                    <div className="modal-dialog">
                        <div className="modal-content">
                            <div className="modal-header">
                                <h5 className="modal-title">Edit Batch Details</h5>
                                <button 
                                    type="button" 
                                    className="btn-close" 
                                    onClick={() => setEditingBatch(null)}
                                ></button>
                            </div>
                            <div className="modal-body">
                                <div className="mb-3">
                                    <label className="form-label">Ingredient Name</label>
                                    <input
                                        type="text"
                                        className="form-control"
                                        value={editingBatch.ingredient_name}
                                        onChange={(e) => setEditingBatch({...editingBatch, ingredient_name: e.target.value})}
                                    />
                                </div>
                                <div className="mb-3">
                                    <label className="form-label">Batch Number</label>
                                    <input
                                        type="text"
                                        className="form-control"
                                        value={editingBatch.batch_number}
                                        onChange={(e) => setEditingBatch({...editingBatch, batch_number: e.target.value})}
                                    />
                                </div>
                                <div className="mb-3">
                                    <label className="form-label">Lot Number</label>
                                    <input
                                        type="text"
                                        className="form-control"
                                        value={editingBatch.lot_number}
                                        onChange={(e) => setEditingBatch({...editingBatch, lot_number: e.target.value})}
                                    />
                                </div>
                                <div className="row">
                                    <div className="col-md-6">
                                        <div className="mb-3">
                                            <label className="form-label">Quantity</label>
                                            <input
                                                type="number"
                                                step="0.01"
                                                className="form-control"
                                                value={editingBatch.quantity}
                                                onChange={(e) => setEditingBatch({...editingBatch, quantity: parseFloat(e.target.value)})}
                                            />
                                        </div>
                                    </div>
                                    <div className="col-md-6">
                                        <div className="mb-3">
                                            <label className="form-label">Unit</label>
                                            <input
                                                type="text"
                                                className="form-control"
                                                value={editingBatch.unit}
                                                onChange={(e) => setEditingBatch({...editingBatch, unit: e.target.value})}
                                            />
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div className="modal-footer">
                                <button 
                                    type="button" 
                                    className="btn btn-secondary" 
                                    onClick={() => setEditingBatch(null)}
                                >
                                    Cancel
                                </button>
                                <button 
                                    type="button" 
                                    className="btn btn-primary" 
                                    onClick={handleSaveEdit}
                                >
                                    Save Changes
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* JDE Dispatch Confirmation Modal */}
            {jdePayload && (
                <div className="modal show d-block" style={{backgroundColor: 'rgba(0,0,0,0.7)'}}>
                    <div className="modal-dialog modal-xl">
                        <div className="modal-content">
                            <div className="modal-header bg-warning text-dark">
                                <h5 className="modal-title">
                                    <i className="fas fa-exclamation-triangle me-2"></i>
                                    Confirm JDE Dispatch - Review All Values
                                </h5>
                                <button 
                                    type="button" 
                                    className="btn-close" 
                                    onClick={() => {
                                        setConfirmDispatch(null);
                                        setJdePayload(null);
                                    }}
                                ></button>
                            </div>
                            <div className="modal-body">
                                <div className="alert alert-warning">
                                    <strong>⚠️ REVIEW ONLY - NO AUTOMATIC DISPATCH:</strong> Please review all values below. 
                                    All fields are editable if corrections are needed. Nothing will be sent to JDE until you click the final confirmation button.
                                </div>
                                
                                <div className="row mb-4">
                                    <div className="col-md-6">
                                        <h6 className="text-primary">Original Batch Info:</h6>
                                        <ul className="list-unstyled">
                                            <li><strong>Batch ID:</strong> {jdePayload.original_batch.unique_transaction_id}</li>
                                            <li><strong>Ingredient:</strong> {jdePayload.original_batch.ingredient_name}</li>
                                            <li><strong>Action ID:</strong> {jdePayload.original_batch.action_id}</li>
                                        </ul>
                                    </div>
                                    <div className="col-md-6">
                                        <h6 className="text-info">Processing Info:</h6>
                                        <ul className="list-unstyled">
                                            <li><strong>Business Unit:</strong> {jdePayload.meta_info.determined_bu}</li>
                                            <li><strong>Unit Conversion:</strong> {jdePayload.original_batch.unit} → {jdePayload.meta_info.converted_unit}</li>
                                            <li><strong>Lot Number:</strong> {jdePayload.meta_info.extracted_lot_number}</li>
                                        </ul>
                                    </div>
                                </div>

                                <h6 className="text-danger mb-3">
                                    <i className="fas fa-cog me-2"></i>
                                    JDE Payload - EDIT VALUES AS NEEDED:
                                </h6>
                                
                                <div className="row">
                                    <div className="col-md-6">
                                        <div className="mb-3">
                                            <label className="form-label"><strong>Branch Plant</strong></label>
                                            <input
                                                type="text"
                                                className="form-control"
                                                value={jdePayload.jde_payload.Branch_Plant}
                                                onChange={(e) => handleUpdateJdePayload('Branch_Plant', e.target.value)}
                                            />
                                        </div>
                                    </div>
                                    <div className="col-md-6">
                                        <div className="mb-3">
                                            <label className="form-label"><strong>Document Type</strong></label>
                                            <input
                                                type="text"
                                                className="form-control"
                                                value={jdePayload.jde_payload.Document_Type}
                                                onChange={(e) => handleUpdateJdePayload('Document_Type', e.target.value)}
                                            />
                                        </div>
                                    </div>
                                </div>

                                <div className="mb-3">
                                    <label className="form-label"><strong>Explanation</strong></label>
                                    <input
                                        type="text"
                                        className="form-control"
                                        value={jdePayload.jde_payload.Explanation}
                                        onChange={(e) => handleUpdateJdePayload('Explanation', e.target.value)}
                                    />
                                </div>

                                <div className="row">
                                    <div className="col-md-4">
                                        <div className="mb-3">
                                            <label className="form-label"><strong>Select Row</strong></label>
                                            <input
                                                type="text"
                                                className="form-control"
                                                value={jdePayload.jde_payload.Select_Row}
                                                onChange={(e) => handleUpdateJdePayload('Select_Row', e.target.value)}
                                            />
                                        </div>
                                    </div>
                                    <div className="col-md-4">
                                        <div className="mb-3">
                                            <label className="form-label"><strong>G/L Date</strong></label>
                                            <input
                                                type="text"
                                                className="form-control"
                                                value={jdePayload.jde_payload.G_L_Date}
                                                onChange={(e) => handleUpdateJdePayload('G_L_Date', e.target.value)}
                                                placeholder="DD/MM/YYYY"
                                            />
                                        </div>
                                    </div>
                                    <div className="col-md-4">
                                        <div className="mb-3">
                                            <label className="form-label"><strong>Transaction Date</strong></label>
                                            <input
                                                type="text"
                                                className="form-control"
                                                value={jdePayload.jde_payload.Transaction_Date}
                                                onChange={(e) => handleUpdateJdePayload('Transaction_Date', e.target.value)}
                                                placeholder="DD/MM/YYYY"
                                            />
                                        </div>
                                    </div>
                                </div>

                                <h6 className="text-success mb-3">
                                    <i className="fas fa-table me-2"></i>
                                    Grid Data (Line Items):
                                </h6>
                                
                                {jdePayload.jde_payload.GridData.map((gridItem, index) => (
                                    <div key={index} className="border p-3 mb-3 rounded">
                                        <h6 className="text-muted">Line Item {index + 1}</h6>
                                        <div className="row">
                                            <div className="col-md-3">
                                                <div className="mb-3">
                                                    <label className="form-label"><strong>Item Number</strong></label>
                                                    <input
                                                        type="text"
                                                        className="form-control"
                                                        value={gridItem.Item_Number}
                                                        onChange={(e) => handleUpdateJdePayload('Item_Number', e.target.value, index)}
                                                    />
                                                </div>
                                            </div>
                                            <div className="col-md-3">
                                                <div className="mb-3">
                                                    <label className="form-label"><strong>Quantity</strong></label>
                                                    <input
                                                        type="number"
                                                        step="0.01"
                                                        className="form-control"
                                                        value={gridItem.Quantity}
                                                        onChange={(e) => handleUpdateJdePayload('Quantity', e.target.value, index)}
                                                    />
                                                </div>
                                            </div>
                                            <div className="col-md-3">
                                                <div className="mb-3">
                                                    <label className="form-label"><strong>Unit of Measure (UM)</strong></label>
                                                    <input
                                                        type="text"
                                                        className="form-control"
                                                        value={gridItem.UM}
                                                        onChange={(e) => handleUpdateJdePayload('UM', e.target.value, index)}
                                                    />
                                                </div>
                                            </div>
                                            <div className="col-md-3">
                                                <div className="mb-3">
                                                    <label className="form-label"><strong>Lot Number (LOTN)</strong></label>
                                                    <input
                                                        type="text"
                                                        className="form-control"
                                                        value={gridItem.LOTN}
                                                        onChange={(e) => handleUpdateJdePayload('LOTN', e.target.value, index)}
                                                    />
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                ))}

                                <div className="alert alert-info mt-4">
                                    <h6><strong>Complete JSON Payload Preview:</strong></h6>
                                    <pre className="bg-dark text-light p-3 rounded" style={{fontSize: '0.85em'}}>
                                        {JSON.stringify(jdePayload.jde_payload, null, 2)}
                                    </pre>
                                </div>
                            </div>
                            <div className="modal-footer">
                                <button 
                                    type="button" 
                                    className="btn btn-secondary" 
                                    onClick={() => {
                                        setConfirmDispatch(null);
                                        setJdePayload(null);
                                    }}
                                >
                                    <i className="fas fa-times me-2"></i>
                                    Cancel
                                </button>
                                <button 
                                    type="button" 
                                    className="btn btn-danger btn-lg" 
                                    onClick={handleConfirmDispatch}
                                >
                                    <i className="fas fa-paper-plane me-2"></i>
                                    YES, DISPATCH TO JDE
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* JDE Payload Review Modal */}
            {showPayloadReview && jdePayload && currentBatchForReview && (
                <div className="modal show d-block" style={{backgroundColor: 'rgba(0,0,0,0.5)'}}>
                    <div className="modal-dialog modal-lg">
                        <div className="modal-content">
                            <div className="modal-header">
                                <h5 className="modal-title">Review JDE Payload</h5>
                                <button 
                                    type="button" 
                                    className="btn-close"
                                    onClick={() => {
                                        setShowPayloadReview(false);
                                        setJdePayload(null);
                                        setCurrentBatchForReview(null);
                                    }}
                                ></button>
                            </div>
                            <div className="modal-body">
                                <div className="row mb-3">
                                    <div className="col-md-6">
                                        <h6>Batch Information</h6>
                                        <table className="table table-sm">
                                            <tbody>
                                                <tr>
                                                    <td><strong>Ingredient:</strong></td>
                                                    <td>{currentBatchForReview.ingredient_name}</td>
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
                                                    <td><strong>Bakery-System Lot Code:</strong></td>
                                                    <td><code className="text-success">{currentBatchForReview.bakery_system_lot_code || 'N/A'}</code></td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Quantity:</strong></td>
                                                    <td>{currentBatchForReview.quantity} {currentBatchForReview.unit}</td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Vessel:</strong></td>
                                                    <td>{currentBatchForReview.vessel_id}</td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                    <div className="col-md-6">
                                        <h6>Processing Info</h6>
                                        <table className="table table-sm">
                                            <tbody>
                                                <tr>
                                                    <td><strong>Business Unit:</strong></td>
                                                    <td>{jdePayload.meta_info?.determined_bu}</td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Converted Unit:</strong></td>
                                                    <td>{jdePayload.meta_info?.converted_unit}</td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Lot Number:</strong></td>
                                                    <td>{jdePayload.meta_info?.extracted_lot_number}</td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                                
                                <h6>JDE Payload</h6>
                                <pre className="bg-light p-3" style={{fontSize: '12px', maxHeight: '300px', overflow: 'auto'}}>
                                    {JSON.stringify(jdePayload.jde_payload, null, 2)}
                                </pre>
                                
                                {/* Dispatch Preview */}
                                {showDispatchPreview && (
                                    <div className="mt-4">
                                        <div className="alert alert-warning">
                                            <h6><strong>⚠️ Ready to Dispatch - Final Review</strong></h6>
                                            <p>The following JSON payload will be sent to the JDE backend:</p>
                                        </div>
                                        <div className="card border-primary">
                                            <div className="card-header bg-primary text-white">
                                                <h6 className="mb-0">
                                                    <i className="fas fa-code me-2"></i>
                                                    Final JDE Payload
                                                </h6>
                                            </div>
                                            <div className="card-body">
                                                <pre className="bg-dark text-light p-3 rounded" style={{fontSize: '0.85em', maxHeight: '400px', overflow: 'auto'}}>
                                                    {JSON.stringify({
                                                        jde_payload: jdePayload.jde_payload,
                                                        batch_data: jdePayload.original_batch
                                                    }, null, 2)}
                                                </pre>
                                            </div>
                                        </div>
                                        
                                        {dispatchResult && (
                                            <div className="mt-3">
                                                <div className={`alert ${dispatchResult.success ? 'alert-success' : 'alert-danger'}`}>
                                                    <h6>
                                                        <i className={`fas ${dispatchResult.success ? 'fa-check' : 'fa-times'} me-2`}></i>
                                                        Dispatch Result
                                                    </h6>
                                                    <p><strong>Status:</strong> {dispatchResult.success ? 'Success' : 'Failed'}</p>
                                                    <p><strong>Message:</strong> {dispatchResult.message}</p>
                                                    {dispatchResult.jde_response && (
                                                        <div>
                                                            <p><strong>JDE Response:</strong></p>
                                                            <pre className="bg-light p-2" style={{fontSize: '0.8em'}}>
                                                                {JSON.stringify(dispatchResult.jde_response, null, 2)}
                                                            </pre>
                                                        </div>
                                                    )}
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                )}
                            </div>
                            <div className="modal-footer">
                                <button 
                                    type="button" 
                                    className="btn btn-secondary"
                                    onClick={() => {
                                        setShowPayloadReview(false);
                                        setJdePayload(null);
                                        setCurrentBatchForReview(null);
                                        setShowDispatchPreview(false);
                                        setDispatchResult(null);
                                    }}
                                >
                                    <i className="fas fa-times me-2"></i>
                                    Cancel
                                </button>
                                
                                {!showDispatchPreview ? (
                                    <button 
                                        type="button" 
                                        className="btn btn-primary"
                                        onClick={() => setShowDispatchPreview(true)}
                                    >
                                        <i className="fas fa-save me-2"></i>
                                        Save Data & Prepare Dispatch
                                    </button>
                                ) : (
                                    <div>
                                        <button 
                                            type="button" 
                                            className="btn btn-warning me-2"
                                            onClick={() => {
                                                setShowDispatchPreview(false);
                                                setDispatchResult(null);
                                            }}
                                        >
                                            <i className="fas fa-edit me-2"></i>
                                            Edit Payload
                                        </button>
                                        
                                        <button 
                                            type="button" 
                                            className={`btn btn-success ${dispatchingBatches.has(`${currentBatchForReview.action_id}_${currentBatchForReview.unique_transaction_id}`) ? 'disabled' : ''}`}
                                            onClick={async () => {
                                                if (dispatchingBatches.has(`${currentBatchForReview.action_id}_${currentBatchForReview.unique_transaction_id}`)) return;
                                                
                                                const batchKey = `${currentBatchForReview.action_id}_${currentBatchForReview.unique_transaction_id}`;
                                                setDispatchingBatches(prev => new Set([...prev, batchKey]));
                                                
                                                try {
                                                    const response = await fetchWithAuth(
                                                        `${API_BASE_URL}/dispatch/prepared_payload_to_jde`,
                                                        {
                                                            method: 'POST',
                                                            headers: {
                                                                'Content-Type': 'application/json'
                                                            },
                                                            body: JSON.stringify({
                                                                jde_payload: jdePayload.jde_payload,
                                                                batch_data: jdePayload.original_batch
                                                            })
                                                        }
                                                    );
                                                    
                                                    const result = await response.json();
                                                    
                                                    // Store the dispatch result
                                                    setDispatchResult(result);
                                                    
                                                    if (result.success) {
                                                        // Mark batch as dispatched in local state
                                                        setBatches(prev =>
                                                            prev.map(b =>
                                                                b.batch_id === currentBatchForReview.batch_id ? {...b, already_dispatched: true} : b
                                                            )
                                                        );
                                                        
                                                        // Show success message after a delay
                                                        setTimeout(() => {
                                                            alert(`Successfully dispatched lot ${currentBatchForReview.lot_number || currentBatchForReview.batch_number || currentBatchForReview.batch_id}`);
                                                        }, 500);
                                                    }
                                                } catch (error) {
                                                    console.error('Error dispatching batch:', error);
                                                    setDispatchResult({
                                                        success: false,
                                                        message: error.message,
                                                        jde_response: null
                                                    });
                                                } finally {
                                                    setDispatchingBatches(prev => {
                                                        const newSet = new Set(prev);
                                                        newSet.delete(batchKey);
                                                        return newSet;
                                                    });
                                                }
                                            }}
                                            disabled={dispatchingBatches.has(`${currentBatchForReview.action_id}_${currentBatchForReview.batch_id}`) || dispatchResult?.success}
                                        >
                                            {dispatchingBatches.has(`${currentBatchForReview.action_id}_${currentBatchForReview.batch_id}`) ? (
                                                <>
                                                    <i className="fas fa-spinner fa-spin me-2"></i>
                                                    Dispatching...
                                                </>
                                            ) : dispatchResult?.success ? (
                                                <>
                                                    <i className="fas fa-check me-2"></i>
                                                    Dispatched
                                                </>
                                            ) : (
                                                <>
                                                    <i className="fas fa-paper-plane me-2"></i>
                                                    Dispatch to JDE
                                                </>
                                            )}
                                        </button>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                </div>
            )}

            <ErrorModal
                show={showErrorModal}
                onHide={() => setShowErrorModal(false)}
                title="Error Loading Batch Data"
                message={error}
            />
        </div>
    );
};

export default BakerySystemToJde;
