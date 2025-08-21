import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { API_BASE_URL } from '../config/api';

const BatchReview = () => {
  const { sessionId } = useParams();
  const navigate = useNavigate();
  const [batchData, setBatchData] = useState([]);
  const [editedPayloads, setEditedPayloads] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [dispatchResults, setDispatchResults] = useState({});
  const [currentBatchIndex, setCurrentBatchIndex] = useState(0);

  useEffect(() => {
    const loadSessionData = async () => {
      try {
        setLoading(true);
        const response = await fetch(`${API_BASE_URL}/batch_review/get_session/${sessionId}`);
        
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
          setBatchData(result.data);
          // Initialize JDE payloads for each batch
          initializePayloads(result.data);
        } else {
          throw new Error(result.error || 'Failed to load session data');
        }
      } catch (err) {
        console.error('Error loading session:', err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    loadSessionData();
  }, [sessionId]);

  const initializePayloads = async (batches) => {
    const payloads = {};
    
    for (const batch of batches) {
      try {
        const response = await fetch(`${API_BASE_URL}/prepare_jde_payload`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(batch)
        });
        
        const result = await response.json();
        
        if (result.success) {
          payloads[batch.unique_transaction_id] = {
            jde_payload: result.jde_payload,
            original_batch: result.original_batch,
            meta_info: result.meta_info
          };
        } else {
          payloads[batch.unique_transaction_id] = {
            error: result.error,
            original_batch: batch
          };
        }
      } catch (err) {
        payloads[batch.unique_transaction_id] = {
          error: `Failed to prepare payload: ${err.message}`,
          original_batch: batch
        };
      }
    }
    
    setEditedPayloads(payloads);
  };

  const handlePayloadEdit = (uniqueTransactionId, field, value) => {
    setEditedPayloads(prev => ({
      ...prev,
      [uniqueTransactionId]: {
        ...prev[uniqueTransactionId],
        jde_payload: {
          ...prev[uniqueTransactionId]?.jde_payload,
          [field]: value
        }
      }
    }));
  };

  const handleGridDataEdit = (uniqueTransactionId, field, value) => {
    setEditedPayloads(prev => ({
      ...prev,
      [uniqueTransactionId]: {
        ...prev[uniqueTransactionId],
        jde_payload: {
          ...prev[uniqueTransactionId]?.jde_payload,
          GridData: prev[uniqueTransactionId]?.jde_payload?.GridData?.map((item, index) => 
            index === 0 ? { ...item, [field]: value } : item
          ) || []
        }
      }
    }));
  };

  const dispatchBatch = async (uniqueTransactionId) => {
    try {
      const payload = editedPayloads[uniqueTransactionId];
      
      if (!payload || !payload.jde_payload) {
        throw new Error('No valid payload available for dispatch');
      }

      const response = await fetch(`${API_BASE_URL}/dispatch/prepared_payload_to_jde`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          jde_payload: payload.jde_payload,
          batch_data: payload.original_batch
        })
      });
      
      const result = await response.json();
      
      setDispatchResults(prev => ({
        ...prev,
        [uniqueTransactionId]: result
      }));
      
      if (result.success) {
        alert(`Batch ${uniqueTransactionId} dispatched successfully!`);
      } else {
        alert(`Failed to dispatch batch ${uniqueTransactionId}: ${result.error}`);
      }
    } catch (err) {
      const errorMsg = `Error dispatching batch ${uniqueTransactionId}: ${err.message}`;
      setDispatchResults(prev => ({
        ...prev,
        [uniqueTransactionId]: { success: false, error: errorMsg }
      }));
      alert(errorMsg);
    }
  };

  const dispatchAllBatches = async () => {
    if (!window.confirm('Are you sure you want to dispatch all batches to JDE?')) {
      return;
    }

    for (const batch of batchData) {
      if (!dispatchResults[batch.unique_transaction_id]?.success) {
        await dispatchBatch(batch.unique_transaction_id);
        // Small delay between dispatches
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
  };

  const goBack = () => {
    navigate('/bakery_system_to-jde');
  };

  if (loading) {
    return (
      <div className="container mt-4">
        <div className="d-flex justify-content-center">
          <div className="spinner-border" role="status">
            <span className="visually-hidden">Loading session data...</span>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mt-4">
        <div className="alert alert-danger" role="alert">
          <h4 className="alert-heading">Error Loading Session</h4>
          <p>{error}</p>
          <hr />
          <button className="btn btn-primary" onClick={goBack}>
            Go Back to Bakery-System to JDE
          </button>
        </div>
      </div>
    );
  }

  if (batchData.length === 0) {
    return (
      <div className="container mt-4">
        <div className="alert alert-warning" role="alert">
          <h4 className="alert-heading">No Batch Data</h4>
          <p>No batches found in this session.</p>
          <button className="btn btn-primary" onClick={goBack}>
            Go Back to Bakery-System to JDE
          </button>
        </div>
      </div>
    );
  }

  const currentBatch = batchData[currentBatchIndex];
  const currentPayload = editedPayloads[currentBatch?.unique_transaction_id];
  const progressPercentage = ((currentBatchIndex + 1) / batchData.length) * 100;

  return (
    <div className="container-fluid mt-4">
      {/* Header */}
      <div className="row mb-4">
        <div className="col">
          <div className="d-flex justify-content-between align-items-center">
            <div>
              <h2>Batch Review - Session: {sessionId.substring(0, 8)}...</h2>
              <p className="text-muted">Review and edit JDE payloads before dispatching</p>
            </div>
            <div>
              <button className="btn btn-secondary me-2" onClick={goBack}>
                Back to Selection
              </button>
              <button 
                className="btn btn-success" 
                onClick={dispatchAllBatches}
                disabled={batchData.every(b => dispatchResults[b.unique_transaction_id]?.success)}
              >
                Dispatch All Remaining
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Progress Bar */}
      <div className="row mb-4">
        <div className="col">
          <div className="card">
            <div className="card-body">
              <h6 className="card-title">Progress</h6>
              <div className="progress mb-2">
                <div 
                  className="progress-bar" 
                  role="progressbar" 
                  style={{width: `${progressPercentage}%`}}
                  aria-valuenow={progressPercentage} 
                  aria-valuemin="0" 
                  aria-valuemax="100"
                >
                  {Math.round(progressPercentage)}%
                </div>
              </div>
              <small className="text-muted">
                Batch {currentBatchIndex + 1} of {batchData.length} • 
                Dispatched: {Object.values(dispatchResults).filter(r => r.success).length} • 
                Remaining: {batchData.length - Object.values(dispatchResults).filter(r => r.success).length}
              </small>
            </div>
          </div>
        </div>
      </div>

      {/* Navigation */}
      <div className="row mb-3">
        <div className="col">
          <div className="d-flex justify-content-between align-items-center">
            <div className="btn-group" role="group">
              <button 
                className="btn btn-outline-primary" 
                onClick={() => setCurrentBatchIndex(Math.max(0, currentBatchIndex - 1))}
                disabled={currentBatchIndex === 0}
              >
                ← Previous
              </button>
              <button 
                className="btn btn-outline-primary" 
                onClick={() => setCurrentBatchIndex(Math.min(batchData.length - 1, currentBatchIndex + 1))}
                disabled={currentBatchIndex === batchData.length - 1}
              >
                Next →
              </button>
            </div>
            <span className="badge bg-secondary">
              Batch {currentBatchIndex + 1} of {batchData.length}
            </span>
          </div>
        </div>
      </div>

      {currentBatch && (
        <div className="row">
          {/* Original Batch Data */}
          <div className="col-md-4">
            <div className="card">
              <div className="card-header">
                <h5>Original Batch Data</h5>
              </div>
              <div className="card-body">
                <table className="table table-sm">
                  <tbody>
                    <tr><td><strong>Batch ID:</strong></td><td>{currentBatch.batch_id}</td></tr>
                    <tr><td><strong>Action ID:</strong></td><td>{currentBatch.action_id}</td></tr>
                    <tr><td><strong>Ingredient ID:</strong></td><td>{currentBatch.ingredient_id}</td></tr>
                    <tr><td><strong>Ingredient Name:</strong></td><td>{currentBatch.ingredient_name}</td></tr>
                    <tr><td><strong>Quantity:</strong></td><td>{currentBatch.quantity}</td></tr>
                    <tr><td><strong>Unit:</strong></td><td>{currentBatch.unit}</td></tr>
                    {currentBatch.vessel_id && (
                      <tr><td><strong>Vessel ID:</strong></td><td>{currentBatch.vessel_id}</td></tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* JDE Payload Editor */}
          <div className="col-md-5">
            <div className="card">
              <div className="card-header d-flex justify-content-between align-items-center">
                <h5>JDE Payload Editor</h5>
                {currentPayload?.error && (
                  <span className="badge bg-danger">Error</span>
                )}
              </div>
              <div className="card-body">
                {currentPayload?.error ? (
                  <div className="alert alert-danger">
                    <strong>Payload Error:</strong> {currentPayload.error}
                  </div>
                ) : currentPayload?.jde_payload ? (
                  <div>
                    {/* Main JDE Fields */}
                    <div className="row mb-3">
                      <div className="col-md-6">
                        <label className="form-label">Branch Plant</label>
                        <input 
                          type="text" 
                          className="form-control form-control-sm"
                          value={currentPayload.jde_payload.Branch_Plant || ''}
                          onChange={(e) => handlePayloadEdit(currentBatch.unique_transaction_id, 'Branch_Plant', e.target.value)}
                        />
                      </div>
                      <div className="col-md-6">
                        <label className="form-label">Document Type</label>
                        <input 
                          type="text" 
                          className="form-control form-control-sm"
                          value={currentPayload.jde_payload.Document_Type || ''}
                          onChange={(e) => handlePayloadEdit(currentBatch.unique_transaction_id, 'Document_Type', e.target.value)}
                        />
                      </div>
                    </div>

                    <div className="mb-3">
                      <label className="form-label">Explanation</label>
                      <input 
                        type="text" 
                        className="form-control form-control-sm"
                        value={currentPayload.jde_payload.Explanation || ''}
                        onChange={(e) => handlePayloadEdit(currentBatch.unique_transaction_id, 'Explanation', e.target.value)}
                      />
                    </div>

                    {/* Grid Data */}
                    {currentPayload.jde_payload.GridData && currentPayload.jde_payload.GridData[0] && (
                      <div>
                        <h6>Grid Data</h6>
                        <div className="row mb-2">
                          <div className="col-md-6">
                            <label className="form-label">Item Number</label>
                            <input 
                              type="text" 
                              className="form-control form-control-sm"
                              value={currentPayload.jde_payload.GridData[0].Item_Number || ''}
                              onChange={(e) => handleGridDataEdit(currentBatch.unique_transaction_id, 'Item_Number', e.target.value)}
                            />
                          </div>
                          <div className="col-md-6">
                            <label className="form-label">Quantity</label>
                            <input 
                              type="text" 
                              className="form-control form-control-sm"
                              value={currentPayload.jde_payload.GridData[0].Quantity || ''}
                              onChange={(e) => handleGridDataEdit(currentBatch.unique_transaction_id, 'Quantity', e.target.value)}
                            />
                          </div>
                        </div>
                        <div className="row mb-2">
                          <div className="col-md-6">
                            <label className="form-label">Unit of Measure</label>
                            <input 
                              type="text" 
                              className="form-control form-control-sm"
                              value={currentPayload.jde_payload.GridData[0].UM || ''}
                              onChange={(e) => handleGridDataEdit(currentBatch.unique_transaction_id, 'UM', e.target.value)}
                            />
                          </div>
                          <div className="col-md-6">
                            <label className="form-label">Lot Number</label>
                            <input 
                              type="text" 
                              className="form-control form-control-sm"
                              value={currentPayload.jde_payload.GridData[0].LOTN || ''}
                              onChange={(e) => handleGridDataEdit(currentBatch.unique_transaction_id, 'LOTN', e.target.value)}
                            />
                          </div>
                        </div>
                      </div>
                    )}

                    {/* Dates */}
                    <div className="row mt-3">
                      <div className="col-md-6">
                        <label className="form-label">G/L Date</label>
                        <input 
                          type="text" 
                          className="form-control form-control-sm"
                          value={currentPayload.jde_payload.G_L_Date || ''}
                          onChange={(e) => handlePayloadEdit(currentBatch.unique_transaction_id, 'G_L_Date', e.target.value)}
                        />
                      </div>
                      <div className="col-md-6">
                        <label className="form-label">Transaction Date</label>
                        <input 
                          type="text" 
                          className="form-control form-control-sm"
                          value={currentPayload.jde_payload.Transaction_Date || ''}
                          onChange={(e) => handlePayloadEdit(currentBatch.unique_transaction_id, 'Transaction_Date', e.target.value)}
                        />
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="alert alert-info">
                    <div className="spinner-border spinner-border-sm me-2" role="status"></div>
                    Preparing JDE payload...
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Actions and Status */}
          <div className="col-md-3">
            <div className="card">
              <div className="card-header">
                <h5>Actions</h5>
              </div>
              <div className="card-body">
                {dispatchResults[currentBatch.unique_transaction_id] ? (
                  <div>
                    {dispatchResults[currentBatch.unique_transaction_id].success ? (
                      <div className="alert alert-success">
                        <strong>✓ Dispatched Successfully</strong>
                        <br />
                        <small>{dispatchResults[currentBatch.unique_transaction_id].message}</small>
                      </div>
                    ) : (
                      <div className="alert alert-danger">
                        <strong>✗ Dispatch Failed</strong>
                        <br />
                        <small>{dispatchResults[currentBatch.unique_transaction_id].error}</small>
                        <hr />
                        <button 
                          className="btn btn-danger btn-sm"
                          onClick={() => dispatchBatch(currentBatch.unique_transaction_id)}
                        >
                          Retry Dispatch
                        </button>
                      </div>
                    )}
                  </div>
                ) : (
                  <div>
                    <button 
                      className="btn btn-primary w-100"
                      onClick={() => dispatchBatch(currentBatch.unique_transaction_id)}
                      disabled={!currentPayload?.jde_payload}
                    >
                      Dispatch to JDE
                    </button>
                    {currentPayload?.meta_info && (
                      <div className="mt-3">
                        <small className="text-muted">
                          <strong>Meta Info:</strong><br />
                          BU: {currentPayload.meta_info.determined_bu}<br />
                          Unit: {currentPayload.meta_info.converted_unit}<br />
                          Lot: {currentPayload.meta_info.extracted_lot_number}
                        </small>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default BatchReview;
