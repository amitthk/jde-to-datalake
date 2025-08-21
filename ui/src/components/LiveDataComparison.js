import React, { useState, useEffect } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import * as XLSX from 'xlsx';
import { useAuth } from '../context/AuthContext';
import ErrorModal from './ErrorModal';
import BarChart from './BarChart';
import { API_BASE_URL } from '../config/api';

function LiveDataComparison() {
  const [liveData, setLiveData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [dispatching, setDispatching] = useState({});
  const [deleting, setDeleting] = useState({});
  const [patching, setPatching] = useState({});
  const [errorModal, setErrorModal] = useState({ show: false, title: '', error: '' });
  const [showMismatchOnly, setShowMismatchOnly] = useState(false);
  const [showDispatchableOnly, setShowDispatchableOnly] = useState(false);
  const [showWithIngredientId, setShowWithIngredientId] = useState(false);
  const [daysBack, setDaysBack] = useState(5); // Default to 5 days back
  const { fetchWithAuth } = useAuth();

  // Check if any checkbox is checked to determine if Dispatch button should always show
  const isAnyFilterActive = showMismatchOnly || showDispatchableOnly || showWithIngredientId;

  const fetchLiveData = async () => {
    setLoading(true);
    try {
      const response = await fetchWithAuth(`${API_BASE_URL}/data/joined_df3?days_back=${daysBack}`);
      const data = await response.json();
      setLiveData(data.data);
    } catch (error) {
      console.error('Error fetching live data:', error);
      let errorMessage = 'Error fetching live data. Please check the backend connection.';
      if (error.message) {
        errorMessage = `${errorMessage}\n\nError Details:\n${error.message}`;
      }
      setErrorModal({
        show: true,
        title: 'Data Fetch Error',
        error: errorMessage
      });
    } finally {
      setLoading(false);
    }
  };

  const handleDispatch = async (transactionId, rawJdeData) => {
    if (!window.confirm(`Are you sure you want to dispatch transaction ${transactionId}?`)) {
      return;
    }

    setDispatching(prev => ({ ...prev, [transactionId]: true }));

    try {
      const response = await fetchWithAuth(`${API_BASE_URL}/dispatch/transaction`, {
        method: 'POST',
        body: JSON.stringify({
          transaction_id: transactionId,
          raw_jde_data: rawJdeData
        })
      });

      const result = await response.json();

      if (result.success) {
        alert(`Transaction ${transactionId} dispatched successfully!`);
        // Refresh the data to show updated status
        await fetchLiveData();
      } else {
        let errorMessage = `Failed to dispatch transaction ${transactionId}`;
        if (result.message) {
          errorMessage = `${errorMessage}\n\nDetails: ${result.message}`;
        }
        alert(errorMessage);
      }
    } catch (error) {
      console.error('Error dispatching transaction:', error);
      let errorMessage = `Error dispatching transaction ${transactionId}. Please try again.`;
      if (error.message) {
        errorMessage = `${errorMessage}\n\nError Details:\n${error.message}`;
      }
      setErrorModal({
        show: true,
        title: 'Dispatch Error',
        error: errorMessage
      });
    } finally {
      setDispatching(prev => ({ ...prev, [transactionId]: false }));
    }
  };

  const handleDeleteIngredient = async (productName, ingredientId) => {
    if (!ingredientId) {
      alert('No Ingredient ID available for deletion.');
      return;
    }

    if (!window.confirm(`Are you sure you want to delete Ingredient ${productName} (ID: ${ingredientId})?\n\nThis action cannot be undone.`)) {
      return;
    }

    setDeleting(prev => ({ ...prev, [productName]: true }));

    try {
      const response = await fetchWithAuth(`${API_BASE_URL}/delete/Ingredient/${ingredientId}`, {
        method: 'DELETE'
      });

      const result = await response.json();

      if (result.success) {
        alert(`Ingredient ${productName} deleted successfully!`);
        // Refresh the data to show updated status
        await fetchLiveData();
      } else {
        let errorMessage = `Failed to delete Ingredient ${productName}`;
        if (result.message) {
          errorMessage = `${errorMessage}\n\nDetails: ${result.message}`;
        }
        if (result.error) {
          errorMessage = `${errorMessage}\n\nError: ${result.error}`;
        }
        alert(errorMessage);
      }
    } catch (error) {
      console.error('Error deleting Ingredient:', error);
      let errorMessage = `Error deleting Ingredient ${productName}. Please try again.`;
      if (error.message) {
        errorMessage = `${errorMessage}\n\nError Details:\n${error.message}`;
      }
      setErrorModal({
        show: true,
        title: 'Ingredient Deletion Error',
        error: errorMessage
      });
    } finally {
      setDeleting(prev => ({ ...prev, [productName]: false }));
    }
  };

  const handlePatchIngredient = async (productName, rawJdeData) => {
    if (!window.confirm(`Are you sure you want to patch Ingredient ${productName}?\n\nThis will set addition rate value and addition rate to None.`)) {
      return;
    }

    setPatching(prev => ({ ...prev, [productName]: true }));

    try {
      const response = await fetchWithAuth(`${API_BASE_URL}/patch/Ingredient`, {
        method: 'PATCH',
        body: JSON.stringify({
          raw_jde_data: rawJdeData
        })
      });

      const result = await response.json();

      if (result.success) {
        // Display detailed success message with result information
        let successMessage = `✅ Ingredient ${productName} patched successfully!\n\n`;
        successMessage += `📋 Changes Applied:\n`;
        successMessage += `• Addition Rate Value: Set to None\n`;
        successMessage += `• Addition Rate Unit: Set to None\n`;
        
        if (result.result && Array.isArray(result.result) && result.result.length > 0) {
          const updateResult = result.result[0];
          if (updateResult) {
            successMessage += `\n🔧 Technical Details:\n`;
            if (updateResult.inventoryUnit) {
              successMessage += `• Inventory Unit: ${updateResult.inventoryUnit}\n`;
            }
            if (updateResult._id) {
              successMessage += `• Bakery-System ID: ${updateResult._id}\n`;
            }
            if (updateResult.name) {
              successMessage += `• Product Name: ${updateResult.name}\n`;
            }
          }
        }
        
        successMessage += `\n💡 Note: Page data not refreshed. The changes are applied in Bakery-System system.`;
        
        alert(successMessage);
      } else {
        let errorMessage = `Failed to patch Ingredient ${productName}`;
        if (result.message) {
          errorMessage = `${errorMessage}\n\nDetails: ${result.message}`;
        }
        alert(errorMessage);
      }
    } catch (error) {
      console.error('Error patching Ingredient:', error);
      let errorMessage = `Error patching Ingredient ${productName}. Please try again.`;
      if (error.message) {
        errorMessage = `${errorMessage}\n\nError Details:\n${error.message}`;
      }
      setErrorModal({
        show: true,
        title: 'Ingredient Patch Error',
        error: errorMessage
      });
    } finally {
      setPatching(prev => ({ ...prev, [productName]: false }));
    }
  };

  // Helper to compare totals and set match/mismatch flag
  const getTotalStatus = (item) => {
    if (item.total_jde_quantity === undefined || item.total_bakery_system_quantity === undefined) return 'Unknown';
    const jde = Number(item.total_jde_quantity);
    const bakery_system = Number(item.total_bakery_system_quantity);
    if (isNaN(jde) || isNaN(bakery_system)) return 'Unknown';
    return jde === bakery-system ? 'Match' : 'Mismatch';
  };

  // Filter data based on checkbox selections
  const getFilteredData = () => {
    let filteredData = [...liveData];

    if (showMismatchOnly) {
      filteredData = filteredData.filter(item => getTotalStatus(item) === 'Mismatch');
    }

    if (showDispatchableOnly) {
      filteredData = filteredData.filter(item => item.can_dispatch);
    }

    if (showWithIngredientId) {
      filteredData = filteredData.filter(item => item.bakery_system_id);
    }

    return filteredData;
  };

  const filteredData = getFilteredData();

  const handleDownloadExcel = () => {
    if (!liveData || liveData.length === 0) {
      alert('No data to download.');
      return;
    }
    // Convert data to worksheet
    const worksheet = XLSX.utils.json_to_sheet(liveData);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Report');
    // Generate Excel file and trigger download
    XLSX.writeFile(workbook, 'LiveDataComparisonReport.xlsx');
  };

  // Load initial data only once when component mounts (using default daysBack value)
  useEffect(() => {
    const initialLoad = async () => {
      setLoading(true);
      try {
        const response = await fetchWithAuth(`${API_BASE_URL}/data/joined_df3?days_back=5`); // Use default value
        const data = await response.json();
        setLiveData(data.data);
      } catch (error) {
        console.error('Error fetching live data:', error);
        let errorMessage = 'Error fetching live data. Please check the backend connection.';
        if (error.message) {
          errorMessage = `${errorMessage}\n\nError Details:\n${error.message}`;
        }
        setErrorModal({
          show: true,
          title: 'Data Fetch Error',
          error: errorMessage
        });
      } finally {
        setLoading(false);
      }
    };
    
    initialLoad();
  }, [fetchWithAuth]); // Only load once on mount

  if (loading) {
    return (
      <div className="container-fluid">
        <h2>Live Data Comparison</h2>
        <div className="d-flex justify-content-center">
          <div className="spinner-border" role="status">
            <span className="visually-hidden">Loading...</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="container-fluid" style={{overflowX: 'auto', minWidth: '1200px'}}>
      {/* Header */}
      <div className="row mb-4">
        <div className="col">
          <h2 className="text-dark">
            <i className="fas fa-balance-scale me-2"></i>
            JDE Cardex Review - JDE vs Bakery-System
          </h2>
          <p className="text-muted">Compare JDE Cardex data with Bakery-System inventory for discrepancies and dispatch actions.</p>
        </div>
      </div>

      {/* Data Controls */}
      <div className="row mb-4">
        <div className="col">
          <div className="card">
            <div className="card-header bg-secondary text-white">
              <h5 className="mb-0">
                <i className="fas fa-cog me-2"></i>
                Data Controls & Filters
              </h5>
            </div>
            <div className="card-body">
              {/* Days Back Selector */}
              <div className="row mb-4">
                <div className="col-lg-8">
                  <label htmlFor="daysBack" className="form-label fw-bold">
                    <i className="fas fa-calendar-alt me-2"></i>
                    Days to Look Back for JDE Cardex Data
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
                  </div>
                  <small className="text-muted">
                    Number of days back to fetch JDE Cardex changes (1-365 days)
                  </small>
                </div>
                <div className="col-lg-4 d-flex align-items-end justify-content-start">
                  <button 
                    className="btn btn-secondary btn-lg" 
                    onClick={fetchLiveData}
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
              
              {/* Filter Checkboxes */}
              <div className="border-top pt-3">
                <h6 className="fw-bold mb-3">
                  <i className="fas fa-filter me-2"></i>
                  Display Filters
                </h6>
                <div className="row">
                  <div className="col-md-4 mb-2">
                    <div className="form-check">
                      <input
                        className="form-check-input"
                        type="checkbox"
                        id="showMismatchOnly"
                        checked={showMismatchOnly}
                        onChange={(e) => setShowMismatchOnly(e.target.checked)}
                      />
                      <label className="form-check-label" htmlFor="showMismatchOnly">
                        <span className="badge bg-danger me-2">Mismatch</span>
                        Show Mismatches Only
                      </label>
                    </div>
                  </div>
                  <div className="col-md-4 mb-2">
                    <div className="form-check">
                      <input
                        className="form-check-input"
                        type="checkbox"
                        id="showDispatchableOnly"
                        checked={showDispatchableOnly}
                        onChange={(e) => setShowDispatchableOnly(e.target.checked)}
                      />
                      <label className="form-check-label" htmlFor="showDispatchableOnly">
                        <span className="badge bg-warning me-2">Dispatch</span>
                        Show Dispatchable Only
                      </label>
                    </div>
                  </div>
                  <div className="col-md-4 mb-2">
                    <div className="form-check">
                      <input
                        className="form-check-input"
                        type="checkbox"
                        id="showWithIngredientId"
                        checked={showWithIngredientId}
                        onChange={(e) => setShowWithIngredientId(e.target.checked)}
                      />
                      <label className="form-check-label" htmlFor="showWithIngredientId">
                        <span className="badge bg-info me-2">ID</span>
                        Show Items with Ingredient ID Only
                      </label>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      
      {/* Data Summary and Actions */}
      {!loading && liveData.length > 0 && (
        <div className="row mb-3">
          <div className="col-md-8">
            <div className="card">
              <div className="card-header bg-secondary text-white">
                <h5 className="mb-0">
                  <i className="fas fa-chart-bar me-2"></i>
                  Summary Statistics
                </h5>
              </div>
              <div className="card-body">
                <BarChart
                  data={[
                    {
                      label: 'Total Records',
                      value: liveData.length,
                      color: '#6c757d'
                    },
                    {
                      label: 'Matches',
                      value: liveData.filter(item => getTotalStatus(item) === 'Match').length,
                      color: '#198754'
                    },
                    {
                      label: 'Mismatches',
                      value: liveData.filter(item => getTotalStatus(item) === 'Mismatch').length,
                      color: '#dc3545'
                    },
                    {
                      label: 'Dispatched',
                      value: liveData.filter(item => item.dispatched).length,
                      color: '#0d6efd'
                    },
                    {
                      label: 'Filtered Results',
                      value: filteredData.length,
                      color: '#fd7e14'
                    }
                  ]}
                  title="Data Overview"
                  height={250}
                />
              </div>
            </div>
          </div>
          <div className="col-md-4">
            <div className="card h-100">
              <div className="card-header bg-secondary text-white">
                <h5 className="mb-0">
                  <i className="fas fa-tools me-2"></i>
                  Actions
                </h5>
              </div>
              <div className="card-body d-flex flex-column justify-content-center">
                <button
                  className="btn btn-success btn-lg"
                  onClick={handleDownloadExcel}
                  disabled={loading || liveData.length === 0}
                >
                  <i className="fas fa-file-excel me-2"></i>
                  Download Excel Report
                </button>
                <small className="text-muted mt-2 text-center">
                  Export all data to Excel format
                </small>
              </div>
            </div>
          </div>
        </div>
      )}

      {filteredData.length > 0 && (
        <div className="row">
          <div className="col">
            <div className="card">
              <div className="card-header bg-dark text-white">
                <h5 className="mb-0">
                  <i className="fas fa-table me-2"></i>
                  Data Comparison Results ({filteredData.length} records)
                </h5>
              </div>
              <div className="card-body p-0">
                <div className="table-responsive">
                  <table className="table table-bordered table-sm table-striped mb-0" style={{minWidth: '1500px'}}>
                    <thead className="table-dark sticky-top">
                      <tr>
                        <th style={{minWidth: '100px'}}>Transaction</th>
                        <th style={{minWidth: '140px'}}>Product</th>
                        <th style={{minWidth: '100px'}}>Batch</th>
                        <th style={{minWidth: '80px'}}>Lot #</th>
                        <th style={{minWidth: '80px'}}>JDE Qty</th>
                        <th style={{minWidth: '60px'}}>Unit</th>
                        <th style={{minWidth: '100px'}}>Date</th>
                        <th style={{minWidth: '80px'}}>Inn. Qty</th>
                        <th style={{minWidth: '70px'}}>Batches</th>
                        <th style={{minWidth: '80px'}}>Tot JDE</th>
                        <th style={{minWidth: '80px'}}>Tot Inn.</th>
                        <th style={{minWidth: '100px'}}>Status</th>
                        <th style={{minWidth: '100px'}}>Add. ID</th>
                        <th style={{minWidth: '180px'}}>Actions</th>
                      </tr>
                    </thead>
                    <tbody>
              {filteredData.map((item, index) => (
                <tr key={index} className={item.dispatched ? 'table-success' : ''}>
                  <td className="text-truncate" title={item.transaction_id}>{item.transaction_id}</td>
                  <td className="text-truncate" title={item.product_name}>{item.product_name}</td>
                  <td className="text-truncate" title={item.batch_name}>{item.batch_name}</td>
                  <td className="text-truncate" title={item.lot_number || 'N/A'}>{item.lot_number || 'N/A'}</td>
                  <td>{item.jde_quantity}</td>
                  <td>{item.jde_unit}</td>
                  <td className="small">{item.jde_date}</td>
                  <td>{item.bakery_system_quantity}</td>
                  <td>{item.bakery_system_batches_count}</td>
                  <td>{item.total_jde_quantity}</td>
                  <td>{item.total_bakery_system_quantity}</td>
                  <td>
                    {getTotalStatus(item) === 'Match' ? (
                      <span className="badge bg-success">Match</span>
                    ) : getTotalStatus(item) === 'Mismatch' ? (
                      <span className="badge bg-danger">Mismatch</span>
                    ) : (
                      <span className="badge bg-secondary">Unknown</span>
                    )}
                  </td>
                  <td className="text-truncate small" title={item.bakery_system_id || 'N/A'}>{item.bakery_system_id || 'N/A'}</td>
                  <td>
                    <div className="d-flex flex-column gap-1" style={{minWidth: '170px'}}>
                      {(isAnyFilterActive || (getTotalStatus(item) === 'Mismatch' && item.can_dispatch)) ? (
                        <button
                          className="btn btn-warning btn-sm"
                          onClick={() => handleDispatch(item.transaction_id, item.raw_jde_data)}
                          disabled={dispatching[item.transaction_id]}
                          title="Dispatch to Bakery-System"
                        >
                          {dispatching[item.transaction_id] ? (
                            <i className="fas fa-spinner fa-spin"></i>
                          ) : (
                            <i className="fas fa-paper-plane"></i>
                          )}
                          {' Dispatch'}
                        </button>
                      ) : null}
                      {item.bakery_system_id ? (
                        <div className="d-flex gap-1">
                          <button
                            className="btn btn-info btn-sm flex-fill"
                            onClick={() => handlePatchIngredient(item.product_name, item.raw_jde_data)}
                            disabled={patching[item.product_name]}
                            title="Patch Ingredient rates"
                          >
                            {patching[item.product_name] ? (
                              <i className="fas fa-spinner fa-spin"></i>
                            ) : (
                              <i className="fas fa-wrench"></i>
                            )}
                            {' Patch'}
                          </button>
                          <button
                            className="btn btn-danger btn-sm flex-fill"
                            onClick={() => handleDeleteIngredient(item.product_name, item.bakery_system_id)}
                            disabled={deleting[item.product_name]}
                            title="Delete Ingredient"
                          >
                            {deleting[item.product_name] ? (
                              <i className="fas fa-spinner fa-spin"></i>
                            ) : (
                              <i className="fas fa-trash"></i>
                            )}
                            {' Delete'}
                          </button>
                        </div>
                      ) : null}
                      {!isAnyFilterActive && !item.can_dispatch && !item.bakery_system_id && getTotalStatus(item) !== 'Mismatch' ? (
                        <span className="text-muted small text-center">
                          {getTotalStatus(item) === 'Match' ? 'Matched' : 
                           item.dispatched ? 'Dispatched' : 'N/A'}
                        </span>
                      ) : null}
                    </div>
                  </td>
                </tr>
              ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
      
      {filteredData.length === 0 && liveData.length > 0 && !loading && (
        <div className="alert alert-info">
          No data matches the current filter settings. Try adjusting the filters above.
        </div>
      )}
      
      {liveData.length === 0 && !loading && (
        <div className="alert alert-info">
          No data available. Try refreshing or check the backend connection.
        </div>
      )}
      
      <div className="mt-3">
        <small className="text-muted">
          <strong>Status Legend:</strong>
          <span className="badge bg-success ms-2">Dispatched</span> - Transaction already processed in Bakery-System
          <span className="badge bg-warning ms-2">Missing in Bakery-System</span> - Transaction exists in JDE but not in Bakery-System
          <span className="badge bg-danger ms-2">Product Not Found</span> - Product doesn't exist in Bakery-System
          <span className="badge bg-info ms-2">Partial Match</span> - Product exists but quantities differ
        </small>
      </div>

      <ErrorModal 
        show={errorModal.show}
        onHide={() => setErrorModal({ show: false, title: '', error: '' })}
        title={errorModal.title}
        error={errorModal.error}
      />
    </div>
  );
}

export default LiveDataComparison;
