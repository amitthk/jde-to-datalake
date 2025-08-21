// src/components/JdeItemMasterReview.js

import React, { useState, useEffect } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import * as XLSX from 'xlsx';
import { useAuth } from '../context/AuthContext';
import ErrorModal from './ErrorModal';
import BarChart from './BarChart';
import { API_BASE_URL } from '../config/api';

function JdeItemMasterReview() {
  const [itemMasterData, setItemMasterData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState({});
  const [deleting, setDeleting] = useState({});
  const [patching, setPatching] = useState({});
  const [errorModal, setErrorModal] = useState({ show: false, title: '', error: '' });
  const [daysBack, setDaysBack] = useState(30); // Default to 30 days back for Item Master
  const [businessUnit, setBusinessUnit] = useState('1110'); // Default business unit
  const [glCategory, setGlCategory] = useState('WA01'); // Default GL category
  const { fetchWithAuth } = useAuth();

  const fetchItemMasterData = async () => {
    setLoading(true);
    try {
      const response = await fetchWithAuth(`${API_BASE_URL}/data/jde_item_master_review?days_back=${daysBack}&bu=${businessUnit}&gl_cat=${glCategory}`);
      const data = await response.json();
      setItemMasterData(data.data);
    } catch (error) {
      console.error('Error fetching item master data:', error);
      let errorMessage = 'Error fetching item master data. Please check the backend connection.';
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

  const handleCreateIngredient = async (itemNumber, rawJdeData) => {
    if (!window.confirm(`Are you sure you want to create Ingredient ${itemNumber}?`)) {
      return;
    }

    setCreating(prev => ({ ...prev, [itemNumber]: true }));

    try {
      const response = await fetchWithAuth(`${API_BASE_URL}/create/Ingredient`, {
        method: 'POST',
        body: JSON.stringify({
          raw_jde_data: rawJdeData
        })
      });

      const result = await response.json();

      if (result.success) {
        alert(`Ingredient ${itemNumber} created successfully!`);
        // Refresh the data to show updated status
        await fetchItemMasterData();
      } else {
        let errorMessage = `Failed to create Ingredient ${itemNumber}`;
        if (result.message) {
          errorMessage = `${errorMessage}\n\nDetails: ${result.message}`;
        }
        alert(errorMessage);
      }
    } catch (error) {
      console.error('Error creating Ingredient:', error);
      let errorMessage = `Error creating Ingredient ${itemNumber}. Please try again.`;
      if (error.message) {
        errorMessage = `${errorMessage}\n\nError Details:\n${error.message}`;
      }
      setErrorModal({
        show: true,
        title: 'Ingredient Creation Error',
        error: errorMessage
      });
    } finally {
      setCreating(prev => ({ ...prev, [itemNumber]: false }));
    }
  };

  const handleDeleteIngredient = async (itemNumber, ingredientId) => {
    if (!ingredientId) {
      alert('No Ingredient ID available for deletion.');
      return;
    }

    if (!window.confirm(`Are you sure you want to delete Ingredient ${itemNumber} (ID: ${ingredientId})?\n\nThis action cannot be undone.`)) {
      return;
    }

    setDeleting(prev => ({ ...prev, [itemNumber]: true }));

    try {
      const response = await fetchWithAuth(`${API_BASE_URL}/delete/Ingredient/${ingredientId}`, {
        method: 'DELETE'
      });

      const result = await response.json();

      if (result.success) {
        alert(`Ingredient ${itemNumber} deleted successfully!`);
        // Refresh the data to show updated status
        await fetchItemMasterData();
      } else {
        let errorMessage = `Failed to delete Ingredient ${itemNumber}`;
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
      let errorMessage = `Error deleting Ingredient ${itemNumber}. Please try again.`;
      if (error.message) {
        errorMessage = `${errorMessage}\n\nError Details:\n${error.message}`;
      }
      setErrorModal({
        show: true,
        title: 'Ingredient Deletion Error',
        error: errorMessage
      });
    } finally {
      setDeleting(prev => ({ ...prev, [itemNumber]: false }));
    }
  };

  const handlePatchIngredient = async (itemNumber, rawJdeData) => {
    if (!window.confirm(`Are you sure you want to patch Ingredient ${itemNumber}?\n\nThis will set addition rate value and addition rate to None.`)) {
      return;
    }

    setPatching(prev => ({ ...prev, [itemNumber]: true }));

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
        let successMessage = `✅ Ingredient ${itemNumber} patched successfully!\n\n`;
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
        let errorMessage = `Failed to patch Ingredient ${itemNumber}`;
        if (result.message) {
          errorMessage = `${errorMessage}\n\nDetails: ${result.message}`;
        }
        alert(errorMessage);
      }
    } catch (error) {
      console.error('Error patching Ingredient:', error);
      let errorMessage = `Error patching Ingredient ${itemNumber}. Please try again.`;
      if (error.message) {
        errorMessage = `${errorMessage}\n\nError Details:\n${error.message}`;
      }
      setErrorModal({
        show: true,
        title: 'Ingredient Patch Error',
        error: errorMessage
      });
    } finally {
      setPatching(prev => ({ ...prev, [itemNumber]: false }));
    }
  };

  const getStatusBadge = (status) => {
    const badgeClass = {
      'Exists in Bakery-System': 'badge bg-success',
      'Missing in Bakery-System': 'badge bg-danger'
    };
    
    return <span className={badgeClass[status] || 'badge bg-secondary'}>{status}</span>;
  };

  const handleDownloadExcel = () => {
    if (!itemMasterData || itemMasterData.length === 0) {
      alert('No data to download.');
      return;
    }
    // Convert data to worksheet
    const worksheet = XLSX.utils.json_to_sheet(itemMasterData);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'JDE Item Master Review');
    // Generate Excel file and trigger download
    XLSX.writeFile(workbook, 'JDE_ItemMaster_Review.xlsx');
  };

  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      try {
        const response = await fetchWithAuth(`${API_BASE_URL}/data/jde_item_master_review?days_back=${daysBack}&bu=${businessUnit}&gl_cat=${glCategory}`);
        const data = await response.json();
        setItemMasterData(data.data);
      } catch (error) {
        console.error('Error fetching item master data:', error);
        alert('Error fetching item master data. Please check the backend connection.');
      } finally {
        setLoading(false);
      }
    };
    
    loadData();
  }, [fetchWithAuth, daysBack, businessUnit, glCategory]);

  if (loading) {
    return (
      <div className="container-fluid">
        <h2>JDE Item Master Review</h2>
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
            <i className="fas fa-database me-2"></i>
            JDE Item Master Review
          </h2>
          <p className="text-muted">Review JDE Item Master data and manage ingredients in Bakery-System system.</p>
        </div>
      </div>
      
      {/* Control Panel */}
      <div className="card mb-4">
        <div className="card-header bg-secondary text-white">
          <h5 className="mb-0">
            <i className="fas fa-cog me-2"></i>
            Data Parameters
          </h5>
        </div>
        <div className="card-body">
          <div className="row">
            <div className="col-md-4">
              <label htmlFor="daysBack" className="form-label fw-bold">
                <i className="fas fa-calendar-alt me-2"></i>
                Days Back:
              </label>
              <input
                type="number"
                id="daysBack"
                className="form-control"
                value={daysBack}
                onChange={(e) => setDaysBack(parseInt(e.target.value))}
                min="1"
                max="365"
              />
              <small className="form-text text-muted">Number of days to look back (1-365)</small>
            </div>
            <div className="col-md-4">
              <label htmlFor="businessUnit" className="form-label fw-bold">
                <i className="fas fa-building me-2"></i>
                Business Unit:
              </label>
              <input
                type="text"
                id="businessUnit"
                className="form-control"
                value={businessUnit}
                onChange={(e) => setBusinessUnit(e.target.value)}
                placeholder="e.g., 1110"
              />
              <small className="form-text text-muted">Business Unit code</small>
            </div>
            <div className="col-md-4">
              <label htmlFor="glCategory" className="form-label fw-bold">
                <i className="fas fa-tags me-2"></i>
                GL Category:
              </label>
              <input
                type="text"
                id="glCategory"
                className="form-control"
                value={glCategory}
                onChange={(e) => setGlCategory(e.target.value)}
                placeholder="e.g., WA01"
              />
              <small className="form-text text-muted">GL Category code</small>
            </div>
          </div>
        </div>
      </div>

      {/* Action Panel */}
      <div className="card mb-4">
        <div className="card-header bg-secondary text-white">
          <h5 className="mb-0">
            <i className="fas fa-tools me-2"></i>
            JDE Item Master Review - Missing ingredients
          </h5>
        </div>
        <div className="card-body">
          <div className="d-flex flex-wrap gap-2 align-items-center">
            <button 
              className="btn btn-primary d-flex align-items-center"
              onClick={fetchItemMasterData}
              disabled={loading}
            >
              {loading ? (
                <>
                  <i className="fas fa-spinner fa-spin me-2"></i>
                  Refreshing...
                </>
              ) : (
                <>
                  <i className="fas fa-sync-alt me-2"></i>
                  Refresh Data
                </>
              )}
            </button>
            <button
              className="btn btn-success d-flex align-items-center"
              onClick={handleDownloadExcel}
              disabled={loading || itemMasterData.length === 0}
            >
              <i className="fas fa-download me-2"></i>
              Download Excel
            </button>
          </div>
        </div>
      </div>
      
      {/* Summary Statistics */}
      {!loading && itemMasterData.length > 0 && (
        <div className="row mb-4">
          <div className="col-md-8">
            <div className="card">
              <div className="card-header bg-secondary text-white">
                <h5 className="mb-0">
                  <i className="fas fa-chart-bar me-2"></i>
                  Item Statistics
                </h5>
              </div>
              <div className="card-body">
                <BarChart
                  data={[
                    {
                      label: 'Total Items',
                      value: itemMasterData.length,
                      color: '#6c757d'
                    },
                    {
                      label: 'Existing in Bakery-System',
                      value: itemMasterData.filter(item => item.exists_in_bakery_system).length,
                      color: '#198754'
                    },
                    {
                      label: 'Missing in Bakery-System',
                      value: itemMasterData.filter(item => !item.exists_in_bakery_system).length,
                      color: '#dc3545'
                    },
                    {
                      label: 'Can Create',
                      value: itemMasterData.filter(item => item.can_create).length,
                      color: '#0d6efd'
                    }
                  ]}
                  title="JDE Item Master Overview"
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
                  <div className="col-12 mb-2">
                    <h4 className="text-secondary mb-0">{itemMasterData.length}</h4>
                    <small className="text-muted">Total Items</small>
                  </div>
                  <div className="col-6 mb-2">
                    <h4 className="text-success mb-0">{itemMasterData.filter(item => item.exists_in_bakery_system).length}</h4>
                    <small className="text-muted">Existing</small>
                  </div>
                  <div className="col-6 mb-2">
                    <h4 className="text-danger mb-0">{itemMasterData.filter(item => !item.exists_in_bakery_system).length}</h4>
                    <small className="text-muted">Missing</small>
                  </div>
                  <div className="col-12">
                    <h4 className="text-primary mb-0">{itemMasterData.filter(item => item.can_create).length}</h4>
                    <small className="text-muted">Can Create</small>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
      
      {/* Data Table */}
      {itemMasterData.length > 0 && (
        <div className="card">
          <div className="card-header bg-secondary text-white">
            <h5 className="mb-0">
              <i className="fas fa-table me-2"></i>
              Data Results ({itemMasterData.length} items)
            </h5>
          </div>
          <div className="card-body p-0">
            <div className="table-responsive" style={{ maxHeight: '600px', overflowY: 'auto' }}>
              <table className="table table-bordered table-sm table-striped mb-0">
                <thead className="table-dark sticky-top">
                  <tr>
                    <th>Item Number</th>
                    <th>Short Item Number</th>
                    <th>Product Name</th>
                    <th>Description</th>
                    <th>Stocking Type</th>
                    <th>Item Type</th>
                    <th>GL Class</th>
                    <th>JDE UOM</th>
                    <th>Status</th>
                    <th>Ingredient ID</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {itemMasterData.map((item, index) => (
                    <tr key={index} className={item.exists_in_bakery-system ? 'table-success' : 'table-warning'}>
                      <td>{item.item_number}</td>
                      <td>{item.short_item_number}</td>
                      <td>{item.product_name}</td>
                      <td>{item.description}</td>
                      <td>{item.jde_stocking_type}</td>
                      <td>{item.jde_item_type}</td>
                      <td>{item.jde_gl_class}</td>
                      <td>{item.jde_uom || 'N/A'}</td>
                      <td>{getStatusBadge(item.status)}</td>
                      <td>{item.ingredient_id || 'N/A'}</td>
                      <td>
                        <div className="btn-group" role="group">
                          {item.can_create ? (
                            <button
                              className="btn btn-primary btn-sm d-flex align-items-center"
                              onClick={() => handleCreateIngredient(item.product_name, item.raw_jde_data)}
                              disabled={creating[item.product_name]}
                            >
                              {creating[item.product_name] ? (
                                <>
                                  <i className="fas fa-spinner fa-spin me-1"></i>
                                  Creating...
                                </>
                              ) : (
                                <>
                                  <i className="fas fa-plus me-1"></i>
                                  Create
                                </>
                              )}
                            </button>
                          ) : null}
                          {item.exists_in_bakery-system ? (
                            <>
                              <button
                                className="btn btn-warning btn-sm d-flex align-items-center"
                                onClick={() => handlePatchIngredient(item.product_name, item.raw_jde_data)}
                                disabled={patching[item.product_name]}
                              >
                                {patching[item.product_name] ? (
                                  <>
                                    <i className="fas fa-spinner fa-spin me-1"></i>
                                    Patching...
                                  </>
                                ) : (
                                  <>
                                    <i className="fas fa-edit me-1"></i>
                                    Patch
                                  </>
                                )}
                              </button>
                              {item.ingredient_id ? (
                                <button
                                  className="btn btn-danger btn-sm d-flex align-items-center"
                                  onClick={() => handleDeleteIngredient(item.product_name, item.ingredient_id)}
                                  disabled={deleting[item.product_name]}
                                >
                                  {deleting[item.product_name] ? (
                                    <>
                                      <i className="fas fa-spinner fa-spin me-1"></i>
                                      Deleting...
                                    </>
                                  ) : (
                                    <>
                                      <i className="fas fa-trash me-1"></i>
                                      Delete
                                    </>
                                  )}
                                </button>
                              ) : null}
                            </>
                          ) : null}
                          {!item.can_create && !item.exists_in_bakery-system ? (
                            <span className="text-muted">N/A</span>
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
      )}
      
      {/* No Data Message */}
      {itemMasterData.length === 0 && !loading && (
        <div className="card">
          <div className="card-body text-center py-5">
            <i className="fas fa-info-circle fa-3x text-info mb-3"></i>
            <h5 className="text-muted">No Data Available</h5>
            <p className="text-muted">Try refreshing the data or check the backend connection.</p>
          </div>
        </div>
      )}
      
      {/* Legend */}
      <div className="card mt-3">
        <div className="card-body">
          <h6 className="card-title">
            <i className="fas fa-info-circle me-2"></i>
            Status Legend
          </h6>
          <div className="d-flex flex-wrap gap-2">
            <span className="badge bg-success">
              <i className="fas fa-check me-1"></i>
              Exists in Bakery-System
            </span>
            <small className="text-muted">- Ingredient already exists in Bakery-System system</small>
            <span className="badge bg-danger ms-3">
              <i className="fas fa-times me-1"></i>
              Missing in Bakery-System
            </span>
            <small className="text-muted">- Ingredient needs to be created in Bakery-System system</small>
          </div>
        </div>
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

export default JdeItemMasterReview;
