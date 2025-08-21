import React, { useState } from 'react';
import ErrorModal from './ErrorModal';
import { fetchWithAuth } from '../utils/auth';
import { API_BASE_URL } from '../config/api';

const AdvancedPatchForm = () => {
    const [searchName, setSearchName] = useState('');
    const [ingredientData, setIngredientData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [patching, setPatching] = useState(false);
    const [error, setError] = useState(null);
    const [showErrorModal, setShowErrorModal] = useState(false);
    const [successMessage, setSuccessMessage] = useState('');
    
    // Form fields for patching
    const [patchData, setPatchData] = useState({
        name: '',
        additionUnit: '',
        inventoryUnit: ''
    });

    const handleSearch = async () => {
        if (!searchName.trim()) {
            setError('Please enter an Ingredient name to search');
            setShowErrorModal(true);
            return;
        }

        setLoading(true);
        setSuccessMessage('');
        
        try {
            const response = await fetchWithAuth(
                `${API_BASE_URL}/search/Ingredient?name=${encodeURIComponent(searchName.trim())}`
            );
            
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
            }
            
            const result = await response.json();
            
            // Debug logging
            console.log('Search API response:', result);
            
            // Handle the actual backend response format
            const ingredientResult = result.ingredient || result.data || result;
            
            console.log('Extracted Ingredient data:', ingredientResult);
            
            if (ingredientResult && ingredientResult._id) {
                console.log('Setting Ingredient data with ID:', ingredientResult._id);
                setIngredientData(ingredientResult);
                // Pre-populate form with current values
                setPatchData({
                    name: ingredientResult.name || '',
                    additionUnit: ingredientResult.categoryFields?.additionUnit || '',
                    inventoryUnit: ingredientResult.inventoryUnit || ''
                });
            } else {
                console.log('No valid Ingredient data found in response');
                setIngredientData(null);
                setError('No Ingredient found with that name');
                setShowErrorModal(true);
            }
        } catch (error) {
            console.error('Error searching for Ingredient:', error);
            setError(error.message);
            setShowErrorModal(true);
            setIngredientData(null);
        } finally {
            setLoading(false);
        }
    };

    const handlePatch = async () => {
        if (!ingredientData || !ingredientData._id) {
            setError('No Ingredient selected for patching');
            setShowErrorModal(true);
            return;
        }

        // Validate required fields
        if (!patchData.name.trim()) {
            setError('Ingredient name is required');
            setShowErrorModal(true);
            return;
        }

        if (!patchData.inventoryUnit) {
            setError('Inventory unit is required');
            setShowErrorModal(true);
            return;
        }

        setPatching(true);
        setSuccessMessage('');

        try {
            // Prepare patch payload to match backend expectations
            const patchPayload = {
                ingredient_name: ingredientData.name, // Use current name to identify the Ingredient
                new_name: patchData.name.trim() !== ingredientData.name ? patchData.name.trim() : undefined,
                new_inventory_unit: patchData.inventoryUnit !== ingredientData.inventoryUnit ? patchData.inventoryUnit : undefined,
                new_addition_unit: patchData.additionUnit !== (ingredientData.categoryFields?.additionUnit || '') ? (patchData.additionUnit || patchData.inventoryUnit) : undefined
            };
            
            // Remove undefined values
            Object.keys(patchPayload).forEach(key => {
                if (patchPayload[key] === undefined) {
                    delete patchPayload[key];
                }
            });
            
            console.log('Patch payload:', patchPayload);

            const response = await fetchWithAuth(
                `${API_BASE_URL}/patch/ingredient/advanced`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(patchPayload)
                }
            );
            
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
            }
            
            const result = await response.json();
            setSuccessMessage(
                `Successfully patched Ingredient: ${result.updated_name}. ` +
                `Inventory Unit: ${result.updated_inventory_unit}, ` +
                `Addition Unit: ${result.updated_addition_unit}`
            );

            // Update the displayed data with new values
            setIngredientData(prev => ({
                ...prev,
                name: result.updated_name,
                inventoryUnit: result.updated_inventory_unit,
                categoryFields: {
                    ...prev.categoryFields,
                    additionUnit: result.updated_addition_unit
                }
            }));

        } catch (error) {
            console.error('Error patching Ingredient:', error);
            setError(error.message);
            setShowErrorModal(true);
        } finally {
            setPatching(false);
        }
    };

    const handleInputChange = (field, value) => {
        setPatchData(prev => ({
            ...prev,
            [field]: value
        }));
    };

    const handleReset = () => {
        setSearchName('');
        setIngredientData(null);
        setPatchData({
            name: '',
            additionUnit: '',
            inventoryUnit: ''
        });
        setSuccessMessage('');
        setError(null);
    };

    return (
        <div className="container-fluid" style={{overflowX: 'auto', minWidth: '1000px'}}>
            <div className="row mb-3">
                <div className="col">
                    <h2 className="text-dark">
                        <i className="fas fa-edit me-2"></i>
                        Advanced Ingredient Patch
                    </h2>
                    <p className="text-muted">
                        Search for an Ingredient by name and modify its name and units of measure
                    </p>
                </div>
            </div>

            {/* Success Message */}
            {successMessage && (
                <div className="row mb-3">
                    <div className="col">
                        <div className="alert alert-success alert-dismissible fade show" role="alert">
                            <i className="fas fa-check-circle me-2"></i>
                            {successMessage}
                            <button 
                                type="button" 
                                className="btn-close" 
                                onClick={() => setSuccessMessage('')}
                            ></button>
                        </div>
                    </div>
                </div>
            )}

            {/* Search Section */}
            <div className="row mb-4">
                <div className="col">
                    <div className="card">
                        <div className="card-header bg-info text-white">
                            <h5 className="mb-0">
                                <i className="fas fa-search me-2"></i>
                                Search Ingredient
                            </h5>
                        </div>
                        <div className="card-body">
                            <div className="row">
                                <div className="col-md-8">
                                    <div className="input-group">
                                        <span className="input-group-text">
                                            <i className="fas fa-flask"></i>
                                        </span>
                                        <input
                                            type="text"
                                            className="form-control"
                                            placeholder="Enter Ingredient name to search..."
                                            value={searchName}
                                            onChange={(e) => setSearchName(e.target.value)}
                                            onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
                                        />
                                    </div>
                                </div>
                                <div className="col-md-4">
                                    <div className="d-grid gap-2 d-md-flex">
                                        <button 
                                            className="btn btn-primary" 
                                            onClick={handleSearch}
                                            disabled={loading}
                                        >
                                            {loading ? (
                                                <>
                                                    <div className="spinner-border spinner-border-sm me-2" role="status"></div>
                                                    Searching...
                                                </>
                                            ) : (
                                                <>
                                                    <i className="fas fa-search me-1"></i>
                                                    Search
                                                </>
                                            )}
                                        </button>
                                        <button 
                                            className="btn btn-secondary" 
                                            onClick={handleReset}
                                        >
                                            <i className="fas fa-refresh me-1"></i>
                                            Reset
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Current Ingredient Data */}
            {ingredientData && (
                <div className="row mb-4">
                    <div className="col">
                        <div className="card">
                            <div className="card-header bg-success text-white">
                                <h5 className="mb-0">
                                    <i className="fas fa-info-circle me-2"></i>
                                    Current Ingredient Data
                                </h5>
                            </div>
                            <div className="card-body">
                                <div className="row">
                                    <div className="col-md-6">
                                        <h6 className="text-primary">Basic Information</h6>
                                        <table className="table table-sm">
                                            <tbody>
                                                <tr>
                                                    <td><strong>ID:</strong></td>
                                                    <td><code>{ingredientData._id}</code></td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Name:</strong></td>
                                                    <td>{ingredientData.name}</td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Inventory Unit:</strong></td>
                                                    <td><span className="badge bg-info">{ingredientData.inventoryUnit}</span></td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Addition Unit:</strong></td>
                                                    <td><span className="badge bg-warning">{ingredientData.categoryFields?.additionUnit || 'N/A'}</span></td>
                                                </tr>
                                                <tr>
                                                    <td><strong>On Hand:</strong></td>
                                                    <td>
                                                        {ingredientData.onHand ? (
                                                            <span className="badge bg-secondary">
                                                                {ingredientData.onHand.amount} ({ingredientData.onHand.batches} batches)
                                                            </span>
                                                        ) : 'N/A'}
                                                    </td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                    <div className="col-md-6">
                                        <h6 className="text-primary">Additional Information</h6>
                                        <table className="table table-sm">
                                            <tbody>
                                                <tr>
                                                    <td><strong>Product Type:</strong></td>
                                                    <td>{ingredientData.productType?.name || 'N/A'}</td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Category:</strong></td>
                                                    <td>{ingredientData.productType?.category || 'N/A'}</td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Public ID:</strong></td>
                                                    <td><code>{ingredientData.publicId || 'N/A'}</code></td>
                                                </tr>
                                                <tr>
                                                    <td><strong>Archived:</strong></td>
                                                    <td>
                                                        <span className={`badge ${!ingredientData.archived ? 'bg-success' : 'bg-danger'}`}>
                                                            {!ingredientData.archived ? 'Active' : 'Archived'}
                                                        </span>
                                                    </td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* Patch Form */}
            {ingredientData && (
                <div className="row mb-4">
                    <div className="col">
                        <div className="card">
                            <div className="card-header bg-warning text-dark">
                                <h5 className="mb-0">
                                    <i className="fas fa-edit me-2"></i>
                                    Modify Ingredient
                                </h5>
                            </div>
                            <div className="card-body">
                                <div className="row">
                                    <div className="col-md-12 mb-3">
                                        <label htmlFor="patchName" className="form-label">
                                            <strong>Ingredient Name</strong>
                                        </label>
                                        <input
                                            type="text"
                                            className="form-control"
                                            id="patchName"
                                            value={patchData.name}
                                            onChange={(e) => handleInputChange('name', e.target.value)}
                                            placeholder="Enter new Ingredient name"
                                        />
                                    </div>
                                    <div className="col-md-6 mb-3">
                                        <label htmlFor="inventoryUnit" className="form-label">
                                            <strong>Inventory Unit</strong>
                                        </label>
                                        <input
                                            type="text"
                                            className="form-control"
                                            id="inventoryUnit"
                                            value={patchData.inventoryUnit}
                                            onChange={(e) => handleInputChange('inventoryUnit', e.target.value)}
                                            placeholder="Enter new Ingredient inventoryUnit"
                                        />
                                    </div>
                                    <div className="col-md-6 mb-3">
                                        <label htmlFor="additionUnit" className="form-label">
                                            <strong>Addition Unit</strong> 
                                            <small className="text-muted">(defaults to inventory unit)</small>
                                        </label>
                                        <input
                                            type="text"
                                            className="form-control"
                                            id="additionUnit"
                                            value={patchData.additionUnit}
                                            onChange={(e) => handleInputChange('additionUnit', e.target.value)}
                                            placeholder="Enter new Ingredient additionUnit"
                                        />
                                    </div>
                                </div>
                                <div className="row">
                                    <div className="col">
                                        <button 
                                            className="btn btn-warning btn-lg me-2" 
                                            onClick={handlePatch}
                                            disabled={patching || !patchData.name.trim() || !patchData.inventoryUnit}
                                        >
                                            {patching ? (
                                                <>
                                                    <div className="spinner-border spinner-border-sm me-2" role="status"></div>
                                                    Patching...
                                                </>
                                            ) : (
                                                <>
                                                    <i className="fas fa-save me-1"></i>
                                                    Apply Patch
                                                </>
                                            )}
                                        </button>
                                        <small className="text-muted">
                                            This will update the Ingredient name and units of measure in Bakery-System
                                        </small>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            <ErrorModal
                show={showErrorModal}
                onHide={() => setShowErrorModal(false)}
                title="Error"
                message={error}
            />
        </div>
    );
};

export default AdvancedPatchForm;
