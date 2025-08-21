// src/components/CompareData.js

import React, { useState, useEffect } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import { API_BASE_URL } from '../config/api';

function CompareData() {
  const [joinedDf2Data, setJoinedDf2Data] = useState([]);

  const handleOverride = (id) => {
    if (window.confirm('Are you sure you want to override the bakery-system value with JDE value?')) {
      // Add your logic here to update the bakery-system API
      alert('Override confirmed');
    }
  };

  // Filter and get the row with the largest Quantity On Hand (non-zero) per each description
  const getFilteredData = () => {
    // Filter out rows where Quantity On Hand is 0, null, or undefined
    const filteredData = joinedDf2Data.filter(item => 
      item['Short Item No'] !== null && 
      item['Quantity On Hand'] && 
      parseFloat(item['Quantity On Hand']) > 0
    );

    if (filteredData.length === 0) return [];

    // Group by description and find the maximum Quantity On Hand for each description
    const groupedByDescription = {};
    
    filteredData.forEach(item => {
      const description = item['description'] || 'Unknown';
      const quantity = parseFloat(item['Quantity On Hand']) || 0;
      
      if (!groupedByDescription[description] || 
          quantity > parseFloat(groupedByDescription[description]['Quantity On Hand'])) {
        groupedByDescription[description] = item;
      }
    });

    // Return all the maximum quantity items for each description
    return Object.values(groupedByDescription);
  };

  useEffect(() => {
    fetch(`${API_BASE_URL}/data/joined_df2`)
      .then(response => response.json())
      .then(data => setJoinedDf2Data(data.data))
      .catch(error => console.error('Error fetching data:', error));
  }, []);

  return (
    <div className="container-fluid">
      <h2>Compare Data</h2>
      {joinedDf2Data.length > 0 && (
        <div style={{ overflowY: 'auto',  width: 'auto', height: '600px' }}>
        <table className="table table-bordered table-sm table-small">
          <thead>
            <tr>
              <th rowSpan="2">Description</th>
              <th colspan="7">JDE</th>
              <th colspan="9">Bakery-System</th>
              <th rowSpan="2">Override Bakery-System Value?</th>
            </tr>
            <tr>
              <th>UM </th>
              <th>Short Item No</th>
              <th>PU UM</th>
              <th>2nd Item Number</th>
              <th>Date Updated</th>
              <th>Quantity On Hand</th>
              <th>Business Unit</th>
              <th>_id</th>
              <th>publicId</th>
              <th>inventory Unit</th>
              <th>archived</th>
              <th>onHand amount</th>
              <th>onHand batches</th>
              <th>addition Unit</th>
              <th>addition RateUnit</th>
              <th>addition RateValue</th>
            </tr>
          </thead>
          <tbody>
            {getFilteredData().map((item, index) => (
              <tr key={index}>
              <td>
                {item['description']}
              </td>
              <td>
                {item['UM']}
              </td>
              <td>
                {item['Short Item No']}
              </td>
              <td>
                {item['PU UM']}
              </td>
              <td>
                {item['2nd Item Number']}
              </td>
              <td>
                {item['Date Updated']}
              </td>
              <td>
                {item['Quantity On Hand']}
              </td>
              <td>
                {item['Business Unit']}
              </td>
              <td>
                {item['_id']}
              </td>
              <td>
                {item['publicId']}
              </td>
              <td>
                {item['inventory_Unit']}
              </td>
              <td>
                {item['archived']}
              </td>
              <td>
                {item['onHand_amount']}
              </td>
              <td>
                {item['onHand_batches']}
              </td>
              <td>
                {item['categoryFields_additionUnit']}
              </td>
              <td>
                {item['categoryFields_additionRateUnit']}
              </td>
              <td>
                {item['categoryFields_additionRateValue']}
              </td>
                <td>
                  <button
                    className="btn btn-primary"
                    onClick={() => handleOverride(index)}
                  >
                    Override
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      )}
    </div>
  );
}

export default CompareData;
