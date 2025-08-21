// src/components/PivotTable.js

import React, { useState, useEffect } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import { API_BASE_URL } from '../config/api';

function PivotTable() {
  const [pivotReport, setPivotReport] = useState([]);

  useEffect(() => {
    fetch(`${API_BASE_URL}/data/pivot_report`)
      .then(response => response.json())
      .then(data => setPivotReport(data.data))
      .catch(error => console.error('Error fetching data:', error));
  }, []);

  return (
    <div className="container">
      <h2>Pivot Table</h2>
      {pivotReport.length > 0 && (
        <table className="table table-bordered table-sm">
          <thead>
            <tr>
              {Object.keys(pivotReport[0]).map(key => (
                <th key={key}>{key}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pivotReport.map((item, index) => (
              <tr key={index}>
                {Object.values(item).map(value => (
                  <td key={value}>{value}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

export default PivotTable;
