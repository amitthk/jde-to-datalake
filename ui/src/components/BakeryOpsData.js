// src/components/BakeryOpsData.js

import React, { useState, useEffect } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import { API_BASE_URL } from '../config/api';

function BakeryOpsData() {
  const [data, setData] = useState([]);

  useEffect(() => {
    fetch(`${API_BASE_URL}/data/df_bakery_ops_expanded`)
      .then(response => response.json())
      .then(data => setData(data.data))
      .catch(error => console.error('Error fetching data:', error));
  }, []);

  return (
    <div className="container">
      <h2>Bakery Operations Data</h2>
      {data.length > 0 && (
<table className="table table-bordered table-sm">
  <thead>
    <tr>
      {Object.keys(data[0]).map(key => (
        <th key={key}>{key}</th>
      ))}
    </tr>
  </thead>
  <tbody>
    {data.map((item, rowIndex) => (
      <tr key={rowIndex}>
        {Object.values(item).map((value, colIndex) => (
          <td key={`${rowIndex}-${colIndex}`}>{value}</td>
        ))}
      </tr>
    ))}
  </tbody>
</table>
      )}
    </div>
  );
}

export default BakeryOpsData;
