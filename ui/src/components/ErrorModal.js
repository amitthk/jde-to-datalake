// src/components/ErrorModal.js

import React from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';

function ErrorModal({ show, onHide, title, error }) {
  if (!show) return null;

  return (
    <div className="modal fade show" style={{ display: 'block' }} tabIndex="-1">
      <div className="modal-overlay" style={{ 
        position: 'fixed', 
        top: 0, 
        left: 0, 
        width: '100%', 
        height: '100%', 
        backgroundColor: 'rgba(0,0,0,0.5)', 
        zIndex: 1040 
      }} onClick={onHide}></div>
      <div className="modal-dialog modal-lg" style={{ zIndex: 1050 }}>
        <div className="modal-content">
          <div className="modal-header bg-danger text-white">
            <h5 className="modal-title">{title || 'Error'}</h5>
            <button type="button" className="btn-close btn-close-white" onClick={onHide}></button>
          </div>
          <div className="modal-body">
            <div className="alert alert-danger mb-0">
              <pre style={{ 
                whiteSpace: 'pre-wrap', 
                wordBreak: 'break-word',
                fontSize: '0.9em',
                marginBottom: 0
              }}>
                {error}
              </pre>
            </div>
          </div>
          <div className="modal-footer">
            <button type="button" className="btn btn-secondary" onClick={onHide}>
              Close
            </button>
            <button 
              type="button" 
              className="btn btn-primary" 
              onClick={() => {
                navigator.clipboard.writeText(error).then(() => {
                  alert('Error details copied to clipboard');
                }).catch(() => {
                  console.warn('Failed to copy to clipboard');
                });
              }}
            >
              Copy Error
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default ErrorModal;
