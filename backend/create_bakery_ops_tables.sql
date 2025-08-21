-- Bakery Operations Database Schema
-- This script creates the necessary tables for the bakery operations system

-- Create schema versions tracking table (managed by schema_manager.py)
CREATE TABLE IF NOT EXISTS schema_versions (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    schema_definition JSONB NOT NULL,
    version_number INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system',
    description TEXT,
    UNIQUE(table_name, version_number)
);

CREATE INDEX IF NOT EXISTS idx_schema_versions_table_version 
ON schema_versions(table_name, version_number DESC);

-- Bakery Operations Products (replaces Bakery-System ingredients)
CREATE TABLE IF NOT EXISTS bakery_ops_products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(100) UNIQUE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    product_category VARCHAR(100) DEFAULT 'Ingredient',
    inventory_unit VARCHAR(10) DEFAULT 'EA',
    jde_item_number VARCHAR(50),
    jde_short_item VARCHAR(25),
    archived BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_sync_at TIMESTAMP
);

-- Bakery Operations Inventory Batches
CREATE TABLE IF NOT EXISTS bakery_ops_batches (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(100) UNIQUE NOT NULL,
    product_id VARCHAR(100) NOT NULL REFERENCES bakery_ops_products(product_id),
    batch_number VARCHAR(100),
    manufacturer_batch_id VARCHAR(100),
    lot_number VARCHAR(100),
    quantity_on_hand DECIMAL(15,4),
    unit VARCHAR(10),
    cost_on_hand DECIMAL(15,2),
    expiration_date DATE,
    vessel_code VARCHAR(50),
    depleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory Movements/Adjustments
CREATE TABLE IF NOT EXISTS bakery_ops_movements (
    id SERIAL PRIMARY KEY,
    movement_id VARCHAR(100) UNIQUE NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    batch_id VARCHAR(100),
    movement_type VARCHAR(50) NOT NULL, -- USAGE, RECEIPT, ADJUSTMENT
    quantity DECIMAL(15,4) NOT NULL,
    unit VARCHAR(10),
    movement_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reason TEXT,
    vessel_code VARCHAR(50),
    lot_number VARCHAR(100),
    jde_transaction_id VARCHAR(100),
    jde_document_number VARCHAR(50),
    dispatched_to_jde BOOLEAN DEFAULT FALSE,
    dispatch_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- JDE Transaction Tracking (enhanced from existing)
CREATE TABLE IF NOT EXISTS jde_transactions (
    id SERIAL PRIMARY KEY,
    unique_transaction_id VARCHAR(100) UNIQUE NOT NULL,
    jde_document_number VARCHAR(50),
    jde_item_number VARCHAR(50),
    jde_short_item VARCHAR(25),
    transaction_type VARCHAR(20), -- II (Inventory Issues), IR (Inventory Receipts)
    quantity DECIMAL(15,4),
    unit VARCHAR(10),
    lot_number VARCHAR(100),
    vessel_code VARCHAR(50),
    transaction_date TIMESTAMP,
    business_unit VARCHAR(10),
    branch_plant VARCHAR(10),
    dispatched_to_bakery_ops BOOLEAN DEFAULT FALSE,
    bakery_ops_movement_id VARCHAR(100),
    dispatch_date TIMESTAMP,
    s3_key VARCHAR(500), -- Reference to S3 storage
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- S3 Data Lake Metadata
CREATE TABLE IF NOT EXISTS s3_data_metadata (
    id SERIAL PRIMARY KEY,
    s3_key VARCHAR(500) UNIQUE NOT NULL,
    bucket_name VARCHAR(100) NOT NULL,
    data_type VARCHAR(100) NOT NULL, -- dispatch_type from S3 helper
    file_size_bytes BIGINT,
    record_count INTEGER,
    partition_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_accessed TIMESTAMP,
    tags JSONB
);

-- Dispatch Audit Trail
CREATE TABLE IF NOT EXISTS dispatch_audit (
    id SERIAL PRIMARY KEY,
    dispatch_id VARCHAR(100) UNIQUE NOT NULL,
    direction VARCHAR(20) NOT NULL, -- TO_BAKERY_OPS, FROM_BAKERY_OPS, TO_JDE, FROM_JDE
    source_system VARCHAR(50) NOT NULL,
    target_system VARCHAR(50) NOT NULL,
    record_count INTEGER,
    success_count INTEGER,
    failure_count INTEGER,
    dispatch_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    s3_key VARCHAR(500),
    error_details JSONB,
    processing_time_seconds INTEGER
);

-- Facility Configuration (replaces winery concept)
CREATE TABLE IF NOT EXISTS facility_config (
    id SERIAL PRIMARY KEY,
    facility_id VARCHAR(100) UNIQUE NOT NULL,
    facility_name VARCHAR(255) NOT NULL,
    facility_type VARCHAR(100) DEFAULT 'Bakery',
    active BOOLEAN DEFAULT TRUE,
    jde_business_unit VARCHAR(10),
    bakery_ops_endpoint VARCHAR(500),
    s3_prefix VARCHAR(200),
    configuration JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_bakery_ops_products_jde_item 
ON bakery_ops_products(jde_item_number);

CREATE INDEX IF NOT EXISTS idx_bakery_ops_batches_product 
ON bakery_ops_batches(product_id);

CREATE INDEX IF NOT EXISTS idx_bakery_ops_movements_product_date 
ON bakery_ops_movements(product_id, movement_date);

CREATE INDEX IF NOT EXISTS idx_jde_transactions_doc_item 
ON jde_transactions(jde_document_number, jde_item_number);

CREATE INDEX IF NOT EXISTS idx_jde_transactions_dispatch 
ON jde_transactions(dispatched_to_bakery_ops, dispatch_date);

CREATE INDEX IF NOT EXISTS idx_s3_metadata_type_date 
ON s3_data_metadata(data_type, partition_date);

CREATE INDEX IF NOT EXISTS idx_dispatch_audit_date_direction 
ON dispatch_audit(dispatch_date, direction);

-- Insert default facility configuration
INSERT INTO facility_config (facility_id, facility_name, facility_type, jde_business_unit, s3_prefix)
VALUES ('bakery-001', 'Main Bakery Facility', 'Bakery', '1110', 'bakery-001')
ON CONFLICT (facility_id) DO NOTHING;

-- Update triggers for automatic timestamp updates
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply update triggers to tables with updated_at columns
CREATE TRIGGER update_bakery_ops_products_updated_at 
    BEFORE UPDATE ON bakery_ops_products 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_bakery_ops_batches_updated_at 
    BEFORE UPDATE ON bakery_ops_batches 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_jde_transactions_updated_at 
    BEFORE UPDATE ON jde_transactions 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_facility_config_updated_at 
    BEFORE UPDATE ON facility_config 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
