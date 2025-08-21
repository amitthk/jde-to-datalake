#!/usr/bin/env python
# coding: utf-8

import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
import os
import time
from utility import retry_request, convert_unit, is_jde, convert_rate_unit, convert_unit_quantity, retry_request_lru, invalidate_lru_cache, validate_unit_mapping, get_db_connection
from functools import lru_cache
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from decimal import Decimal
import numpy as np

def get_jde_cardex_with_comparison(bu: str, days_back: int = 5) -> dict:
    """Fetch JDE cardex data and compare with Bakery Operations - streamlined approach"""
    load_dotenv()
    
    # Calculate date string
    today = datetime.now()
    start_date = today - timedelta(days=days_back)
    date_str = start_date.strftime('%d/%m/%Y')
    
    print(f"Fetching JDE cardex data for BU {bu} since {date_str}")
    
    # Get JDE data
    from jde_helper import get_latest_jde_cardex
    jde_data = get_latest_jde_cardex(bu, date_str)
    if not jde_data or 'ServiceRequest1' not in jde_data:
        print(f"No JDE data found for BU {bu}")
        return None
    
    # Extract JDE transaction data
    jde_transactions = jde_data['ServiceRequest1']['fs_DATABROWSE_V4111A']['data']['gridData']['rowset']
    df_jde = pd.DataFrame(jde_transactions)
    
    # Get Bakery Operations data
    from bakery_ops_helper import get_data_from_bakery_operations
    bakery_ops_data = get_data_from_bakery_operations()
    if not bakery_ops_data:
        print("No Bakery Operations data found")
        return None
    
    df_bakery_ops = pd.DataFrame(bakery_ops_data)
    
    # Calculate total JDE quantity for each product name
    total_jde_quantity_map = {}
    for _, jde_row in df_jde.iterrows():
        product_name = str(jde_row['F4111_LITM']) if pd.notnull(jde_row['F4111_LITM']) else None
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(__file__)))
        from utility import preserve_quantity_precision
        jde_quantity = preserve_quantity_precision(jde_row['F4111_TRQT']) if pd.notnull(jde_row['F4111_TRQT']) else 0
        if product_name:
            total_jde_quantity_map[product_name.lower()] = total_jde_quantity_map.get(product_name.lower(), 0) + jde_quantity

    # Calculate total Bakery Operations quantity on hand for each product name
    total_bakery_ops_quantity_map = {}
    if not df_bakery_ops.empty and 'productName' in df_bakery_ops.columns and 'onHand' in df_bakery_ops.columns:
        for _, row in df_bakery_ops.iterrows():
            pname = str(row['productName']) if pd.notnull(row['productName']) else None
            on_hand = row.get('onHand', {})
            qty = 0
            if isinstance(on_hand, dict):
                qty = on_hand.get('amount', 0)
            if pname:
                total_bakery_ops_quantity_map[pname.lower()] = total_bakery_ops_quantity_map.get(pname.lower(), 0) + (qty if qty is not None else 0)

    # Process and compare data
    comparison_data = []
    
    for _, jde_row in df_jde.iterrows():
        product_name = str(jde_row['F4111_LITM']) if pd.notnull(jde_row['F4111_LITM']) else None
        transaction_id = str(jde_row['F4111_DOC']) if pd.notnull(jde_row['F4111_DOC']) else None
        lot_number = str(jde_row['F4111_LOTN']) if pd.notnull(jde_row['F4111_LOTN']) else None
        batch_name = product_name if lot_number is None else f"{product_name}_{lot_number}"
        jde_quantity = preserve_quantity_precision(jde_row['F4111_TRQT']) if pd.notnull(jde_row['F4111_TRQT']) else 0
        
        # Find matching product in Bakery Operations
        bakery_ops_match = df_bakery_ops[df_bakery_ops['productName'].str.lower() == product_name.lower()] if product_name else pd.DataFrame()
        bakery_ops_quantity = 0
        bakery_ops_batches = []
        bakery_ops_id = None
        dispatched = False
        
        if not bakery_ops_match.empty:
            bakery_ops_product = bakery_ops_match.iloc[0]
            bakery_ops_id = bakery_ops_product.get('product_id')
            # Check onHand data
            on_hand = bakery_ops_product.get('onHand', {})
            if isinstance(on_hand, dict):
                bakery_ops_quantity = on_hand.get('amount', 0)
                bakery_ops_batches = on_hand.get('batches', [])
            if not isinstance(bakery_ops_batches, list):
                bakery_ops_batches = []
            for batch in bakery_ops_batches:
                if isinstance(batch, dict) and batch.get('batchNumber') == batch_name:
                    dispatched = True
                    break
        
        status = "Missing in Bakery Ops"
        if bakery_ops_match.empty:
            status = "Product Not Found"
        elif dispatched:
            status = "Dispatched"
        elif bakery_ops_quantity > 0:
            status = "Partial Match"
        
        # Add total quantities
        total_jde_quantity = total_jde_quantity_map.get(product_name.lower(), 0) if product_name else 0
        total_bakery_ops_quantity = total_bakery_ops_quantity_map.get(product_name.lower(), 0) if product_name else 0
        
        comparison_data.append({
            'transaction_id': transaction_id,
            'product_name': product_name,
            'batch_name': batch_name,
            'lot_number': lot_number,
            'jde_quantity': jde_quantity,
            'jde_unit': str(jde_row['F4111_TRUM']) if pd.notnull(jde_row['F4111_TRUM']) else '',
            'jde_date': str(jde_row['F4111_TRDJ']) if pd.notnull(jde_row['F4111_TRDJ']) else '',
            'bakery_ops_quantity': bakery_ops_quantity,
            'bakery_ops_batches_count': len(bakery_ops_batches) if isinstance(bakery_ops_batches, list) else 0,
            'bakery_ops_id': bakery_ops_id,
            'status': status,
            'dispatched': dispatched,
            'can_dispatch': not dispatched and product_name is not None,
            'raw_jde_data': jde_row.to_dict(),
            'total_jde_quantity': total_jde_quantity,
            'total_bakery_ops_quantity': total_bakery_ops_quantity
        })
    
    return {
        'comparison_data': comparison_data,
        'total_jde_records': len(df_jde),
        'total_bakery_ops_products': len(df_bakery_ops),
        'date_range': f"{date_str} to {today.strftime('%d/%m/%Y')}"
    }

def fetch_existing_product(product_name: str) -> dict:
    """Fetch a product by name from Bakery Operations"""
    load_dotenv()

    facility_id = os.getenv("FACILITY_ID")
    bakery_ops_base_url = os.getenv("BAKERY_OPS_BASE_URL")
    bakery_ops_api_token = os.getenv("BAKERY_OPS_API_TOKEN")
    
    if not all([facility_id, bakery_ops_base_url]):
        print("Missing required environment variables for Bakery Operations")
        return None
    
    headers = {'Content-Type': 'application/json'}
    if bakery_ops_api_token:
        headers['Authorization'] = f'Bearer {bakery_ops_api_token}'
    
    url = f'{bakery_ops_base_url}/facilities/{facility_id}/products'
    params = {
        'archived': False,
        'includeAccess': True,
        'includeBatches': True,
        'includeNotes': True,
        'offset': 0,
        'productCategory': 'Ingredient',
        'size': 200,
        'sort': 'productName:1',
        'q': product_name
    }

    try:
        response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                # Find exact match
                for product in data:
                    if product.get('productName', '').lower() == product_name.lower():
                        return product
                # Return first match if no exact match
                return data[0]
        else:
            print(f"Error fetching product: HTTP {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"Exception while fetching product: {e}")
    
    return None

def create_new_product_batch(product_id: str, batch_name: str) -> dict:
    """Create a new product batch in Bakery Operations"""
    load_dotenv()

    payload = {
        'product_id': product_id,
        'batchNumber': batch_name,
        'manufacturerBatchId': batch_name,
        'depleted': False,
        'onHand': None,
        'categoryFields': {'expirationDate': None},
        'expirationDate': None,
        'costOnHand': None,
        'tags': [],
        'notes': [{'text': f"Product ID: {product_id}, Batch: {batch_name}"}]
    }
    
    facility_id = os.getenv("FACILITY_ID")
    bakery_ops_base_url = os.getenv("BAKERY_OPS_BASE_URL")
    bakery_ops_api_token = os.getenv("BAKERY_OPS_API_TOKEN")
    
    headers = {'Content-Type': 'application/json'}
    if bakery_ops_api_token:
        headers['Authorization'] = f'Bearer {bakery_ops_api_token}'
    
    url = f'{bakery_ops_base_url}/facilities/{facility_id}/batches'
    
    try:
        response = requests.post(url, headers=headers, json=payload, verify=False, timeout=30)
        
        if response.status_code in [200, 201]:
            return response.json()
        else:
            print(f"Error creating batch: HTTP {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"Exception while creating batch: {e}")
    
    return None

# Import S3 helper for data lake storage
from s3_helper import s3_helper

def submit_product_batch_action(data: dict) -> list:
    """Generate the final payload for stock update - streamlined with existing JDE transaction tracking"""
    load_dotenv()

    df_json = data['ServiceRequest1']['fs_DATABROWSE_V4111A']['data']['gridData']['rowset']
    df = pd.DataFrame([row for row in df_json])
    facility_id = os.getenv("FACILITY_ID")
    bakery_ops_api_token = os.getenv("BAKERY_OPS_API_TOKEN")
    post_data = []
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    
    for index, row in df.iterrows():
        product_name = str(row['F4111_LITM']) if pd.notnull(row['F4111_LITM']) else None
        result = fetch_or_create_product(product_name, row)
        
        # Create batch name using product_name + "_" + F4111_LOTN (if exists)
        lot_number = str(row['F4111_LOTN']) if pd.notnull(row['F4111_LOTN']) else None
        batch_name = product_name if lot_number is None else f"{product_name}_{lot_number}"
        
        if result and result.get('product_id'):
            product_id = result.get('product_id')
            
            # Check if batch exists, create if not
            batch_result = None
            existing_batches = result.get('batches', [])
            batch_found = False
            
            for existing_batch in existing_batches:
                if existing_batch.get('batchNumber') == batch_name:
                    batch_result = existing_batch
                    batch_found = True
                    break
            
            if not batch_found:
                batch_result = create_new_product_batch(product_id, batch_name)
            
            if batch_result:
                # Prepare the stock update data
                stock_update = {
                    'product_id': product_id,
                    'batch_id': batch_result.get('_id'),
                    'product_name': product_name,
                    'batch_name': batch_name,
                    'quantity': preserve_quantity_precision(row['F4111_TRQT']) if pd.notnull(row['F4111_TRQT']) else 0,
                    'unit': str(row['F4111_TRUM']) if pd.notnull(row['F4111_TRUM']) else 'EA',
                    'transaction_id': str(row['F4111_DOC']) if pd.notnull(row['F4111_DOC']) else None,
                    'lot_number': lot_number,
                    'jde_date': str(row['F4111_TRDJ']) if pd.notnull(row['F4111_TRDJ']) else '',
                    'created_at': now
                }
                
                post_data.append(stock_update)
                print(f"[{index + 1}] Prepared stock update for {product_name}")
            else:
                print(f"[{index + 1}] Failed to create/find batch for {product_name}")
        else:
            print(f"[{index + 1}] Failed to create/find product {product_name}")
    
    # Store the dispatch data in S3
    try:
        s3_key = s3_helper.store_jde_dispatch(post_data, 'cardex_changes', cur_dt.strftime('%Y-%m-%d'))
        print(f"Stored cardex changes in S3: {s3_key}")
    except Exception as e:
        print(f"Warning: Failed to store in S3: {e}")
    
    return post_data

def fetch_or_create_product(product_name: str, jde_row: pd.Series) -> dict:
    """Fetch an existing product or create a new one if it doesn't exist"""
    if not product_name:
        return None
    
    # Try to fetch existing product
    existing_product = fetch_existing_product(product_name)
    if existing_product:
        return existing_product
    
    # Create new product if it doesn't exist
    from bakery_ops_helper import create_product_in_bakery_operations
    
    product_data = {
        'name': product_name,
        'description': f"Product imported from JDE: {product_name}",
        'inventory_unit': str(jde_row.get('F4111_TRUM', 'EA')),
        'jde_item_number': str(jde_row.get('F4111_AITM', '')),
        'vendor_info': {}
    }
    
    new_product = create_product_in_bakery_operations(product_data)
    return new_product

# Airflow DAG definition
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    'jde_cardex_changes_to_bakery_ops',
    default_args=default_args,
    description='Pipeline to sync JDE cardex changes to Bakery Operations system',
    schedule_interval=timedelta(hours=2),  # Run every 2 hours
)

def sync_cardex_changes(**kwargs):
    """Sync JDE cardex changes to Bakery Operations"""
    bu = kwargs.get('bu', '1110')  # Default business unit
    days_back = kwargs.get('days_back', 1)
    
    result = get_jde_cardex_with_comparison(bu, days_back)
    if result:
        print(f"Successfully processed {len(result['comparison_data'])} cardex changes")
        # Store results for next task
        kwargs['ti'].xcom_push(key='cardex_result', value=result)
        return result
    else:
        print("No cardex changes found")
        return None

sync_cardex = PythonOperator(
    task_id='sync_cardex_changes',
    dag=dag,
    python_callable=sync_cardex_changes,
    provide_context=True
)

# Set the pipeline order
sync_cardex
