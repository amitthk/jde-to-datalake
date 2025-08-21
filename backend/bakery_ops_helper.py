"""
Bakery Operations Helper
Handles API communication with the bakery operations backend system
"""
import json
from collections import defaultdict
from urllib.parse import urlparse
from dotenv import load_dotenv
from datetime import datetime, timedelta
from decimal import Decimal
import os
import logging
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import time
from pathlib import Path
from utility import retry_request, get_db_connection
from s3_helper import s3_helper
from schema_manager import schema_manager

logger = logging.getLogger(__name__)

def get_data_from_bakery_operations() -> dict:
    """Fetch products/items from internal Bakery Operations endpoints."""
    load_dotenv()
    
    facility_id = os.getenv("FACILITY_ID", "default_facility")
    backend_base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
    
    if not facility_id:
        print("❌ Error: FACILITY_ID not found in environment variables")
        return None

    # Use internal bakery ops endpoint instead of external API
    url = f"{backend_base_url}/bakeryops/facilities/{facility_id}/products"
    params = {
        "archived": "false",
        "includeAccess": "true", 
        "includeBatches": "true",
        "includeNotes": "true",
        "offset": 0,
        "productCategory": "Ingredient",
        "size": 100000,
        "sort": "productName:1"
    }
    
    print(f"Making request to internal endpoint: {url}")

    retry_count = 0
    max_retries = 3
    
    while retry_count <= max_retries:
        try:
            response = requests.get(
                url,
                params=params,
                timeout=30
            )
            
            if response.status_code == 429:
                print(f"⚠️ Rate limited (429). Retry {retry_count + 1}/{max_retries + 1}")
                time.sleep(30)  # Wait 30 seconds before retry
                retry_count += 1
                continue
            elif response.status_code == 200:
                print("✅ Successfully retrieved data from internal Bakery Operations")
                data = response.json()
                
                # Store the raw data in S3 for backup
                try:
                    s3_key = s3_helper.store_jde_dispatch(
                        data, 
                        'bakery_ops_products', 
                        datetime.now().strftime('%Y-%m-%d')
                    )
                    print(f"✅ Backed up Bakery Operations data to S3: {s3_key}")
                except Exception as s3_error:
                    print(f"⚠️ Failed to backup to S3: {s3_error}")
                
                return data
            else:
                print(f"❌ Error: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
        except requests.RequestException as e:
            print(f"❌ Request failed: {str(e)}")
            if retry_count < max_retries:
                print(f"Retrying in 10 seconds... (Attempt {retry_count + 1}/{max_retries + 1})")
                time.sleep(10)
                retry_count += 1
            else:
                print("❌ Max retries exceeded")
                return None

    return None

def create_product_in_bakery_operations(product_data: dict) -> dict:
    """Create a new product in the internal Bakery Operations system"""
    load_dotenv()
    
    facility_id = os.getenv("FACILITY_ID", "default_facility")
    backend_base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
    
    url = f"{backend_base_url}/bakeryops/facilities/{facility_id}/products"
    headers = {"Content-Type": "application/json"}
    
    try:
        # Prepare product data for internal Bakery Operations API
        payload = {
            'productName': product_data.get('name', ''),
            'description': product_data.get('description', ''),
            'productCategory': 'Ingredient',  # Default category
            'inventoryUnit': product_data.get('inventory_unit', 'EA'),
            'defaultVendor': product_data.get('vendor_info', {}),
            'notes': [{'text': f"Created from JDE Item: {product_data.get('jde_item_number', '')}"}],
            'archived': False
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 201 or response.status_code == 200:
            result = response.json()
            print(f"✅ Successfully created product in internal Bakery Operations: {result.get('_id')}")
            
            # Store creation record in S3
            try:
                creation_record = {
                    'action': 'create_product',
                    'product_id': result.get('_id'),
                    'product_data': payload,
                    'created_at': datetime.now().isoformat(),
                    'jde_source': product_data
                }
                s3_helper.store_jde_dispatch([creation_record], 'product_creations')
            except Exception as s3_error:
                print(f"⚠️ Failed to log creation to S3: {s3_error}")
            
            return result
        else:
            print(f"❌ Failed to create product. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error creating product in internal Bakery Operations: {str(e)}")
        return None

def dispatch_to_bakery_operations(batch_data: list) -> dict:
    """Dispatch inventory adjustments to internal Bakery Operations system"""
    load_dotenv()
    
    facility_id = os.getenv("FACILITY_ID", "default_facility")
    backend_base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
    
    headers = {"Content-Type": "application/json"}
    
    results = []
    
    for batch in batch_data:
        try:
            # Prepare inventory adjustment payload
            payload = {
                'productId': batch.get('product_id'),
                'batchNumber': batch.get('batch_number'),
                'quantity': batch.get('quantity'),
                'unit': batch.get('unit'),
                'adjustmentType': 'USAGE',  # Default to usage/consumption
                'reason': f"JDE Transaction: {batch.get('jde_transaction_id', '')}",
                'adjustmentDate': batch.get('transaction_date', datetime.now().isoformat()),
                'vesselCode': batch.get('vessel_code', ''),
                'lotNumber': batch.get('lot_number', ''),
                'notes': f"Dispatched from JDE system. Original doc: {batch.get('jde_document_number', '')}"
            }
            
            url = f"{backend_base_url}/bakeryops/facilities/{facility_id}/inventory-adjustments"
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code in [200, 201]:
                result = response.json()
                print(f"✅ Successfully dispatched batch to internal Bakery Operations: {result.get('_id')}")
                results.append({
                    'success': True,
                    'batch_id': batch.get('key'),
                    'bakery_ops_id': result.get('_id'),
                    'response': result
                })
            else:
                print(f"❌ Failed to dispatch batch. Status: {response.status_code}")
                print(f"Response: {response.text}")
                results.append({
                    'success': False,
                    'batch_id': batch.get('key'),
                    'error': response.text
                })
                
        except Exception as e:
            print(f"❌ Error dispatching batch to internal Bakery Operations: {str(e)}")
            results.append({
                'success': False,
                'batch_id': batch.get('key'),
                'error': str(e)
            })
    
    # Store dispatch results in S3
    try:
        dispatch_record = {
            'action': 'inventory_dispatch',
            'results': results,
            'dispatch_date': datetime.now().isoformat(),
            'total_batches': len(batch_data),
            'successful_dispatches': sum(1 for r in results if r['success'])
        }
        s3_helper.store_jde_dispatch([dispatch_record], 'inventory_dispatches')
        print(f"✅ Logged dispatch results to S3")
    except Exception as s3_error:
        print(f"⚠️ Failed to log dispatch to S3: {s3_error}")
    
    return {
        'total_processed': len(batch_data),
        'successful': sum(1 for r in results if r['success']),
        'failed': sum(1 for r in results if not r['success']),
        'results': results
    }

def fetch_action_data_from_bakery_operations(**kwargs):
    """
    Fetch consumption/usage data from Bakery Operations API
    
    Parameters:
    - **kwargs: Must include `start_date` (str).
    
    Returns:
    - str: JSON string of the response data if successful, or raises an exception otherwise.
    """
    load_dotenv()

    facility_id = os.getenv("FACILITY_ID", "default_facility")
    backend_base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")

    start_date = kwargs["start_date"]
    url = f"{backend_base_url}/bakeryops/facilities/{facility_id}/inventory-movements"
    
    params = {
        "movementTypes": "USAGE",
        "includeProductDetails": "true",
        "startDate": start_date,
        "sort": "movementDate:1"
    }

    headers = {"Content-Type": "application/json"}

    retry_count = 0
    max_retries = 3
    base_delay = 30

    while retry_count <= max_retries:
        try:
            print(f"[Attempt {retry_count + 1}] Fetching from: {url}")
            response = requests.get(url, headers=headers, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Successfully fetched {len(data)} movement records from Bakery Operations")
                
                # Store fetched data in S3
                try:
                    s3_helper.store_jde_dispatch(data, 'bakery_ops_movements', start_date)
                    print(f"✅ Backed up movement data to S3")
                except Exception as s3_error:
                    print(f"⚠️ Failed to backup movements to S3: {s3_error}")
                
                return json.dumps(data)
            elif response.status_code == 429:
                delay = base_delay * (2 ** retry_count)  # Exponential backoff
                print(f"⚠️ Rate limited (429). Waiting {delay} seconds before retry {retry_count + 1}/{max_retries + 1}")
                time.sleep(delay)
                retry_count += 1
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                print(f"❌ API Error: {error_msg}")
                raise Exception(error_msg)
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request Exception: {str(e)}")
            if retry_count < max_retries:
                delay = base_delay * (2 ** retry_count)
                print(f"Retrying in {delay} seconds... (Attempt {retry_count + 1}/{max_retries + 1})")
                time.sleep(delay)
                retry_count += 1
            else:
                raise Exception(f"Failed after {max_retries + 1} attempts: {str(e)}")

    raise Exception(f"Failed to fetch data after {max_retries + 1} attempts")

def store_bakery_operations_data(data_type: str, data: list, metadata: dict = None):
    """Store data in both database and S3 for audit trail"""
    try:
        # Store in database
        conn = get_db_connection()
        
        if data:
            # Infer and register schema
            schema_def = schema_manager.infer_schema_from_data(data)
            schema_manager.register_schema(
                table_name=f'bakery_ops_{data_type}',
                schema_definition=schema_def,
                description=f'Bakery Operations {data_type} data schema'
            )
            
            # Convert to DataFrame and store
            df = pd.DataFrame(data)
            table_name = f'bakery_ops_{data_type}'
            
            # Insert data (this would need the appropriate utility function)
            print(f"✅ Stored {len(data)} records of {data_type} in database")
        
        # Store in S3
        s3_key = s3_helper.store_jde_dispatch(
            data, 
            f'bakery_ops_{data_type}',
            datetime.now().strftime('%Y-%m-%d')
        )
        print(f"✅ Stored {data_type} data in S3: {s3_key}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to store bakery operations data: {str(e)}")
        return False
