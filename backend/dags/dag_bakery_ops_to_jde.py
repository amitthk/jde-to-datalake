#!/usr/bin/env python
# coding: utf-8

import os
from dotenv import load_dotenv
import requests
import json
import time
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
import logging
from urllib.parse import urlparse
from utility import retry_request, get_db_connection, insert_into_table, convert_unit, is_jde, convert_rate_unit, convert_unit_quantity
from psycopg2 import connect
from s3_helper import s3_helper
from schema_manager import schema_manager

def verify_products_submitted_status_db():
    """Verify the status of products submitted to JDE from database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT product_id, movement_id, dispatched_to_jde, dispatch_date 
            FROM bakery_ops_movements 
            WHERE dispatched_to_jde = true 
            ORDER BY dispatch_date DESC 
            LIMIT 10
        """)
        
        results = cursor.fetchall()
        for row in results:
            print(f"Product {row[0]}: Movement {row[1]}, Dispatched: {row[2]}, Date: {row[3]}")
        
        cursor.close()
        conn.close()
        return results
        
    except Exception as e:
        print(f"Error verifying product status: {e}")
        return []

def fetch_from_bakery_ops_api(**kwargs):
    """
    Fetch data from Bakery Operations API with retry logic for 429 Too Many Requests.
    
    Parameters:
    - **kwargs: Must include `start_date` (str).
    
    Returns:
    - str: JSON string of the response data if successful, or raises an exception otherwise.
    """
    load_dotenv()

    facility_id = os.getenv("FACILITY_ID")
    bakery_ops_base_url = os.getenv("BAKERY_OPS_BASE_URL")
    bakery_ops_api_token = os.getenv("BAKERY_OPS_API_TOKEN")

    start_date = kwargs["start_date"]
    url = f"{bakery_ops_base_url}/facilities/{facility_id}/inventory-movements" \
          f"?movementTypes=USAGE&includeProductDetails=true" \
          f"&startDate={start_date}&sort=movementDate:1"

    headers = {"Content-Type": "application/json"}
    if bakery_ops_api_token:
        headers["Authorization"] = f"Bearer {bakery_ops_api_token}"

    retry_count = 0
    max_retries = 3
    base_delay = 30

    while retry_count <= max_retries:
        try:
            print(f"[Attempt {retry_count + 1}] Fetching from: {url}")
            response = requests.get(url, headers=headers, verify=False, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Successfully fetched {len(data)} movement records from Bakery Operations")
                
                # Store fetched data in S3
                try:
                    s3_key = s3_helper.store_jde_dispatch(data, 'bakery_ops_movements', start_date)
                    print(f"✅ Backed up movement data to S3: {s3_key}")
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

def parse_bakery_ops_json(data):
    """Parse Bakery Operations movement data and flatten for JDE dispatch"""
    flattened_entries = []

    for entry in json.loads(data):
        if entry.get("movementType") == "USAGE":
            movement_id = entry.get("_id")
            product_summary = {}
            product_units = {}
            batch_summary = []

            product = entry.get("product", {})
            batches = entry.get("batches", [])

            product_id = str(product.get("_id"))
            product_name = product.get("productName")
            inventory_unit = product.get("inventoryUnit")

            product_summary[product_id] = product_name
            product_units[product_id] = inventory_unit

            for batch_entry in batches:
                batch = batch_entry.get("batch", {})
                batch_id = str(batch.get("_id"))
                batch_number = batch.get("batchNumber")
                quantity_used = batch_entry.get("quantityUsed")
                unit = batch_entry.get("unit")

                batch_info = {
                    "batch_id": batch_id,
                    "batch_number": batch_number,
                    "quantity_used": quantity_used,
                    "unit": unit
                }
                batch_summary.append(batch_info)

            movement_date = entry.get("movementDate")
            vessel_code = entry.get("vesselCode", "")
            lot_number = entry.get("lotNumber", "")
            reason = entry.get("reason", "")

            flattened_entry = {
                "movement_id": movement_id,
                "product_summary": product_summary,
                "product_units": product_units,
                "batch_summary": batch_summary,
                "movement_date": movement_date,
                "vessel_code": vessel_code,
                "lot_number": lot_number,
                "reason": reason
            }
            flattened_entries.append(flattened_entry)

    # Store parsed data in S3
    try:
        s3_key = s3_helper.store_jde_dispatch(flattened_entries, 'parsed_bakery_ops_movements')
        print(f"✅ Stored parsed movement data in S3: {s3_key}")
    except Exception as e:
        print(f"⚠️ Failed to store parsed data in S3: {e}")

    return json.dumps(flattened_entries)

def check_product_status(product_id, conn):
    """Check if the product movement is already marked as dispatched to JDE in DB"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT dispatched_to_jde FROM bakery_ops_movements 
        WHERE product_id = %s AND dispatched_to_jde = true 
        ORDER BY dispatch_date DESC LIMIT 1
    """, (product_id,))
    
    result = cursor.fetchone()
    cursor.close()
    
    return result is not None

def insert_into_table(conn, data):
    """Insert all captured values from parse_bakery_ops_json into the table."""
    cursor = conn.cursor()
    
    for entry in json.loads(data):
        movement_id = entry["movement_id"]
        movement_date = entry["movement_date"]
        vessel_code = entry.get("vessel_code", "")
        lot_number = entry.get("lot_number", "")
        reason = entry.get("reason", "")
        
        # Process each product in the movement
        for product_id, product_name in entry["product_summary"].items():
            unit = entry["product_units"].get(product_id, "EA")
            
            # Process each batch for this product
            for batch_info in entry["batch_summary"]:
                cursor.execute("""
                    INSERT INTO bakery_ops_movements 
                    (movement_id, product_id, batch_id, movement_type, quantity, unit, 
                     movement_date, reason, vessel_code, lot_number, dispatched_to_jde) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (movement_id) DO NOTHING
                """, (
                    movement_id,
                    product_id,
                    batch_info["batch_id"],
                    "USAGE",
                    batch_info["quantity_used"],
                    batch_info["unit"],
                    movement_date,
                    reason,
                    vessel_code,
                    lot_number,
                    False  # Not yet dispatched to JDE
                ))
    
    conn.commit()
    cursor.close()

def post_data_to_jde(data):
    """Dispatch product consumptions to JDE using unique transaction ID"""
    load_dotenv()

    facility_id = os.getenv("FACILITY_ID")
    bakery_ops_base_url = os.getenv("BAKERY_OPS_BASE_URL")
    bakery_ops_api_token = os.getenv("BAKERY_OPS_API_TOKEN")

    url = os.getenv("JDE_IA_URL")
    username = os.getenv("JDE_CARDEX_USERNAME")
    password = os.getenv("JDE_CARDEX_PASSWORD")

    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(username, password)
    
    items = json.loads(data)
    conn = get_db_connection()
    resp_arr = []

    try:
        for idx, item in enumerate(items):
            movement_id = item['movement_id']
            
            # Check if already processed
            if check_product_status(item['product_summary'].keys(), conn):
                print(f"[{idx}] Movement {movement_id} already dispatched to JDE")
                continue
            
            # Process each product in the movement
            for product_id, product_name in item['product_summary'].items():
                for batch_info in item['batch_summary']:
                    # Create JDE payload
                    jde_payload = {
                        "ServiceRequest": [{
                            "fs_DATABROWSE_W41111A": {
                                "data": {
                                    "gridData": {
                                        "rowset": [{
                                            "Branch_Plant": os.getenv("JDE_BUSINESS_UNIT", "1110"),
                                            "Item_Number": product_name,  # Use product name as item number
                                            "Document_Type": "II",  # Inventory Issue
                                            "Quantity_Inventory": batch_info["quantity_used"],
                                            "Unit_of_Measure": batch_info["unit"],
                                            "Lot_Serial": item.get("lot_number", ""),
                                            "Location": item.get("vessel_code", ""),
                                            "Explanation": f"Bakery Ops Usage - {item.get('reason', '')}"
                                        }]
                                    }
                                }
                            }
                        }]
                    }
                    
                    try:
                        response = requests.post(url, headers=headers, json=jde_payload, auth=auth, verify=False, timeout=60)
                        
                        if response.status_code in [200, 201]:
                            print(f"[{idx}] Successfully dispatched to JDE. Status: {response.status_code}")
                            
                            # Mark as dispatched in database
                            cursor = conn.cursor()
                            cursor.execute("""
                                UPDATE bakery_ops_movements 
                                SET dispatched_to_jde = true, dispatch_date = CURRENT_TIMESTAMP,
                                    jde_transaction_id = %s
                                WHERE movement_id = %s AND product_id = %s
                            """, (response.json().get('transaction_id'), movement_id, product_id))
                            conn.commit()
                            cursor.close()
                            
                            resp_arr.append({
                                'success': True,
                                'movement_id': movement_id,
                                'product_id': product_id,
                                'jde_response': response.json()
                            })
                        else:
                            print(f"[{idx}] Failed to dispatch to JDE. Status: {response.status_code}")
                            resp_arr.append({
                                'success': False,
                                'movement_id': movement_id,
                                'product_id': product_id,
                                'error': response.text
                            })
                            
                    except Exception as e:
                        print(f"[{idx}] Error dispatching to JDE: {e}")
                        resp_arr.append({
                            'success': False,
                            'movement_id': movement_id,
                            'product_id': product_id,
                            'error': str(e)
                        })
    
    finally:
        conn.close()
    
    # Store dispatch results in S3
    try:
        s3_key = s3_helper.store_jde_dispatch(resp_arr, 'jde_dispatch_results')
        print(f"✅ Stored JDE dispatch results in S3: {s3_key}")
    except Exception as e:
        print(f"⚠️ Failed to store dispatch results in S3: {e}")
    
    return json.dumps(resp_arr)

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
    'bakery_ops_movements_to_jde',
    default_args=default_args,
    description='Pipeline to fetch and post Bakery Operations movement data to JDE with S3 data lake integration',
    schedule_interval=timedelta(minutes=10),  # Run every 10 minutes
)

def get_start_date():
    """Get the start date for fetching data (usually yesterday)"""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

def fetch_bakery_ops_task(**kwargs):
    """Task to fetch data from Bakery Operations API"""
    start_date = get_start_date()
    return fetch_from_bakery_ops_api(start_date=start_date)

def parse_bakery_ops_data_task(**kwargs):
    """Task to parse the fetched Bakery Operations data"""
    data = kwargs["ti"].xcom_pull(task_ids="fetch_bakery_ops")
    return parse_bakery_ops_json(data)

def post_to_jde_task(**kwargs):
    """Task to post processed data to JDE"""
    data = kwargs["ti"].xcom_pull(task_ids="parse_bakery_ops_data")
    response = post_data_to_jde(data)
    return response

# Define the tasks
fetch_bakery_ops = PythonOperator(
    task_id='fetch_bakery_ops',
    dag=dag,
    python_callable=fetch_bakery_ops_task
)

parse_bakery_ops_data = PythonOperator(
    task_id='parse_bakery_ops_data',
    dag=dag,
    python_callable=parse_bakery_ops_data_task
)

post_to_jde = PythonOperator(
    task_id='post_to_jde',
    dag=dag,
    python_callable=post_to_jde_task
)

# Set the pipeline order
fetch_bakery_ops >> parse_bakery_ops_data >> post_to_jde
