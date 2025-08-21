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
    """Fetch JDE cardex data and compare with Bakery-System - streamlined approach"""
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
    
    # Get Bakery-System data
    from bakery_helper import get_data_from_bakery_system
    bakery_system_data = get_data_from_bakery_system()
    if not bakery_system_data:
        print("No Bakery-System data found")
        return None
    
    df_bakery_system = pd.DataFrame(bakery_system_data)
    
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
    
    # Calculate total Bakery-System quantity for each product name
    total_bakery_system_quantity_map = {}
    if not df_bakery_system.empty and 'name' in df_bakery_system.columns and 'onHand' in df_bakery_system.columns:
        for _, row in df_bakery_system.iterrows():
            pname = str(row['name']) if pd.notnull(row['name']) else None
            on_hand = row.get('onHand', {})
            qty = 0
            if isinstance(on_hand, dict):
                qty = on_hand.get('amount', 0)
            if pname:
                total_bakery_system_quantity_map[pname.lower()] = total_bakery_system_quantity_map.get(pname.lower(), 0) + (qty if qty is not None else 0)
    
    # Find mismatched items only
    mismatched_transactions = []
    for _, jde_row in df_jde.iterrows():
        product_name = str(jde_row['F4111_LITM']) if pd.notnull(jde_row['F4111_LITM']) else None
        if not product_name:
            continue
            
        total_jde_quantity = total_jde_quantity_map.get(product_name.lower(), 0)
        total_bakery_system_quantity = total_bakery_system_quantity_map.get(product_name.lower(), 0)
        
        # Only include if quantities don't match
        if abs(total_jde_quantity - total_bakery_system_quantity) > 0.001:  # Allow for small floating point differences
            print(f"Mismatch found for {product_name}: JDE={total_jde_quantity}, Bakery-System={total_bakery_system_quantity}")
            mismatched_transactions.append(jde_row.to_dict())
    
    if not mismatched_transactions:
        print("No mismatched items found - all quantities match")
        return None
    
    # Return in the same format expected by the processing function
    return {
        'ServiceRequest1': {
            'fs_DATABROWSE_V4111A': {
                'data': {
                    'gridData': {
                        'rowset': mismatched_transactions
                    }
                }
            }
        }
    }




def post_to_api(endpoint: str, data: dict) -> bool:
    """Post data to the API"""
    load_dotenv()

    url = f"{os.getenv('STICAL_TARGET_API')}/{endpoint}"
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, headers=headers, json=data, verify=False)
        if response.status_code == 200 or response.status_code == 201:
            logging.info(f"Data posted successfully to {endpoint} endpoint")
            return True
        else:
            logging.error(f"Failed to post data to {endpoint} endpoint")
            return False
    except Exception as e:
        logging.error(f"Error posting to API: {e}")
        return False

#@lru_cache(maxsize=250)
def fetch_existing_ingredient(product_name: str) -> dict:
    """Fetch an Ingredient product by name"""
    load_dotenv()

    facility_id = os.getenv("FACILITY_ID", "default_facility")
    backend_base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    endpoint = 'products'
    url = f'{backend_base_url}/bakeryops/facilities/{facility_id}/{endpoint}'

    headers = {'Content-Type': 'application/json'}
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
        params['q'] = product_name
        data_json = retry_request_lru(url=url, headers=headers, method='GET', params=params)
        print(f"{now} : Found Ingredient products with this query {product_name}")
        print(data_json)
        ingredient_product = data_json
        if isinstance(data_json,list):
            ingredient_product = data_json[0]
        elif isinstance(data_json,str):
            ingredient_product = json.loads(data_json)
        if ingredient_product['name'].lower() == product_name.lower():
            print(f"{now} :Found exact match by product name {json.dumps(ingredient_product)}")
            return(ingredient_product)
    except Exception as e:
        logging.error(f"Error fetching existing Ingredient product: {e}")
    print(f"{now} : No existing Ingredient was found with this name: {product_name}")
    return None




def create_new_ingredient(payload: dict) -> dict:
    """Create a new Ingredient product"""
    load_dotenv()

    facility_id = os.getenv("FACILITY_ID", "default_facility")
    backend_base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")

    endpoint = 'products'
    url = f'{backend_base_url}/bakeryops/facilities/{facility_id}/{endpoint}'

    headers = {'Content-Type': 'application/json'}

    try:
        data_json = retry_request(url=url, headers=headers, method='POST', payload=payload)

        # Get the new Ingredient product ID
        for item in data_json['data']['rowset']:
            return {'item': item, 'id': item['_id']}

    except Exception as e:
        logging.error(f"Error creating new Ingredient product: {e}")

    return None




def fetch_or_create_ingredient(product_name: str, row: dict) -> dict:
    """Fetch or create an Ingredient product"""
    from utility import preserve_quantity_precision
    result = fetch_existing_ingredient(product_name)
    if result is not None:
        return result
    converted_quantity = convert_unit_quantity(
        source_unit=row['F4111_TRUM'],
        target_unit='g',  # or any other target unit, e.g. 'L'
        quantity=preserve_quantity_precision(row['F4111_TRQT']) if not pd.isnull(row['F4111_TRQT']) else 0
    )
    jde_unit = row['F4111_TRUM'] if not pd.isnull(row['F4111_TRUM']) else None
    bakery_system_unit = convert_unit(Unit=jde_unit, direction='from_jde')
    payload = {
        'access': {'global': False, 'owners': []},
        'manufacturer': None,
        'defaultVendor': None,
        'productType': {'_id': 11},
        'additionCustomUnit': {'additionCustomUnit': 'false',
                               'additionRateUnit': None,
                               'additionRateValue': None, # converted_quantity if not pd.isnull(row['F4111_TRQT']) else '1', 
                               'additionUnit': bakery_system_unit },
        'categoryFields': {'additionRateUnit':  None, 
                           'additionRateValue': None, # converted_quantity if not pd.isnull(row['F4111_TRQT']) else '1', 
                           'additionUnit': bakery_system_unit , 'additionCustomUnit': 'false' },
        'inventoryUnit': bakery_system_unit,
        'name': row['F4111_LITM'] if not pd.isnull(row['F4111_LITM']) else '',
        'tags': [],
        'notes': [{'text': f"TRDJ: {row['F4111_TRDJ']}" if pd.isnull(row['F4111_TRDJ']) or row['F4111_TRDJ'].strip() == '' else str(row['F4111_TRDJ']) +
                 "ITM: "+str(row['F4111_ITM']) if not pd.isnull(row['F4111_ITM']) else None +
                 "LITM: "+str(row['F4111_LITM']) if not pd.isnull(row['F4111_LITM']) else None +
                 "DCT: "+ str(row['F4111_DCT']) if not pd.isnull(row['F4111_DCT']) else None}]
    }

    result = create_new_ingredient(payload)
    return result



#@lru_cache(maxsize=250)
def fetch_existing_ingredient_batch(ingredient_id: str, batch_name: str) -> dict:
    """Fetch an Ingredient product batch by id and batch name"""
    load_dotenv()

    facility_id = os.getenv("FACILITY_ID", "default_facility")
    backend_base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    endpoint = 'inventory-movements?archived=false&depleted=false&includeDefaultVendor=true&includeNotes=true&size=9999'
    url = f'{backend_base_url}/bakeryops/facilities/{facility_id}/{endpoint}'

    headers = {'Content-Type': 'application/json'}

    try:
        data_json = retry_request_lru(url=url, headers=headers, method='GET')
        
        # Add debugging to see what we actually got
        print(f"{now} : Response type: {type(data_json)}, Content: {data_json}")
        
        # Handle case where data_json might be None
        if data_json is None:
            print(f"{now} : No data returned from API")
            return None
        
        # Handle case where data_json is a string (maybe JSON string that needs parsing)
        if isinstance(data_json, str):
            try:
                data_json = json.loads(data_json)
                print(f"{now} : Parsed JSON string to object")
            except json.JSONDecodeError as e:
                print(f"{now} : Failed to parse JSON string: {e}")
                return None
        
        # Handle case where data_json is a dict instead of list
        if isinstance(data_json, dict):
            # If it's a single item, check if it matches
            if 'batchNumber' in data_json:
                if str(data_json['batchNumber']).lower() == batch_name.lower():
                    print(f"{now} : Found a batch matching the batch name {batch_name}")
                    return data_json
            else:
                print(f"{now} : Response is a dict but doesn't contain 'batchNumber' key")
                print(f"{now} : Available keys: {list(data_json.keys())}")
                return None
        
        # Handle case where data_json is a list (your original logic)
        elif isinstance(data_json, list):
            # Check if the Ingredient product batch exists by batch name
            for item in data_json:
                if isinstance(item, dict) and 'batchNumber' in item:
                    if str(item['batchNumber']).lower() == batch_name.lower():
                        print(f"{now} : Found a batch matching the batch name {batch_name}")
                        return item
                else:
                    print(f"{now} : List item is not a dict or missing 'batchNumber' key: {item}")
            
            print(f"{now} : Could not find any batch matching the batch name {batch_name}")
            return None
        
        else:
            print(f"{now} : Unexpected data type: {type(data_json)}")
            return None

    except Exception as e:
        print(f"{now} : Error processing response: {e}")
        return None



def check_transaction_exists_in_batch_actions(ingredient_id: str, batch_id: str, transaction_number: str) -> bool:
    """Check if a transaction already exists in the batch actions by looking through notes"""
    load_dotenv()

    facility_id = os.getenv("FACILITY_ID", "default_facility")
    backend_base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    
    # Get all inventory movements for this product
    endpoint = f'inventory-movements'
    url = f'{backend_base_url}/bakeryops/facilities/{facility_id}/{endpoint}'
    
    headers = {'Content-Type': 'application/json'}

    try:
        data_json = retry_request_lru(url=url, headers=headers, method='GET')
        
        # Collect all JDE transaction IDs from notes
        jde_transaction_ids = []

        for action in data_json:
            notes = action.get('notes', [])
            for note in notes:
                note_text = note.get('text', '')
                if 'JDE_Transaction_Id:' in note_text:
                    transaction_id = note_text.replace('JDE_Transaction_Id:', '').strip()
                    jde_transaction_ids.append(transaction_id)

        # Display found transaction IDs
        print(f"{now} : Found the following JDE Transaction Ids in batch actions: {jde_transaction_ids}")

        # Check if the given transaction number exists
        if transaction_number in jde_transaction_ids:
            print(f"{now} : Found existing transaction {transaction_number} in batch actions")
            return True
        else:
            print(f"{now} : Transaction {transaction_number} not found in batch actions")
            return False
        
    except Exception as e:
        logging.error(f"Error checking transaction existence in batch actions: {e}")
        return False


def create_new_ingredient_batch(ingredient_id: str, batch_name: str) -> dict:
    """Create a new Ingredient product batch"""
    load_dotenv()

    payload = {
        '_id': None,
        'batchNumber': batch_name,
        'manufacturerBatchId': batch_name,
        'depleted': False,
        'onHand': None,
        'categoryFields':{'expirationDate': None},
        'expirationDate':None,
        'costOnHand':None,
        'defaultVendor':{'_id': '67597'},
        'tags': [],
        'notes': [{'text': f"IngredientId: {ingredient_id}, Batch: {batch_name}"}]
    }
    facility_id = os.getenv("FACILITY_ID", "default_facility")
    backend_base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    endpoint = 'inventory-adjustments'
    url = f'{backend_base_url}/bakeryops/facilities/{facility_id}/{endpoint}'

    headers = {
        'Content-Type': 'application/json'
    }
    print(f"{now} : Sending this payload to create inventory adjustment for ingredient {ingredient_id} batch {json.dumps(payload)}")
    return retry_request(url=url, headers=headers, method='POST', payload=payload)




def fetch_or_create_ingredient_batch(ingredient_id: str, batch_name: str) -> dict:
    """Fetch or create an Ingredient product batch"""
    load_dotenv()

    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    result = fetch_existing_ingredient_batch(ingredient_id, batch_name)
    if result is not None:
        return {'batch_result': result, 'is_new_batch': False }

    result = create_new_ingredient_batch(ingredient_id, batch_name)
    return  {'batch_result': result, 'is_new_batch': True }




def post_batch_action_payload(ingredient_id: str, batch_result: dict, row: dict, batch_name: str) -> dict:
    """Post the action data payload for a batch transaction"""
    load_dotenv()
    
    facility_id = os.getenv("FACILITY_ID", "default_facility")
    backend_base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    
    txt_note_row = row.copy()
    txt_note_row['POSTED_PRODUCTID'] = f"'{str(row['F4111_LITM']) if pd.notnull(row['F4111_LITM']) else None}'"
    txt_note_row['POSTED_BATCHID'] = f"'{batch_name}'"
    txt_note_json = json.dumps(txt_note_row)
    
    txt_note = f"{{F4111_LITM: {str(row['F4111_LITM']) if pd.notnull(row['F4111_LITM']) else None},"\
    f"F4111_TRDJ: {str(row['F4111_TRDJ']) if pd.notnull(row['F4111_TRDJ']) else None},"\
    f"F4111_DCT: {str(row['F4111_DCT']) if pd.notnull(row['F4111_DCT']) else None},"\
    f"F4111_LOTN: {str(row['F4111_LOTN']) if pd.notnull(row['F4111_LOTN']) else None},"\
    f"F4111_TRQT: {str(row['F4111_TRQT']) if pd.notnull(row['F4111_TRQT']) else None},"\
    f"F4111_TRUM: {str(row['F4111_TRUM']) if pd.notnull(row['F4111_TRUM']) else None},"\
    f"POSTED_PRODUCTID: ,"\
    f"POSTED_BATCHID: {batch_name}}}"
    
    payload = {
        "actionData": {
            "batch": {
                "_id": batch_result['_id'],
                "publicId": batch_result['publicId'],
                "batchNumber": batch_result['batchNumber'],
                "manufacturerBatchId": batch_result['manufacturerBatchId'],
                "depleted": False,
                "onHand": 1,
                "categoryFields": {
                    "expirationDate": None
                },
                "ingredientId": ingredient_id,
                "defaultVendorId": 67597,
                "tags": [],
                "archived": False,
                "defaultVendor": {
                    "_id": 67597,
                    "name": "Amazon.com",
                    "outletId": None
                },
                "displayString": f"#{str(row['F4111_LITM']) if pd.notnull(row['F4111_LITM']) else None}_{str(row['F4111_ITM']) if pd.notnull(row['F4111_ITM']) else None}_{str(row['F4111_DOC']) if pd.notnull(row['F4111_DOC']) else None}"
            },
            "vendor": {
                "_id": 67597
            },
            "numberOfItems": 1,
            "itemSize": float(Decimal(str(row['F4111_TRQT']))) if pd.notnull(row['F4111_TRQT']) else 0.0
        },
        "actionType": "RECEIVE_DRY_GOOD",
        "tags": [],
        "notes": [
            {'text': f"JDE_Transaction_Id: {row['F4111_DOC']}"}, 
            {'text': txt_note_json}, 
            {'text': f"JDE_batch_name: {row['F4111_LOTN']}"}
        ]
    }
    
    headers = {'Content-Type': 'application/json'}
    endpoint = 'inventory-adjustments'
    url = f'{backend_base_url}/bakeryops/facilities/{facility_id}/{endpoint}'
    
    data_json = retry_request(url=url, headers=headers, method='POST', payload=payload)
    print(f'{now} : Here is the response we got from bakery-system after posting actionData: {json.dumps(data_json)}')
    
    return data_json


def call_bakery_system_api(url: str, payload: dict) -> dict:
    load_dotenv()

    outlet_id = os.getenv("OUTLET_ID")
    bakeryops_token = os.getenv("BAKERY_SYSTEM_TOKEN")
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    headers = {'Content-Type': 'application/json', 'Authorization': 'Access-Token {}'.format(bakeryops_token)}

    try:
        print(f"{now} : Sending this payload to endpoint {url} payload {json.dumps(payload)}")
        response = retry_request(url=url, headers=headers, method='POST', json=payload)
        return response
    except Exception as e:
        logging.error(f"Error sending data to {url}")
    return None

def invalidate_ingredient_lru_cache(facility_id: str, ingredient_id: str, batch_id:str):
    backend_base_url = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
    endpoint = f'inventory-movements'
    url = f'{backend_base_url}/bakeryops/facilities/{facility_id}/{endpoint}'
    
    headers = {'Content-Type': 'application/json'}
    invalidate_lru_cache(url=url, headers=headers,method='GET')



def submit_ingredient_batch_action(data: dict) -> list:
    """Generate the final payload for stock update - streamlined with existing JDE transaction tracking"""
    load_dotenv()

    df_json = data['ServiceRequest1']['fs_DATABROWSE_V4111A']['data']['gridData']['rowset']
    df = pd.DataFrame([row for row in df_json])
    outlet_id = os.getenv("OUTLET_ID")
    bakeryops_token = os.getenv("BAKERY_SYSTEM_TOKEN")
    post_data = []
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    
    for index, row in df.iterrows():
        ingredient_product_name = str(row['F4111_LITM']) if pd.notnull(row['F4111_LITM']) else None
        result = fetch_or_create_ingredient(ingredient_product_name, row)
        
        # Create batch name using ingredient_name + "_" + F4111_LOTN (if exists)
        lot_number = str(row['F4111_LOTN']) if pd.notnull(row['F4111_LOTN']) else None
        batch_name = ingredient_product_name if lot_number is None else f"{ingredient_product_name}_{lot_number}"
        transaction_number = str(row['F4111_DOC']) if pd.notnull(row['F4111_DOC']) else None
        
        if result is not None:
            ingredient_id = result['_id']
            
            # Fetch or create the batch based on batch_name
            batch_result_info = fetch_or_create_ingredient_batch(ingredient_id, batch_name)
            batch_result = batch_result_info['batch_result']
            is_new_batch = batch_result_info['is_new_batch']
            
            if batch_result is not None:
                batch_id = batch_result['_id']
                # Check if this specific transaction already exists in batch actions using JDE transaction ID
                transaction_exists = False
                if not is_new_batch:
                    transaction_exists = check_transaction_exists_in_batch_actions(
                        ingredient_id, batch_id, transaction_number
                    )
                
                # Only post if it's a new batch or transaction doesn't exist
                if is_new_batch or not transaction_exists:
                    data_json = post_batch_action_payload(ingredient_id, batch_result, row, batch_name)
                    post_data.append(data_json)
                    invalidate_ingredient_lru_cache(outlet_id=outlet_id, bakeryops_token=bakeryops_token, ingredient_id=ingredient_id, batch_id=batch_id)
                else:
                    print(f"{now}: Payload was NOT posted as the JDE transaction {transaction_number} already exists in batch actions.")
            else:
                print(f'{now} : The batch {batch_name} was empty!')
        else:
            # Log error when item cannot be found or created
            error_message = f"Item '{ingredient_product_name}' does not exist in Bakery-System and could not be created. Please add this item using Item Master review first."
            print(f"ERROR: {error_message}")
            logging.error(f"{now}: {error_message}")
                
    return post_data




def main():
    load_dotenv()

    today = datetime.now()
    date_time_str = today.strftime("%d/%m/%Y %H:%M:%S")

    bu = '1110'
    print(f'{date_time_str} : Processing streamlined comparison for bu {bu} for past 5 days')

    # Use streamlined approach - only process mismatched items
    data = get_jde_cardex_with_comparison(bu, days_back=5)
    if data is not None:
        post_data = submit_ingredient_batch_action(data)
        print(f"{date_time_str}: Posted Data =====> {json.dumps(post_data)}")
    else:
        print(f"{date_time_str}: No mismatched items found - skipping processing")


# Function to process a single BU using streamlined approach
def process_bu(bu, days_back=5):
    print(f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: Processing streamlined comparison for BU {bu} for past {days_back} days")
    
    # Fetch JDE cardex with comparison - only get mismatched items
    data = get_jde_cardex_with_comparison(bu, days_back)
    
    if not data:
        print(f"No mismatched items found for BU {bu} - all quantities match")
        return
    
    # Generate final payload for mismatched items only
    post_data = submit_ingredient_batch_action(data)

    # Log the posted data (optional)
    print(f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: Posted Data =====> {json.dumps(post_data)}")

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'jde_cardex_streamlined_to_bakery_system_dag',
    default_args=default_args,
    description='Streamlined DAG to process only mismatched JDE cardex data and update Bakery-System',
    schedule_interval='*/30 * * * *',
)

# Task 1: Process BU 1110 using streamlined approach
process_bu_1110_task = PythonOperator(
    task_id='process_bu_1110_streamlined',
    python_callable=process_bu,
    op_kwargs={'bu': '1110', 'days_back': 5},
    dag=dag
)

# Set dependencies: Single task for streamlined processing
process_bu_1110_task

