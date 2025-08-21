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
import urllib3
from pathlib import Path
from utility import retry_request, convert_unit, convert_rate_unit, convert_unit_quantity, invalidate_lru_cache, validate_unit, get_db_connection, retry_request_lru
from functools import lru_cache
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from decimal import Decimal
from urllib.parse import urlparse

def get_latest_jde_cardex(bu: str, rDate: str) -> dict:
    """Fetch purchase orders from JDE"""
    # Get the directory where this script is located
    current_dir = Path(__file__).parent
    env_path = current_dir / '.env'
    
    # Load environment variables from the backend directory
    load_dotenv(env_path)

    url = os.getenv("JDE_CARDEX_CHANGES_TO_BAKERY_SYSTEM_URL")
    username = os.getenv("JDE_CARDEX_USERNAME")
    password = os.getenv("JDE_CARDEX_PASSWORD")
    
    # Debug print to check if variables are loaded
    print(f"Debug - URL: {url}")
    print(f"Debug - Username: {username}")
    
    if not url:
        print("❌ Error: JDE_CARDEX_CHANGES_TO_BAKERY_SYSTEM_URL not found in environment variables")
        return None
    
    if not username or not password:
        print("❌ Error: JDE credentials not found in environment variables")
        return None

    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(username, password)
    params = {
        'bu': bu,
        'rDate': rDate
    }

    try:
        response = requests.get(url, headers=headers, auth=auth, params=params, verify=False)
        if response.status_code == 200 or response.status_code == 201:
            return json.loads(response.text)
        else:
            logging.error(f"Failed to fetch details from STICAL_PO_SUMMARY endpoint {url}")
            return None
    except Exception as e:
        logging.error(f"Error fetching JDE PO summary: {e}")
        return None


def get_jde_item_master(bu: str, rDate: str, glCat: str) -> dict:
    """Fetch item master data from JDE"""
    # Get the directory where this script is located
    current_dir = Path(__file__).parent
    env_path = current_dir / '.env'
    
    # Load environment variables from the backend directory
    load_dotenv(env_path)
    
    url = os.getenv("JDE_ITEM_MASTER_UPDATES_URL")
    username = os.getenv("JDE_CARDEX_USERNAME")
    password = os.getenv("JDE_CARDEX_PASSWORD")

    # Debug print to check if variables are loaded
    print(f"Debug - JDE Item Master URL: {url}")
    print(f"Debug - JDE Username: {username}")
    print(f"Debug - Parameters: bu={bu}, glCat={glCat}")
    
    if not url:
        print("❌ Error: JDE_ITEM_MASTER_UPDATES_URL not found in environment variables")
        return None
    
    if not username or not password:
        print("❌ Error: JDE credentials not found in environment variables")
        return None

    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(username, password)
    params = {
        'bu': bu,
        'glCat': glCat,
        'rDate': rDate
    }

    try:
        print(f"Making request to: {url} with params: {params}")
        response = requests.get(url, headers=headers, auth=auth, params=params, verify=False)
        
        print(f"Response status code: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            response_text = response.text
            print(f"Response text length: {len(response_text)}")
            if len(response_text) > 1000:
                print(f"Response text preview: {response_text[:1000]}...")
            else:
                print(f"Response text: {response_text}")
            
            try:
                json_data = json.loads(response_text)
                return json_data
            except json.JSONDecodeError as je:
                print(f"❌ JSON decode error: {je}")
                print(f"Raw response: {response_text}")
                return None
        else:
            error_msg = f"HTTP {response.status_code}: {response.reason}"
            try:
                error_detail = response.text
                print(f"❌ Error response: {error_detail}")
                error_msg += f" - {error_detail}"
            except:
                pass
            
            logging.error(f"Failed to fetch details from JDE_ITEM_MASTER_UPDATES_URL endpoint {url} : {error_msg}")
            return None
            
    except requests.exceptions.RequestException as re:
        error_msg = f"Request exception: {re}"
        print(f"❌ Request error: {error_msg}")
        logging.error(f"Request error fetching JDE Item Master: {error_msg}")
        return None
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        print(f"❌ Unexpected error: {error_msg}")
        logging.error(f"Error fetching JDE Item Master: {error_msg}")
        return None

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

def fetch_existing_ingredient(product_name: str) -> dict:
    """Fetch an ingredient product by name"""
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
        print(f"{now} : Found ingredient products with this query {product_name}")
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
        logging.error(f"Error fetching existing ingredient product: {e}")
    print(f"{now} : No existing ingredient was found with this name: {product_name}")
    return None




def create_new_ingredient(payload: dict) -> dict:
    """Create a new ingredient product"""
    load_dotenv()

    outlet_id = os.getenv("OUTLET_ID")
    bakeryops_token = os.getenv("BAKERY_SYSTEM_TOKEN")
    bakeryops_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")

    endpoint = 'ingredients'
    url = f'{bakeryops_base_url}/outlets/{outlet_id}/{endpoint}'

    headers = {'Content-Type': 'application/json', 'Authorization': 'Access-Token {}'.format(bakeryops_token)}

    try:
        data_json = retry_request(url=url, headers=headers, method='POST', payload=payload)

        # Handle different response structures
        if isinstance(data_json, dict):
            # If the response is a direct object with _id (newer API format)
            if '_id' in data_json:
                return {'item': data_json, '_id': data_json['_id']}
            
            # If the response has the old data/rowset structure
            elif 'data' in data_json and 'rowset' in data_json['data']:
                for item in data_json['data']['rowset']:
                    return {'item': item, '_id': item['_id']}
            
            # If the response is a single item in an array-like structure
            elif isinstance(data_json, list) and len(data_json) > 0:
                item = data_json[0]
                return {'item': item, '_id': item['_id']}
        
        # Log the response structure to help debug
        print(f"Unexpected response structure from create_new_ingredient: {data_json}")
        
    except Exception as e:
        logging.error(f"Error creating new ingredient product: {e}")
        print(f"Exception details: {e}")
        print(f"Response data: {data_json if 'data_json' in locals() else 'No response data'}")

    return None




def fetch_or_create_ingredient(product_name: str, row: dict) -> dict:
    """Fetch or create an ingredient product"""
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


def fetch_or_create_ingredient_from_item_master(product_name: str, row: dict) -> dict:
    """Fetch or create an ingredient product from JDE Item Master data"""
    result = fetch_existing_ingredient(product_name)
    if result is not None:
        logging.info(f"Found existing ingredient: {product_name}. Using existing one.")
        return result

    # Only create new ingredient if none was found
    logging.info(f"No existing ingredient found for {product_name}. Creating new one from item master.")

    def raise_exception(message):
        raise ValueError(message)

    # For item master data, use F4102 and F4101 fields as specified
    # F4102_ITM: Item Number (from F4102 table)
    # F4102_LITM: Short Item Number (from F4102 table) 
    # F4101_DSC1: Description (from F4101 table)
    # F4101_UOM1: Unit of Measure (from F4101 table)
    
    item_id = row['F4102_ITM'] if not pd.isnull(row.get('F4102_ITM')) else raise_exception("Product Item ID (F4102_ITM) cannot be null")
    item_name = row['F4102_LITM'] if not pd.isnull(row.get('F4102_LITM')) else raise_exception("Product Item Name (F4102_LITM) cannot be null")
    F4101_DSC1 = row['F4101_DSC1'] if not pd.isnull(row.get('F4101_DSC1')) and str(row['F4101_DSC1']).strip() != '' else ''
    jde_unit = row['F4101_UOM1'] if not pd.isnull(row.get('F4101_UOM1')) else raise_exception("JDE Unit (F4101_UOM1) cannot be null")

    # Validate and convert the unit from Item Master data (F4101_UOM1)
    try:
        # Validate unit exists in mapping
        validate_unit(jde_unit, "F4101_UOM1")
        # Convert unit for inventory and addition unit (same value after conversion)
        inventory_unit = convert_unit(jde_unit)
        converted_addition_unit = inventory_unit
    except ValueError as e:
        # Halt the process with detailed error
        raise ValueError(f"Unit validation failed for Item Master data: {e}")

    notes_text = f"F4102_ITM: {item_id}| F4102_LITM: {item_name}| F4101_DSC1: {F4101_DSC1}| F4101_UOM1: {jde_unit}"

    payload = {
        'access': {'global': False, 'owners': []},
        'manufacturer': None,
        'defaultVendor': {'_id': '67597'},
        'productType': {'_id': 11},
        'additionCustomUnit': {'additionCustomUnit': 'false', 'additionRateUnit': None, 'additionRateValue': None, 'additionUnit': converted_addition_unit},
        'categoryFields': {'additionRateUnit': None, 'additionRateValue': None, 'additionUnit': converted_addition_unit, 'additionCustomUnit': 'false'},
        'inventoryUnit': inventory_unit,
        'name': row['F4102_LITM'] if not pd.isnull(row.get('F4102_LITM')) else '',
        'notes': [{'text': notes_text}],
        'tags': []
    }

    result = create_new_ingredient(payload)
    return result



#@lru_cache(maxsize=250)
def fetch_existing_ingredient_batch(ingredient_id: str, batch_name: str) -> dict:
    """Fetch an ingredient product batch by id and batch name"""
    load_dotenv()

    outlet_id = os.getenv("OUTLET_ID")
    bakeryops_token = os.getenv("BAKERY_SYSTEM_TOKEN")
    bakeryops_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    endpoint = 'batches?archived=false&depleted=false&includeDefaultVendor=true&includeNotes=true&size=9999'
    url = f'{bakeryops_base_url}/outlets/{outlet_id}/ingredients/{ingredient_id}/{endpoint}'

    headers = {'Content-Type': 'application/json', 'Authorization': 'Access-Token {}'.format(bakeryops_token)}

    try:
        data_json = retry_request(url=url, headers=headers, method='GET')
        
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
                existing_batch_name = str(data_json['batchNumber']).strip()
                search_batch_name = batch_name.strip()
                if existing_batch_name.lower() == search_batch_name.lower():
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
                    existing_batch_name = str(item['batchNumber']).strip()
                    search_batch_name = batch_name.strip()
                    if existing_batch_name.lower() == search_batch_name.lower():
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

    outlet_id = os.getenv("OUTLET_ID")
    bakeryops_token = os.getenv("BAKERY_SYSTEM_TOKEN")
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    
    # Get all actions for this batch
    endpoint = f'batches/{batch_id}/actions'
    bakeryops_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
    url = f'{bakeryops_base_url}/outlets/{outlet_id}/ingredients/{ingredient_id}/{endpoint}'
    
    headers = {'Content-Type': 'application/json', 'Authorization': 'Access-Token {}'.format(bakeryops_token)}

    try:
        data_json = retry_request(url=url, headers=headers, method='GET')
        
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
    """Create a new ingredient product batch"""
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
    outlet_id = os.getenv("OUTLET_ID")
    bakeryops_token = os.getenv("BAKERY_SYSTEM_TOKEN")
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    endpoint = 'batches'
    bakeryops_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
    url = f'{bakeryops_base_url}/outlets/{outlet_id}/ingredients/{ingredient_id}/{endpoint}'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Access-Token {bakeryops_token}'
    }
    print(f"{now} : Sending this payload to ingredient {ingredient_id} batch {json.dumps(payload)}")
    return retry_request(url=url, headers=headers, method='POST', payload=payload)




def fetch_or_create_ingredient_batch(ingredient_id: str, batch_name: str) -> dict:
    """Fetch or create an ingredient product batch"""
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
    
    outlet_id = os.getenv("OUTLET_ID")
    bakeryops_token = os.getenv("BAKERY_SYSTEM_TOKEN")
    bakeryops_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")

    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    
    txt_note_row = row.copy()
    txt_note_row['POSTED_PRODUCTID'] = f"'{str(row['F4111_LITM']) if pd.notnull(row['F4111_LITM']) else None}'"
    txt_note_row['POSTED_BATCHID'] = f"'{batch_name}'"
    txt_note_json = json.dumps(txt_note_row.to_json(orient='records'))
    
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
    
    headers = {'Content-Type': 'application/json', 'Authorization': 'Access-Token {}'.format(bakeryops_token)}
    endpoint = 'actions'
    url = f'{bakeryops_base_url}/outlets/{outlet_id}/{endpoint}'
    
    data_json = retry_request(url=url, headers=headers, method='POST', payload=payload)
    print(f'{now} : Here is the response we got from bakeryops after posting actionData: {json.dumps(data_json)}')
    
    return data_json


def call_bakeryops_api(url: str, payload: dict) -> dict:
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

def invalidate_ingredient_lru_cache(outlet_id: str, bakeryops_token: str, ingredient_id: str, batch_id:str):
    load_dotenv()
    endpoint = f'batches/{batch_id}/actions'
    bakeryops_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
    url = f'{bakeryops_base_url}/outlets/{outlet_id}/ingredients/{ingredient_id}/{endpoint}'
    
    headers = {'Content-Type': 'application/json', 'Authorization': 'Access-Token {}'.format(bakeryops_token)}
    invalidate_lru_cache(url=url, headers=headers,method='GET')



def submit_ingredient_batch_action(data: dict) -> list:
    """Generate the final payload for stock update"""
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
            
            # Fetch or create the batch based on batch_name (not transaction)
            batch_result_info = fetch_or_create_ingredient_batch(ingredient_id, batch_name)
            batch_result = batch_result_info['batch_result']
            is_new_batch = batch_result_info['is_new_batch']
            
            if batch_result is not None:
                batch_id = batch_result['_id']
                # Check if this specific transaction already exists in batch actions
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
                    print(f"{now}: Payload was NOT posted as the transaction {transaction_number} already exists in batch actions.")
            else:
                print(f'{now} : The batch {batch_name} was empty!')
        else:
            logging.error(f"{now}: Failed to submit ingredient batch. No batch data was provided.")
                
    return post_data




def process_full_cardex():
    load_dotenv()

    today = datetime.now()
    yesterday = today - timedelta(days=5)

    date_str = yesterday.strftime('%d/%m/%Y')
    date_time_str = today.strftime("%d/%m/%Y %H:%M:%S")

    bu = '1110'
    print(f'{date_time_str} : Processing for bu {bu} for recieved date greater than {date_str}')


def patch_one_item(row: dict):
    """Patch an ingredient item to set addition rate value and addition rate to None"""
    load_dotenv()
    cur_dt = datetime.now()
    now = cur_dt.strftime("%d/%m/%Y %H:%M:%S")
    final_updates = []
    outlet_id = os.getenv("OUTLET_ID")
    bakeryops_token = os.getenv("BAKERY_SYSTEM_TOKEN")  # Use BAKERY_SYSTEM_TOKEN to match read operations
    bakeryops_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
    
    headers = {'Content-Type': 'application/json', 'Authorization': 'Access-Token {}'.format(bakeryops_token)}
    
    # Use environment variable for base URL instead of hardcoded URL
    url_base = f'{bakeryops_base_url}/outlets/{outlet_id}'
    
    # Determine which data source we're dealing with and get the product name accordingly
    # CARDEX data uses F4111_LITM, Item Master data uses F4102_LITM
    ingredient_product_name = None
    is_item_master = False
    
    if 'F4102_LITM' in row and pd.notnull(row['F4102_LITM']):
        # Item Master data
        ingredient_product_name = str(row['F4102_LITM'])
        is_item_master = True
        print(f"{now}: Using Item Master data source (F4102_LITM)")
    elif 'F4111_LITM' in row and pd.notnull(row['F4111_LITM']):
        # CARDEX data  
        ingredient_product_name = str(row['F4111_LITM'])
        is_item_master = False
        print(f"{now}: Using CARDEX data source (F4111_LITM)")
    else:
        raise ValueError(f"No valid product name found. Available keys: {list(row.keys())}")
    
    # Use the appropriate fetch function based on data source
    if is_item_master:
        result = fetch_or_create_ingredient_from_item_master(ingredient_product_name, row)
    else:
        result = fetch_or_create_ingredient(ingredient_product_name, row)
    
    # Check if item exists or was created successfully
    if result is None:
        return {
            "success": False,
            "error": f"Item '{ingredient_product_name}' does not exist in Bakery-System and could not be created. Please add this item using Item Master review first.",
            "ingredient_name": ingredient_product_name
        }
    
    bakery_system_id = str(result['_id'])
    print(f"Processing {ingredient_product_name} , ID: {bakery_system_id}")
    
    url = f"{url_base}/ingredients/{bakery_system_id}"
    print(json.dumps(result))
    
    # Update inventory unit and additionUnit based on data source with validation
    if is_item_master and 'F4101_UOM1' in row and row['F4101_UOM1']:
        # Item Master data uses F4101_UOM1
        try:
            # Validate unit is in mapping
            validate_unit(row['F4101_UOM1'], "F4101_UOM1")
            # Convert unit for inventory and set additionUnit same as inventoryUnit
            inventory_unit = convert_unit(row['F4101_UOM1'])
            converted_addition_unit = inventory_unit
            
            result['inventoryUnit'] = inventory_unit
            result['categoryFields']['additionUnit'] = converted_addition_unit
            # Handle additionCustomUnit if it exists
            if 'additionCustomUnit' in result:
                result['additionCustomUnit']['additionUnit'] = converted_addition_unit
            print(f"{now}: Updated inventoryUnit from F4101_UOM1: {result['inventoryUnit']}")
            print(f"{now}: Updated additionUnit from F4101_UOM1: {converted_addition_unit}")
        except ValueError as e:
            # Halt the process with detailed error
            raise ValueError(f"Unit validation failed during patch for Item Master data: {e}")
            
    elif not is_item_master and 'F4111_TRUM' in row and row['F4111_TRUM']:
        # CARDEX data uses F4111_TRUM
        try:
            # Validate unit is in mapping
            validate_unit(row['F4111_TRUM'], "F4111_TRUM")
            # Convert unit for inventory and set additionUnit same as inventoryUnit
            inventory_unit = convert_unit(row['F4111_TRUM'])
            converted_addition_unit = inventory_unit
            
            result['inventoryUnit'] = inventory_unit
            result['categoryFields']['additionUnit'] = converted_addition_unit
            # Handle additionCustomUnit if it exists
            if 'additionCustomUnit' in result:
                result['additionCustomUnit']['additionUnit'] = converted_addition_unit
            print(f"{now}: Updated inventoryUnit from F4111_TRUM: {result['inventoryUnit']}")
            print(f"{now}: Updated additionUnit from F4111_TRUM: {converted_addition_unit}")
        except ValueError as e:
            # Halt the process with detailed error
            raise ValueError(f"Unit validation failed during patch for CARDEX data: {e}")
    else:
        # Keep existing inventory unit if no valid unit found in row data
        print(f"{now}: Keeping existing inventoryUnit: {result['inventoryUnit']}")
    
    # Ensure additionUnit always matches inventoryUnit (after any unit conversion above)
    inventory_unit_final = result['inventoryUnit']
    result['categoryFields'] = {
        "additionUnit": inventory_unit_final,
        "additionRateUnit": None,
        "additionRateValue": None,
        "additionCustomUnit": False,
        "concentration": None,
        "instructions": ""
    }
    result['defaultVendorId'] = None
    result['defaultVendor'] = None
    result['indicators'] = []
    
    print(f"{now}: Final categoryFields set with additionUnit: {inventory_unit_final}")
    
    print("sending: " + json.dumps(result))
    upd_result = retry_request(url=url, headers=headers, method='PUT', payload=result)
    final_updates.append(upd_result)
    return final_updates




def prepare_jde_payload(batch_data):
    """
    Prepare JDE payload without dispatching - for preview and editing
    
    Expected batch_data format:
    {
        "action_id": str,
        "ingredient_id": str,  
        "ingredient_name": str,
        "batch_id": str,
        "batch_number": str,
        "lot_number": str,
        "quantity": float,
        "unit": str,
        "vessel_id": str (optional)
    }
    """
    from datetime import datetime
    from utility import convert_unit, normalize_quantity_for_transaction_id, preserve_quantity_precision
    from decimal import Decimal
    
    # Validate required fields
    required_fields = ['action_id', 'ingredient_id', 'ingredient_name', 'batch_id', 'quantity', 'unit']
    missing_fields = [field for field in required_fields if not batch_data.get(field)]
    
    if missing_fields:
        return {
            "success": False,
            "error": f"Missing required fields: {', '.join(missing_fields)}"
        }
    
    # Check for zero or null quantities
    quantity = batch_data.get('quantity')
    if quantity is None or quantity == 0 or (isinstance(quantity, str) and quantity.strip() in ['0', '', '0.0']):
        return {
            "success": False,
            "error": "Cannot dispatch batch with zero or null quantity. This transaction will be skipped."
        }
    
    # Check if already dispatched using unique transaction ID
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Create unique transaction ID with normalized quantity
        normalized_quantity = normalize_quantity_for_transaction_id(batch_data['quantity'])
        unique_transaction_id = f"{batch_data['ingredient_name']}_{batch_data.get('lot_number', '')}_{batch_data.get('vessel_code', '')}_{normalized_quantity}"
        
        cur.execute("""
            SELECT status FROM ingredient_submitted_status 
            WHERE unique_transaction_id = %s AND status = 'done';
        """, (unique_transaction_id,))
        
        result = cur.fetchone()
        if result and result[0] == 'done':
            return {
                "success": False,
                "error": f"Transaction {unique_transaction_id} already dispatched"
            }
        
        # Convert unit for JDE
        converted_unit = convert_unit(batch_data['unit'], direction='to_jde')
        if not converted_unit:
            converted_unit = batch_data['unit']  # Fallback to original unit
        
        # Determine business unit from product name
        product_name = batch_data['ingredient_name']
        bu = "1110"  # default
        bu_map = {
            "B_": "1110",
            "P_": "1130",
            "M_": "1120"
        }
        
        for prefix, business_unit in bu_map.items():
            if product_name.startswith(prefix):
                bu = business_unit
                break
        
        # Extract lot number from batch number
        batch_number = batch_data.get('batch_number', '')
        lot_number = batch_data.get('lot_number', '')
        
        if not lot_number and batch_number:
            # Remove product name prefix to get lot number
            lot_number = batch_number.replace(f"{product_name}_", "", 1)
        
        if not lot_number:
            lot_number = batch_data['batch_id']  # Fallback to batch_id
        
        # Prepare JDE payload
        current_date = datetime.utcnow().strftime("%d/%m/%Y")
        
        jde_payload = {
            "Branch_Plant": bu,
            "Document_Type": "II",
            "Explanation": f"BAKERYOPS. DEPL: {batch_data['ingredient_id']}:{batch_data['action_id']}",
            "Select_Row": "1",
            "GridData": [
                {
                    "Item_Number": product_name,
                    "Quantity": str(batch_data['quantity']),
                    "UM": converted_unit,
                    "LOTN": lot_number
                }
            ],
            "G_L_Date": current_date,
            "Transaction_Date": current_date
        }
        
        return {
            "success": True,
            "jde_payload": jde_payload,
            "original_batch": batch_data,
            "meta_info": {
                "converted_unit": converted_unit,
                "determined_bu": bu,
                "extracted_lot_number": lot_number
            }
        }
            
    except Exception as e:
        return {
            "success": False,
            "error": f"Error preparing payload: {str(e)}"
        }
    finally:
        cur.close()
        conn.close()


def dispatch_prepared_payload_to_jde(jde_payload, batch_data):
    """
    Dispatch a pre-prepared and potentially edited JDE payload
    
    Args:
        jde_payload: The JDE payload dict ready to be sent
        batch_data: Original batch data for logging purposes
    """
    from datetime import datetime
    import requests
    from requests.auth import HTTPBasicAuth
    import json
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Get JDE credentials
    url = os.getenv("JDE_IA_URL")
    username = os.getenv("JDE_CARDEX_USERNAME") 
    password = os.getenv("JDE_CARDEX_PASSWORD")
    
    if not all([url, username, password]):
        return {
            "success": False,
            "error": "JDE credentials not configured"
        }
    
    # Check if already dispatched one more time using unique transaction ID
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Create unique transaction ID with normalized quantity
        from utility import normalize_quantity_for_transaction_id
        normalized_quantity = normalize_quantity_for_transaction_id(batch_data['quantity'])
        unique_transaction_id = f"{batch_data['ingredient_name']}_{batch_data.get('lot_number', '')}_{batch_data.get('vessel_code', '')}_{normalized_quantity}"
        
        cur.execute("""
            SELECT status FROM ingredient_submitted_status 
            WHERE unique_transaction_id = %s AND status = 'done';
        """, (unique_transaction_id,))
        
        result = cur.fetchone()
        if result and result[0] == 'done':
            return {
                "success": False,
                "error": f"Transaction {unique_transaction_id} already dispatched"
            }
        
        # Send to JDE
        headers = {'Content-Type': 'application/json'}
        auth = HTTPBasicAuth(username, password)
        
        response = requests.post(url, headers=headers, auth=auth, json=jde_payload, verify=False)
        
        # Process response
        status_text = ""
        try:
            json_data = response.json()
            status_text = str(json_data)[:699]  # Limit to 699 chars
        except ValueError:
            status_text = (response.text or "")[:699]
        
        if response.status_code in [200, 201]:
            # Mark as dispatched in database using unique transaction ID
            cur.execute("""
                INSERT INTO ingredient_submitted_status 
                (action_id, ingredient_id, lot_id, ingredient_name, addition_unit, status_text, status, unique_transaction_id, vessel_code)
                VALUES (%s, %s, %s, %s, %s, %s, 'done', %s, %s)
                ON CONFLICT (unique_transaction_id) DO UPDATE
                SET status_text = EXCLUDED.status_text, 
                    status = 'done';
            """, (
                batch_data['action_id'],
                batch_data['ingredient_id'],
                batch_data['batch_id'],
                batch_data.get('ingredient_name', ''),
                batch_data.get('unit', ''),
                status_text,
                unique_transaction_id,
                batch_data.get('vessel_code', '')
            ))
            conn.commit()
            
            return {
                "success": True,
                "message": f"Successfully dispatched transaction {unique_transaction_id} to JDE",
                "jde_response": status_text,
                "payload_sent": jde_payload
            }
        else:
            return {
                "success": False,
                "error": f"JDE API error {response.status_code}: {status_text}",
                "payload_sent": jde_payload
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": f"Network or database error: {str(e)}"
        }
    finally:
        cur.close()
        conn.close()


def dispatch_single_batch_to_jde(batch_data):
    """
    Simplified function to dispatch a single batch to JDE
    
    Expected batch_data format:
    {
        "action_id": str,
        "ingredient_id": str,  
        "ingredient_name": str,
        "batch_id": str,
        "batch_number": str,
        "lot_number": str,
        "quantity": float,
        "unit": str,
        "vessel_id": str (optional)
    }
    """
    
    load_dotenv()
    
    # Validate required fields
    required_fields = ['action_id', 'ingredient_id', 'ingredient_name', 'batch_id', 'quantity', 'unit']
    missing_fields = [field for field in required_fields if not batch_data.get(field)]
    
    if missing_fields:
        return {
            "success": False,
            "error": f"Missing required fields: {', '.join(missing_fields)}"
        }
    
    # Check for zero or null quantities
    quantity = batch_data.get('quantity')
    if quantity is None or quantity == 0 or (isinstance(quantity, str) and quantity.strip() in ['0', '', '0.0']):
        return {
            "success": False,
            "error": "Cannot dispatch batch with zero or null quantity. This transaction will be skipped."
        }
    
    # Check if already dispatched using unique transaction ID
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Create unique transaction ID with normalized quantity
        from utility import normalize_quantity_for_transaction_id
        normalized_quantity = normalize_quantity_for_transaction_id(batch_data['quantity'])
        unique_transaction_id = f"{batch_data['ingredient_name']}_{batch_data.get('lot_number', '')}_{batch_data.get('vessel_code', '')}_{normalized_quantity}"
        
        cur.execute("""
            SELECT status FROM ingredient_submitted_status 
            WHERE unique_transaction_id = %s AND status = 'done';
        """, (unique_transaction_id,))
        
        result = cur.fetchone()
        if result and result[0] == 'done':
            return {
                "success": False,
                "error": f"Transaction {unique_transaction_id} already dispatched"
            }
        
        # Convert unit for JDE
        converted_unit = convert_unit(batch_data['unit'], direction='to_jde')
        if not converted_unit:
            return {
                "success": False,
                "error": f"Unable to convert unit: {batch_data['unit']}"
            }
        
        # Determine business unit from product name
        product_name = batch_data['ingredient_name']
        bu = "1110"  # default
        bu_map = {
            "B_": "1110",
            "P_": "1130",
            "M_": "1120"
        }
        
        for prefix, business_unit in bu_map.items():
            if product_name.startswith(prefix):
                bu = business_unit
                break
        
        # Extract lot number from batch number
        batch_number = batch_data.get('batch_number', '')
        lot_number = batch_data.get('lot_number', '')
        
        if not lot_number and batch_number:
            # Remove product name prefix to get lot number
            lot_number = batch_number.replace(f"{product_name}_", "", 1)
        
        if not lot_number:
            return {
                "success": False,
                "error": "Unable to determine lot number"
            }
        
        # Prepare JDE payload
        jde_payload = {
            "Branch_Plant": bu,
            "Document_Type": "II",
            "Explanation": f"BAKERYOPS. DEPL: {batch_data['ingredient_id']}:{batch_data['action_id']}",
            "Select_Row": "1",
            "GridData": [
                {
                    "Item_Number": product_name,
                    "Quantity": str(batch_data['quantity']),
                    "UM": converted_unit,
                    "LOTN": lot_number
                }
            ],
            "G_L_Date": datetime.utcnow().strftime("%d/%m/%Y"),
            "Transaction_Date": datetime.utcnow().strftime("%d/%m/%Y")
        }
        
        # Get JDE credentials
        url = os.getenv("JDE_IA_URL")
        username = os.getenv("JDE_CARDEX_USERNAME") 
        password = os.getenv("JDE_CARDEX_PASSWORD")
        
        if not all([url, username, password]):
            return {
                "success": False,
                "error": "JDE credentials not configured"
            }
        
        # Send to JDE
        headers = {'Content-Type': 'application/json'}
        auth = HTTPBasicAuth(username, password)
        
        response = requests.post(url, headers=headers, auth=auth, json=jde_payload, verify=False)
        
        # Process response
        status_text = ""
        try:
            json_data = response.json()
            status_text = str(json_data)[:699]  # Limit to 699 chars
        except ValueError:
            status_text = (response.text or "")[:699]
        
        if response.status_code in [200, 201]:
            # Mark as dispatched in database using unique transaction ID
            cur.execute("""
                INSERT INTO ingredient_submitted_status 
                (action_id, ingredient_id, lot_id, ingredient_name, addition_unit, status_text, status, unique_transaction_id, vessel_code)
                VALUES (%s, %s, %s, %s, %s, %s, 'done', %s, %s)
                ON CONFLICT (unique_transaction_id) DO UPDATE
                SET status_text = EXCLUDED.status_text, 
                    status = 'done';
            """, (
                batch_data['action_id'],
                batch_data['ingredient_id'],
                batch_data['batch_id'],
                batch_data.get('ingredient_name', ''),
                batch_data.get('unit', ''),
                status_text,
                unique_transaction_id,
                batch_data.get('vessel_code', '')
            ))
            conn.commit()
            
            return {
                "success": True,
                "message": f"Successfully dispatched transaction {unique_transaction_id} to JDE",
                "jde_response": status_text,
                "payload_sent": jde_payload
            }
        else:
            return {
                "success": False,
                "error": f"JDE API error {response.status_code}: {status_text}",
                "payload_sent": jde_payload
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": f"Database or network error: {str(e)}"
        }
    finally:
        cur.close()
        conn.close()


def dispatch_bakery_system_batches_to_jde(data):
    """Fetch purchase orders from JDE"""

    load_dotenv()  # loads variables from .env into environment variables

    outlet_id = os.getenv("OUTLET_ID")
    bakery_system_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
    bakery_system_api_token = os.getenv("BAKERY_SYSTEM_API_TOKEN", default=None)
    stical_target_api = os.getenv("STICAL_TARGET_API")
    stical_api_token = os.getenv("STICAL_API_TOKEN", default=None)

    url = os.getenv("JDE_IA_URL")
    username = os.getenv("JDE_CARDEX_USERNAME")
    password = os.getenv("JDE_CARDEX_PASSWORD")

    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(username, password)
    
    items = json.loads(data)

    # Connect to DB
    conn = get_db_connection()

    resp_arr = []

    try:
        for idx, item in enumerate(items):
            unique_id = item['key']
            # Check if the action_id and ingredient_id is already processed
            action_id = item['value']['action_id']
            ingredient_id = item['value']['ingredient_id']
            resp_arr.append({"INFO": f"==========> Processing: {json.dumps(item)} <=========="})
            cur = conn.cursor()
            cur.execute("""
                SELECT status FROM ingredient_submitted_status 
                WHERE action_id = %s AND ingredient_id = %s;
            """, (action_id, ingredient_id))

            result = cur.fetchone()

            if result and result[0] == 'done':
                # Already processed, skip
                resp_arr.append({"ERROR": f"==========> Action {action_id} for Ingredient {ingredient_id} already marked as done. Skipping... <=========="})
                continue

            # Convert addition_unit using unit conversion map
            original_unit = item["value"]["addition_unit"] if "addition_unit" in item["value"] else None
            converted_unit = convert_unit(item["value"]["addition_unit"], direction='to_jde') if "addition_unit" in item["value"] else None
            original_quantity = item["value"]["change_value"] if "change_value" in item["value"] else None

            # Extract quantity and UOM from actionData
            change_value = original_quantity
            uom = original_unit
            productName = item["value"]["ingredient_name"] if "ingredient_name" in item["value"] else ""
            


            bu = "1110"
            bu_map = {
                "B_": "1110",
                "P_": "1130",
                "M_": "1120"
            }

            if productName.startswith(tuple(bu_map.keys())):
                bu = bu_map[productName[:2]]

            batches = item['value']['batches']
            
            for batch in batches:
                batch_key = batch['key']
                unique_id = f"{unique_id}:{batch_key}"

            lotn = ""
            # Todo: need to obtain batchNumber and then remove the productName+"_" from batchNumber to get the actual LOTN
            batch_number = batch.get("batchNumber", "")
            if batch_number:
                lotn = batch_number.replace(f"{productName}_", "", 1)

            if change_value == None or lotn == "" or productName == "":
                resp_arr.append({"ERROR": f"==========> One of the required fields for {productName} was empty. Skipping. <=========="})
                continue            
            # Accepts this type of payload:
            #{
            #    "Transaction_Date": "string",
            #    "Document_Type": "string",
            #    "Branch_Plant": "string",
            #    "G_L_Date": "string",
            #    "Explanation": "string",
            #    "Select_Row": "string",
            #    "GridData": [
            #        {
            #        "Item_Number": "string",
            #        "Quantity": "string",
            #        "UM": "string",
            #        "LOTN": "string"
            #        }
            #    ]
            #}
            # Create sample payload for JDE
            sample_payload_for_jde = {
                "Branch_Plant": bu,
                "Document_Type": "II",
                "Explanation": f"BAKERYOPS. DEPL: {ingredient_id}:{action_id}",
                "Select_Row": "1",
                "GridData": [
                    {
                        "Item_Number": productName,  # Replace with actual item number
                        "Quantity": f"{str(change_value)}",
                        "UM": converted_unit,
                        "LOTN": lotn
                    }
                ],
                "G_L_Date": datetime.utcnow().strftime("%d/%m/%Y"),
                "Transaction_Date": datetime.utcnow().strftime("%d/%m/%Y")
            }


            # Post data to JDE
            response = requests.post(url, headers=headers, auth=auth, json=sample_payload_for_jde, verify=False)
            status_text = ""
            try:
                json_data = response.json()
                if json_data is not None:
                    status_text = str(json_data)
                else:
                    status_text = response.text or ""
            except ValueError:
                # If response is not valid JSON, fall back to text (or empty string if None)
                status_text = response.text or ""

            # Trim to 699 characters
            status_text = status_text[:699]

            if response.status_code == 200 or response.status_code == 201:
                # Mark as "done" in DB after successful post
                cur.execute("""
                    INSERT INTO ingredient_submitted_status 
                    (action_id, ingredient_id, lot_id, ingredient_name, addition_unit, status_text, status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'done')
                    ON CONFLICT (action_id, ingredient_id, lot_id) DO UPDATE
                    SET status_text = EXCLUDED.status_text, 
                    status = 'done';
                """, (
                    action_id,
                    ingredient_id,
                    unique_id,
                    productName,
                    uom,
                    status_text
                ))
                conn.commit()
                resp_arr.append({"info": f"=========> Posted: {json.dumps(sample_payload_for_jde)} <=========="})
                resp_arr.append(response.json())  # Use append instead of push and use response.json() directly
            else:
                cur.execute("""
                    INSERT INTO ingredient_submitted_status 
                    (action_id, ingredient_id, lot_id, ingredient_name, addition_unit, status_text, status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'error')
                    ON CONFLICT (action_id, ingredient_id, lot_id) DO UPDATE
                    SET status_text = EXCLUDED.status_text, 
                    status = 'error';
                """, (
                    action_id,
                    ingredient_id,
                    unique_id,
                    productName,
                    uom,
                    status_text
                ))
                conn.commit()
                resp_arr.append({"ERROR": f"==========> Error processing {productName} :  {response} <=========="})
                resp_arr.append({"api_ERROR" : f"Error Sending data to endpoint : {url}"})
    finally:
        if conn:
            conn.close()

    return resp_arr


if __name__ == "__main__":
    today = datetime.now()
    yesterday = today - timedelta(days=7)
    date_str = yesterday.strftime('%d/%m/%Y')
    date_time_str = today.strftime("%d/%m/%Y %H:%M:%S")
    bu = '1110'
    print(f'{date_time_str} : Processing for bu {bu} for recieved date greater than {date_str}')
    
    data = get_latest_jde_cardex(bu, date_str)
    post_data = submit_ingredient_batch_action(data)
    print(f"{date_time_str}: Posted Data =====> {json.dumps(post_data)}")

