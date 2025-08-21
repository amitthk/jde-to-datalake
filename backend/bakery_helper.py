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
from utility import retry_request

def get_db_connection():
    """Create and return a PostgreSQL connection"""
    # Get the directory where this script is located
    current_dir = Path(__file__).parent
    env_path = current_dir / '.env'
    
    # Load environment variables from the backend directory
    load_dotenv(env_path)
    
    PG_DATABASE_URL = os.getenv("PG_DATABASE_URL")
    if not PG_DATABASE_URL:
        raise ValueError("Missing environment variable: PG_DATABASE_URL")
    
    DB_NAME = os.getenv("DB_NAME") or "inventory_backup_db"
    schema_name = f"{DB_NAME}_schema"
    
    conn = psycopg2.connect(PG_DATABASE_URL)
    
    # Set the search path to the schema
    try:
        with conn.cursor() as cursor:
            # Create schema if it doesn't exist
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
            # Set search path to the schema
            cursor.execute(f'SET search_path TO "{schema_name}"')
            conn.commit()
            print(f"✅ Set schema to '{schema_name}'")
    except Exception as e:
        print(f"❌ Error setting schema '{schema_name}': {e}")
        conn.rollback()
        raise
    
    return conn

def drop_table(conn, table_name):
    """Drop table if it exists"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.commit()
            print(f"✅ Dropped table '{table_name}' if it existed")
    except Exception as e:
        print(f"❌ Error dropping table '{table_name}': {e}")
        conn.rollback()

def create_table(conn, table_name, columns):
    """Create table with given columns (all TEXT type)"""
    try:
        with conn.cursor() as cursor:
            # Escape column names with double quotes to handle special characters
            column_definitions = [f'"{col}" TEXT' for col in columns]
            create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(column_definitions)})'
            cursor.execute(create_sql)
            conn.commit()
            print(f"✅ Created table '{table_name}' with {len(columns)} columns")
    except Exception as e:
        print(f"❌ Error creating table '{table_name}': {e}")
        conn.rollback()

def insert_into_table(conn, table_name, df):
    """Insert DataFrame data into table"""
    try:
        with conn.cursor() as cursor:
            # Prepare column names (escaped with double quotes)
            columns = [f'"{col}"' for col in df.columns]
            columns_str = ", ".join(columns)
            
            # Convert DataFrame to list of tuples with proper type handling
            data_tuples = []
            for row in df.values:
                processed_row = []
                for value in row:
                    if isinstance(value, dict):
                        # Convert dict to JSON string
                        processed_row.append(json.dumps(value))
                    elif isinstance(value, list):
                        # Convert list to JSON string
                        processed_row.append(json.dumps(value))
                    elif pd.isna(value):
                        # Handle NaN/None values
                        processed_row.append(None)
                    else:
                        processed_row.append(value)
                data_tuples.append(tuple(processed_row))
            
            # Use execute_values for efficient bulk insert
            insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES %s'
            execute_values(cursor, insert_sql, data_tuples)
            conn.commit()
            print(f"✅ Inserted {len(data_tuples)} rows into '{table_name}'")
    except Exception as e:
        print(f"❌ Error inserting data into '{table_name}': {e}")
        conn.rollback()

def insert_into_table_alternate(conn, table_name, df):
    """Insert DataFrame data into table with robust type handling"""
    try:
        with conn.cursor() as cursor:
            # Prepare column names (escaped with double quotes)
            columns = [f'"{col}"' for col in df.columns]
            columns_str = ", ".join(columns)
            
            # Create a copy of the DataFrame to avoid modifying the original
            df_copy = df.copy()
            
            # Process each column based on its data type
            for col in df_copy.columns:
                # Check if column contains dict/list objects
                if df_copy[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    # Convert dict/list to JSON strings
                    df_copy[col] = df_copy[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                    )
            
            # Replace NaN with None for proper NULL handling
            df_copy = df_copy.where(pd.notnull(df_copy), None)
            
            # Convert to list of tuples
            data_tuples = [tuple(row) for row in df_copy.values]
            
            # Use execute_values for efficient bulk insert
            insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES %s'
            execute_values(cursor, insert_sql, data_tuples)
            conn.commit()
            print(f"✅ Inserted {len(data_tuples)} rows into '{table_name}'")
    except Exception as e:
        print(f"❌ Error inserting data into '{table_name}': {e}")
        print(f"DataFrame dtypes: {df.dtypes}")
        print(f"Sample data: {df.head()}")
        conn.rollback()

# Quick diagnostic function to identify problematic columns
def diagnose_dataframe(df):
    """Diagnose DataFrame for potential insertion issues"""
    print("DataFrame Analysis:")
    print("-" * 50)
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print("\nData types:")
    for col in df.columns:
        dtype = df[col].dtype
        has_dict = df[col].apply(lambda x: isinstance(x, dict)).any()
        has_list = df[col].apply(lambda x: isinstance(x, list)).any()
        has_nan = df[col].isna().any()
        print(f"  {col}: {dtype} | Dict: {has_dict} | List: {has_list} | NaN: {has_nan}")
    
    print("\nSample values for problematic columns:")
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            print(f"  {col}: {sample_value}")

def process_api_data(input_data):
    """Process API data and insert into PostgreSQL database"""
    # Get the target table name from environment variable or default to 'bakery_system_dry_goods_inventory'
    table_name = os.getenv("BAKERY_SYSTEM_INVENTORY_TABLE_NAME", "bakery_system_dry_goods_inventory")
    
    conn = get_db_connection()
    
    try:
        if not input_data:
            print("⚠️ No data provided for insertion.")
            return

        # Create DataFrame directly from the list of items
        df = pd.DataFrame(input_data)

        # Ensure all column names are strings (in case some keys were numbers)
        df.columns = [str(col) for col in df.columns]

        # Replace NaN values with empty strings to ensure consistency
        df = df.fillna('')

        # Create the table if it doesn't exist
        create_table(conn, table_name, df.columns.tolist())

        diagnose_dataframe(df)

        # Insert data into the table
        insert_into_table(conn, table_name, df)

    finally:
        conn.close()
        print("✅ Database connection closed")


def fetch_existing_ingredient_by_id(ingredient_id: str) -> dict:
    """Fetch an Ingredient product by ID"""
    load_dotenv()

    outlet_id = os.getenv("OUTLET_ID")
    bakeryops_token = os.getenv("BAKERY_SYSTEM_TOKEN")
    bakeryops_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
    
    endpoint = f'ingredients/{ingredient_id}'
    url = f'{bakeryops_base_url}/outlets/{outlet_id}/{endpoint}'

    headers = {'Content-Type': 'application/json', 'Authorization': 'Access-Token {}'.format(bakeryops_token)}
    
    try:
        data_json = retry_request(url=url, headers=headers, method='GET')
        
        if data_json is None:
            print(f"API returned None for Ingredient ID: {ingredient_id}")
            return None
        
        print(f"Found Ingredient by ID {ingredient_id}: {data_json.get('name', 'Unknown')}")
        return data_json
        
    except Exception as e:
        logging.error(f"Error fetching existing Ingredient by ID {ingredient_id}: {e}")
        print(f"Exception occurred while fetching Ingredient by ID {ingredient_id}: {str(e)}")
        return None


def get_data_from_bakery_system() -> dict:
    """Fetch stocks from Bakery-System with rate limit handling."""
    # Get the directory where this script is located
    current_dir = Path(__file__).parent
    env_path = current_dir / '.env'
    
    # Load environment variables from the backend directory
    load_dotenv(env_path)
    
    outlet_id = os.getenv("OUTLET_ID")
    bakery_system_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
    bakery_system_api_token = os.getenv("BAKERY_SYSTEM_API_TOKEN")
    
    # Debug print to check if variables are loaded
    print(f"Debug - Bakery-System URL base: {bakery_system_base_url}")
    print(f"Debug - Outlet ID: {outlet_id}")
    print(f"Debug - Token available: {'Yes' if bakery_system_api_token else 'No'}")
    
    if not bakery_system_api_token:
        print("❌ Error: BAKERY_SYSTEM_API_TOKEN not found in environment variables")
        return None
    
    if not outlet_id:
        print("❌ Error: OUTLET_ID not found in environment variables")
        return None
        
    if not bakery_system_base_url:
        print("❌ Error: BAKERY_SYSTEM_BASE_URL not found in environment variables")
        return None

    headers = {"Content-Type": "application/json"}
    headers["Authorization"] = f"Access-Token {bakery_system_api_token}"
    
    url = f"{bakery_system_base_url}/outlets/{outlet_id}/ingredients?archived=false&includeAccess=true&includeBatches=true&includeNotes=true&offset=0&productCategory=Ingredient&size=100000&sort=productName:1"
    
    print(f"Making request to: {url}")

    retry_count = 0
    max_retries = 3
    
    while retry_count <= max_retries:
        try:
            response = requests.get(
                url,
                headers=headers,
                verify=False,
                timeout=30
            )
            
            print(f"Response status code: {response.status_code}")
            print(f"Response headers: {dict(response.headers)}")

            if response.status_code in (200, 201):
                try:
                    response_text = response.text
                    print(f"Response text length: {len(response_text)}")
                    
                    json_data = json.loads(response_text)
                    print(f"Successfully parsed JSON data with {len(json_data) if isinstance(json_data, list) else 'unknown'} items")
                    return json_data
                    
                except json.JSONDecodeError as je:
                    print(f"❌ JSON decode error: {je}")
                    print(f"Raw response preview: {response_text[:500]}...")
                    return None
                    
            elif response.status_code == 429:
                # Attempt to extract wait time from the metadata
                try:
                    parsed_response = json.loads(response.text)
                    wait_seconds = parsed_response.get("metadata", {}).get("wait", 0)

                    if wait_seconds > 0:
                        print(f"Rate limited. Waiting {wait_seconds} seconds before retrying.")
                        logging.warning(f"Rate limited. Waiting {wait_seconds} seconds before retrying.")
                        time.sleep(wait_seconds)
                    else:
                        # Default to a 60-second retry interval
                        print("Rate limit reached, waiting 60 seconds before retrying.")
                        logging.warning("Rate limit reached, waiting 60 seconds before retrying.")
                        time.sleep(60)

                except Exception as e:
                    print(f"❌ Error parsing rate limit response: {e}")
                    logging.error(f"Error parsing response: {e}")

                retry_count += 1
                continue  # Retry the request after the wait

            else:
                error_msg = f"HTTP {response.status_code}: {response.reason}"
                try:
                    error_detail = response.text
                    print(f"❌ Error response: {error_detail}")
                    error_msg += f" - {error_detail}"
                except:
                    pass
                
                print(f"❌ Failed to fetch from Bakery-System: {error_msg}")
                logging.error(
                    f"Non-429 error occurred. Status Code: {response.status_code}. "
                    f"Failed to fetch details from Bakery-System endpoint {url}. Error: {error_msg}"
                )
                return None

        except requests.exceptions.Timeout:
            print(f"❌ Request timeout (attempt {retry_count + 1}/{max_retries + 1})")
            retry_count += 1
            if retry_count <= max_retries:
                time.sleep(5)  # Wait 5 seconds before retrying
                continue
            else:
                logging.error("Request timeout after maximum retries")
                return None
                
        except requests.exceptions.RequestException as re:
            print(f"❌ Request exception: {re}")
            logging.error(f"Request error fetching Bakery-System data: {re}")
            return None
            
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            logging.error(f"Error fetching Bakery-System data: {e}")
            return None
    
    print("❌ Maximum retries exceeded")
    return None

def main():
    load_dotenv()

    today = datetime.now()
    yesterday = today - timedelta(days=7)
    
    date_str = yesterday.strftime('%d/%m/%Y')
    date_time_str = today.strftime("%d/%m/%Y %H:%M:%S")

    data = get_data_from_bakery_system()
    if data is not None and isinstance(data, list):
        process_api_data(data)
    else:
        logging.error("Failed to retrieve BAKERY-SYSTEM data")
    
    print(f"{date_time_str}: Processed Data =====> {json.dumps(data)}")


def get_streamlined_action_data(start_date=None):
    """
    Get streamlined Bakery-System action data with individual batches ready for dispatch
    
    Returns list of individual batch records with all data needed for JDE dispatch:
    [
        {
            "action_id": str,
            "ingredient_id": str,
            "ingredient_name": str,
            "batch_id": str,
            "batch_number": str,
            "lot_number": str,
            "quantity": float,
            "unit": str,
            "vessel_id": str,
            "vessel_code": str,
            "action_date": str,
            "depleted": bool,
            "already_dispatched": bool,
            "bakery_system_lot_id": str,
            "bakery_system_lot_code": str,
            "bakery_system_lot_stage": str,
            "bakery_system_lot_color": str
        }
    ]
    """
    from datetime import datetime, timedelta
    import json
    
    # Default to last 3 days if no start date provided
    if not start_date:
        end_date = datetime.now()
        start_date_obj = end_date - timedelta(days=3)
        start_date = start_date_obj.strftime('%Y-%m-%d')
    
    try:
        # Fetch raw action data
        raw_data = fetch_action_data_from_bakery_system_api(start_date=start_date)
        if not raw_data:
            return []
        
        # Parse and streamline
        streamlined_batches = []
        
        # Check what's already dispatched
        from jde_helper import get_db_connection
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT unique_transaction_id 
                FROM ingredient_submitted_status 
                WHERE status = 'done' AND unique_transaction_id IS NOT NULL
            """)
            dispatched_records = {row[0] for row in cur.fetchall()}
            print(f"DEBUG - Found {len(dispatched_records)} already dispatched transactions")
        finally:
            cur.close()
            conn.close()
        
        # Process each action
        raw_json_data = json.loads(raw_data)
        print(f"DEBUG - Total entries from API: {len(raw_json_data)}")
        
        addition_count = 0
        for entry in raw_json_data:
            action_type = entry.get("actionType")
            if action_type == "ADDITION":
                addition_count += 1
        
        print(f"DEBUG - ADDITION entries found: {addition_count}")
        
        for entry in raw_json_data:
            action_type = entry.get("actionType")
            if action_type != "ADDITION":
                continue
                
            action_id = entry.get("_id")
            action_date = entry.get("effectiveAt", "")
            print(f"DEBUG - Processing ADDITION action {action_id} from {action_date}")
            
            # Build Ingredient summary and batch summary first
            ingredient_summary = {}
            ingredient_units = {}
            batch_summary = []

            ingredients = entry.get("actionData", {}).get("ingredients", [])
            lots = entry.get("actionData", {}).get("lots", [])
            
            print(f"DEBUG - Action {action_id}: found {len(ingredients)} ingredients and {len(lots)} lots")

            # Process ingredients to build lookup tables
            for ingredient_entry in ingredients:
                Ingredient = ingredient_entry.get("Ingredient", {})
                ingredient_id = str(Ingredient.get("_id"))
                ingredient_name = Ingredient.get("productName")
                addition_unit = Ingredient.get("additionUnit")

                ingredient_summary[ingredient_id] = ingredient_name
                ingredient_units[ingredient_id] = addition_unit

                # Process batches for this Ingredient
                for batch_entry in ingredient_entry.get("batches", []):
                    batch = batch_entry.get("batch", {})
                    batch_id = batch.get("_id")
                    batch_number = batch.get("batchNumber")
                    depleted = batch.get("depleted", False)

                    batch_key = f"Ingredient:{ingredient_id}:batch:{batch_id}"
                    batch_summary.append({
                        "key": batch_key, 
                        "ingredient_id": ingredient_id,
                        "batch_id": batch_id,
                        "batch_number": batch_number,
                        "depleted": depleted
                    })

            # Process lots and vessels to get actual quantities
            for lot_entry in lots:
                bakery_system_lot_id = lot_entry.get("_id")
                bakery_system_lot_code = lot_entry.get("lotCode", "")
                bakery_system_lot_stage = lot_entry.get("stage")
                bakery_system_lot_color = lot_entry.get("color", "")
                
                for vessel_entry in lot_entry.get("vessels", []):
                    vessel_id = vessel_entry.get("_id")
                    vessel_code = vessel_entry.get("vesselCode", "")
                    vessel_name = vessel_entry.get("name", "")
                    additions = vessel_entry.get("additions", {})

                    # For each Ingredient that has additions in this vessel
                    for ingredient_id_key, change_value in additions.items():
                        ingredient_id = str(ingredient_id_key)
                        
                        # Get Ingredient info
                        ingredient_name = ingredient_summary.get(ingredient_id, "")
                        addition_unit = ingredient_units.get(ingredient_id, "")
                        
                        # Find all batches for this Ingredient
                        batch_summary_entries = [b for b in batch_summary if b["ingredient_id"] == ingredient_id]
                        
                        print(f"DEBUG - Processing Ingredient {ingredient_name} ({ingredient_id}) with quantity {change_value}")
                        
                        # Create individual batch records with the quantity distributed
                        # For now, we'll assign the full quantity to each batch (this might need refinement)
                        for batch_info in batch_summary_entries:
                            # Extract lot number (remove product name prefix)
                            lot_number = batch_info["batch_number"].replace(f"{ingredient_name}_", "", 1) if batch_info["batch_number"] else ""
                            
                            # Normalize quantity to ensure consistent formatting in unique ID
                            from utility import normalize_quantity_for_transaction_id, preserve_quantity_precision
                            normalized_quantity = normalize_quantity_for_transaction_id(change_value)
                            
                            # Create unique transaction identifier with quantity to handle multiple additions
                            unique_transaction_id = f"{ingredient_name}_{lot_number}_{vessel_code}_{normalized_quantity}"
                            
                            # Check if already dispatched using unique transaction ID
                            already_dispatched = unique_transaction_id in dispatched_records
                            
                            # Create streamlined record
                            batch_record = {
                                "action_id": action_id,
                                "ingredient_id": ingredient_id,
                                "ingredient_name": ingredient_name,
                                "batch_id": batch_info["batch_id"],
                                "batch_number": batch_info["batch_number"],
                                "lot_number": lot_number,
                                "quantity": preserve_quantity_precision(change_value) if change_value else 0.0,  # Preserve up to 9 decimal places
                                "unit": addition_unit,
                                "vessel_id": vessel_name,
                                "vessel_code": vessel_code,
                                "action_date": action_date,
                                "depleted": batch_info["depleted"],
                                "already_dispatched": already_dispatched,
                                "unique_transaction_id": unique_transaction_id,  # Add the unique ID to the record
                                "bakery_system_lot_id": bakery_system_lot_id,
                                "bakery_system_lot_code": bakery_system_lot_code,
                                "bakery_system_lot_stage": bakery_system_lot_stage,
                                "bakery_system_lot_color": bakery_system_lot_color,
                            }
                            
                            print(f"DEBUG - Created batch record: {batch_record['ingredient_name']} - {batch_record['quantity']} {batch_record['unit']}")
                            streamlined_batches.append(batch_record)
        
        print(f"DEBUG - Final streamlined_batches count: {len(streamlined_batches)}")
        return streamlined_batches
        
    except Exception as e:
        print(f"Error getting streamlined action data: {e}")
        return []


def fetch_action_data_from_bakery_system_api(**kwargs):
    """
    Fetch data from Bakery-System API with retry logic for 429 Too Many Requests.
    
    Parameters:
    - **kwargs: Must include `start_date` (str).
    
    Returns:
    - str: JSON string of the response data if successful, or raises an exception otherwise.
    """

    # Load environment variables
    load_dotenv()

    outlet_id = os.getenv("OUTLET_ID")
    bakery_system_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
    bakery_system_api_token = os.getenv("BAKERY_SYSTEM_API_TOKEN", default=None)
    stical_target_api = os.getenv("STICAL_TARGET_API")
    stical_api_token = os.getenv("STICAL_API_TOKEN", default=None)

    start_date = kwargs["start_date"]
    url = f"{bakery_system_base_url}/outlets/{outlet_id}/actions" \
          f"?actionTypes=ADDITION&includeOutletContents=True" \
          f"&offset=0&size=1000&sort=effectiveAt:1&startEffectiveAt={start_date}"

    headers = {"Content-Type": "application/json"}
    if bakery_system_api_token:
        headers["Authorization"] = f"Access-Token {bakery_system_api_token}"
    
    data = retry_request(url=url,headers=headers,method='GET')
    if not isinstance(data, list):
        raise ValueError("Expected a list of items from Bakery-System API")
    return json.dumps(data)
        


def parse_bakery_system_action_data(data):
    flattened_entries = []

    for entry in json.loads(data):
        if entry.get("actionType") == "ADDITION":
            action_id = entry.get("_id")
            ingredient_summary = {}
            ingredient_units = {}
            batch_summary = []

            ingredients = entry.get("actionData", {}).get("ingredients", [])
            lots = entry.get("actionData", {}).get("lots", [])

            for ingredient_entry in ingredients:
                Ingredient = ingredient_entry.get("Ingredient", {})
                ingredient_id = str(Ingredient.get("_id"))  # Convert to string here
                ingredient_name = Ingredient.get("productName")
                inventory_unit = Ingredient.get("inventoryUnit")
                addition_unit = Ingredient.get("additionUnit")
                addition_rate_value = Ingredient.get("additionRateValue")
                addition_rate_unit = Ingredient.get("additionRateUnit")

                ingredient_summary[ingredient_id] = ingredient_name
                ingredient_units[ingredient_id] = addition_unit

                for batch_entry in ingredient_entry.get("batches", []):
                    batch = batch_entry.get("batch", {})
                    batch_id = batch.get("_id")
                    batch_number = batch.get("batchNumber")
                    depleted = batch.get("depleted")

                    flat_record = {
                        "action_id": action_id,
                        "ingredient_id": ingredient_id,
                        "productName": ingredient_name,
                        "inventoryUnit": inventory_unit,
                        "additionUnit": addition_unit,
                        "additionRateValue": addition_rate_value,
                        "additionRateUnit": addition_rate_unit,
                        "batch_id": batch_id,
                        "batchNumber": batch_number,
                        "depleted": depleted
                    }

                    key = f"Ingredient:{ingredient_id}:batch:{batch_id}"
                    batch_summary.append({"key": key, "value": flat_record})
            for lot_entry in lots:
                lot_id = lot_entry.get("_id")
                for vessel_entry in lot_entry.get("vessels",[]):
                    vessel_id = vessel_entry.get("_id")
                    additions = vessel_entry.get("additions", {})
                    bakery_system_lot_id = lot_entry.get("_id")
                    bakery_system_lot_code = lot_entry.get("lotCode", "")
                    bakery_system_lot_stage = lot_entry.get("stage")
                    bakery_system_lot_color = lot_entry.get("color", "")
                    for ingredient_id, change_value in additions.items():
                        key = f"Ingredient:{str(ingredient_id)}:lot:{lot_id}:vessel:{vessel_id}"  # Convert to string here too
                        batch_summary_entries = [b for b in batch_summary if b["key"].startswith(f'Ingredient:{str(ingredient_id)}:batch:')]

                        flat_record = {
                            "action_id": action_id, 
                            "ingredient_id": str(ingredient_id),
                            "ingredient_name": ingredient_summary[str(ingredient_id)],
                            "addition_unit": ingredient_units[str(ingredient_id)],
                            "lot_id": lot_id,
                            "vessel_id": vessel_id ,
                            "change_value": change_value,
                            "bakery_system_lot_id": bakery_system_lot_id,
                            "bakery_system_lot_code": bakery_system_lot_code,
                            "bakery_system_lot_stage": bakery_system_lot_stage,
                            "bakery_system_lot_color": bakery_system_lot_color,
                            "batches": batch_summary_entries
                        }
                        flattened_entries.append({"key": key, "value": flat_record})

    return json.dumps(flattened_entries)
