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

def verify_ingredients_submmited_status_db():
    load_dotenv()
    # Step 1: Read environment variable for connection string
    PG_DATABASE_URL = os.environ.get("PG_DATABASE_URL")
    if not PG_DATABASE_URL:
        raise ValueError("Environment variable 'PG_DATABASE_URL' is required")

    parsed = urlparse(PG_DATABASE_URL)
    host = parsed.hostname
    port = int(parsed.port) if parsed.port else 5432  # Default PostgreSQL port
    user = parsed.username
    password = parsed.password

    try:
        # Step 4: Connect to the newly created database
        new_conn = connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname="ingredients_db"
        )

        # Step 5: Create table and insert sample data
        with new_conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE ingredient_submitted_status (
                "key" SERIAL PRIMARY KEY,
                action_id INTEGER,
                ingredient_id INTEGER,
                ingredient_name VARCHAR(255),
                addition_unit VARCHAR(255),
                lot_id VARCHAR(255),
                vessel_id VARCHAR(128),
                vessel_code VARCHAR(255),
                change_value VARCHAR(32),
                batches VARCHAR(255),
                status_text VARCHAR(700), 
                status_code INTEGER,
                status VARCHAR(32),
                unique_transaction_id VARCHAR(255) );
            """)
            cur.execute("""
            ALTER TABLE ingredient_submitted_status
                ADD CONSTRAINT unique_ingredient_submission
                UNIQUE (unique_transaction_id);
            """)
            cur.execute("""
                INSERT INTO ingredient_submitted_status (action_id,ingredient_id,ingredient_name,addition_unit,lot_id,vessel_id,vessel_code,change_value,batches,status_code,status,unique_transaction_id)
                VALUES 
                    ('1234', '3456','test','kg','testlot','testvessel','TESTVESSEL','1.0','abcd','201','successful','test_testlot_TESTVESSEL')
            """)
            new_conn.commit()
            print("Inserted sample data")

        # Step 6: Fetch and display results in plain English
        with new_conn.cursor() as cur:
            cur.execute("SELECT * FROM ingredient_submitted_status")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

            print("\nIngredient Todo List:")
            for row in rows:
                print(f"Row: {row}")

    except Exception as e:
        print(f" Error during database operations: {e}")
    finally:
        if new_conn:
            new_conn.close()



def fetch_from_bakery_system_api(**kwargs):
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
          f"&offset=0&size=100&sort=effectiveAt:1&startEffectiveAt={start_date}"

    headers = {"Content-Type": "application/json"}
    if bakery_system_api_token:
        headers["Authorization"] = f"Access-Token {bakery_system_api_token}"
    
    data = retry_request(url=url,headers=headers,method='GET')
    if not isinstance(data, list):
        raise ValueError("Expected a list of items from Bakery-System API")
    return json.dumps(data)
        


def parse_bakery_system_json(data):
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
                lot_number = lot_entry.get("lotNumber", "")
                for vessel_entry in lot_entry.get("vessels",[]):
                    vessel_id = vessel_entry.get("_id")
                    vessel_code = vessel_entry.get("vesselCode", "")
                    additions = vessel_entry.get("additions", {})

                    for ingredient_id, change_value in additions.items():
                        # Skip zero or null quantities
                        if change_value is None or change_value == 0 or (isinstance(change_value, str) and change_value.strip() in ['0', '', '0.0']):
                            continue
                        
                        key = f"Ingredient:{str(ingredient_id)}:lot:{lot_id}:vessel:{vessel_id}"  # Convert to string here too
                        batch_summary_entries = [b for b in batch_summary if b["key"].startswith(f'Ingredient:{str(ingredient_id)}:batch:')]
                        
                        # Generate unique transaction ID using ProductName_LotNumber_VesselCode_Quantity format
                        import sys
                        import os
                        sys.path.append(os.path.dirname(os.path.dirname(__file__)))
                        from utility import normalize_quantity_for_transaction_id, preserve_quantity_precision
                        normalized_quantity = normalize_quantity_for_transaction_id(change_value)
                        unique_transaction_id = f"{ingredient_summary[str(ingredient_id)]}_{lot_number}_{vessel_code}_{normalized_quantity}"

                        flat_record = {
                            "action_id": action_id, 
                            "ingredient_id": str(ingredient_id),
                            "ingredient_name": ingredient_summary[str(ingredient_id)],
                            "addition_unit": ingredient_units[str(ingredient_id)],
                            "lot_id": lot_id,
                            "lot_number": lot_number,
                            "vessel_id": vessel_id,
                            "vessel_code": vessel_code,
                            "change_value": preserve_quantity_precision(change_value),
                            "batches": batch_summary_entries,
                            "unique_transaction_id": unique_transaction_id
                        }
                        flattened_entries.append({"key": key, "value": flat_record})

    return json.dumps(flattened_entries)

def post_backup_data_to_stical_api(data):

    load_dotenv()  # loads variables from .env into environment variables

    outlet_id = os.getenv("OUTLET_ID")
    bakery_system_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
    bakery_system_api_token = os.getenv("BAKERY_SYSTEM_API_TOKEN", default=None)
    stical_target_api = os.getenv("STICAL_TARGET_API")
    stical_api_token = os.getenv("STICAL_API_TOKEN", default=None)

    url = stical_target_api + '/bakeryopsnb_'
    token = stical_api_token

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    items = json.loads(data)
    for idx, item in enumerate(items):
        try:
            item_json={"value": json.dumps(item)}
            json_string = json.dumps(item_json)  
            print(json_string)
            url=url.split("_")[0]+"_"+str(idx)
            response = requests.post(url, headers=headers, json=item_json)
            print(f"[{idx}] Posted to stical API. Status: {response.status_code}")
            #time.sleep(0.1)
        except Exception as e:
            print(f"[Item {idx} ] Error posting to stical API: {e}")

def check_ingredient_status(ingredient_id, conn):
    """Check if the Ingredient is already marked as 'done' in DB"""
    cur = conn.cursor()
    cur.execute("""
        SELECT status FROM ingredient_submitted_status 
        WHERE action_id = %s AND ingredient_id = %s;
    """, (ingredient_id, ingredient_id))
    result = cur.fetchone()
    return result[0] if result else None


def insert_into_table(conn, data):
    """Insert all captured values from parse_bakery_system_json into the table."""
    with conn.cursor() as cur:
        # Use execute_values for bulk insertion
        try:
            columns = [f'"{col}"' for col in data[0].keys()]
            cols_str = ", ".join(columns)
            insert_sql = f"INSERT INTO ingredient_submitted_status ({cols_str}) VALUES %s"

            values = []
            for item in data:
                # Handle JSON and NULL values
                formatted_item = {k: json.dumps(v) if isinstance(v, (dict, list)) or v is None else v for k, v in item.items()}
                values.append(tuple(formatted_item.values()))

            from psycopg2 import execute_values
            execute_values(cur, insert_sql, values)
            conn.commit()
        except Exception as e:
            logging.error(f"Error inserting data into table: {e}")
            conn.rollback()


def post_data_to_jde(data):
    """Dispatch Ingredient consumptions to JDE using unique transaction ID"""

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
            # Extract values from item
            action_id = item['value']['action_id']
            ingredient_id = item['value']['ingredient_id']
            unique_transaction_id = item['value']['unique_transaction_id']
            
            resp_arr.append({"INFO": f"==========> Processing: {json.dumps(item)} <=========="})
            cur = conn.cursor()
            
            # Check if the unique transaction ID is already processed
            cur.execute("""
                SELECT status FROM ingredient_submitted_status 
                WHERE unique_transaction_id = %s;
            """, (unique_transaction_id,))

            result = cur.fetchone()

            if result and result[0] == 'done':
                # Already processed, skip
                resp_arr.append({"ERROR": f"==========> Unique transaction {unique_transaction_id} already marked as done. Skipping... <=========="})
                continue

            # Convert addition_unit using unit conversion map
            original_unit = item["value"]["addition_unit"] if "addition_unit" in item["value"] else ""
            converted_unit = convert_unit(item["value"]["addition_unit"], direction='to_jde') if "addition_unit" in item["value"] else ""
            original_quantity = item["value"]["change_value"] if "change_value" in item["value"] else 0.0
            converted_quantity = convert_unit_quantity(item["value"]["addition_unit"], converted_unit, item["value"]["change_value"]) if "change_value" in item["value"] else 0.0

            # Extract quantity and UOM from actionData
            change_value = original_quantity
            uom = original_unit
            productName = item["value"]["ingredient_name"] if "ingredient_name" in item["value"] else ""
            lot_number = item["value"].get("lot_number", "")
            vessel_code = item["value"].get("vessel_code", "")
            
            if change_value == 0.0 or uom == "" or productName == "":
                resp_arr.append({"ERROR": f"==========> One of the required fields for {productName} was empty. Skipping. <=========="})
                continue

            bu = "1110"
            bu_map = {
                "B_": "1110",
                "P_": "1130",
                "M_": "1120"
            }

            if productName.startswith(tuple(bu_map.keys())):
                bu = bu_map[productName[:2]]

            # Create sample payload for JDE
            sample_payload_for_jde = {
                "Branch_Plant": bu,
                "Document_Type": "II",
                "Explanation": f"BAKERYOPS. DEPL: {ingredient_id}:{action_id}",
                "Select_Row": "1",
                "GridData": [
                    {
                        "Item_Number": productName,  # Replace with actual item number
                        "Quantity": f"{str(change_value)}"
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
                # Mark as "done" in DB after successful post using unique transaction ID
                cur.execute("""
                    INSERT INTO ingredient_submitted_status 
                    (action_id, ingredient_id, lot_id, vessel_id, vessel_code, ingredient_name, addition_unit, status_text, status, unique_transaction_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'done', %s)
                    ON CONFLICT (unique_transaction_id) DO UPDATE
                    SET status_text = EXCLUDED.status_text, 
                    status = 'done';
                """, (
                    action_id,
                    ingredient_id,
                    item["value"].get("lot_id", ""),
                    item["value"].get("vessel_id", ""),
                    vessel_code,
                    productName,
                    uom,
                    status_text,
                    unique_transaction_id
                ))
                conn.commit()
                resp_arr.append({"info": f"=========> Posted: {json.dumps(sample_payload_for_jde)} <=========="})
                resp_arr.append(response.json())  # Use append instead of push and use response.json() directly
            else:
                cur.execute("""
                    INSERT INTO ingredient_submitted_status 
                    (action_id, ingredient_id, lot_id, vessel_id, vessel_code, ingredient_name, addition_unit, status_text, status, unique_transaction_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'error', %s)
                    ON CONFLICT (unique_transaction_id) DO UPDATE
                    SET status_text = EXCLUDED.status_text, 
                    status = 'error';
                """, (
                    action_id,
                    ingredient_id,
                    item["value"].get("lot_id", ""),
                    item["value"].get("vessel_id", ""),
                    vessel_code,
                    productName,
                    uom,
                    status_text,
                    unique_transaction_id
                ))
                conn.commit()
                resp_arr.append({"ERROR": f"==========> Error processing {productName} :  {response} <=========="})
                resp_arr.append({"api_ERROR" : f"Error Sending data to endpoint : {url}"})
    finally:
        if conn:
            conn.close()

    return resp_arr

from airflow import DAG
from airflow.exceptions import AirflowSkipException
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
    'bakery_system_depletions_to_jde_unique_transaction_id',
    default_args=default_args,
    description='Pipeline to fetch and post Bakery-System data to STICAL and JDE with unique transaction ID tracking',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
)

def get_start_date():
    today = datetime.utcnow()
    yesterday = today + timedelta(days=-1)
    return yesterday.strftime('%Y-%m-%dT%H:%M:%S.000Z')


def fetch_bakery_system_task(**kwargs):
    start_date = get_start_date()
    data = []
    try:
        data = fetch_from_bakery_system_api(start_date=start_date)
    except Exception as e:
        raise AirflowSkipException(f"No data fetched from Bakery-System API: {e}")
    
    if not isinstance(data, list) or not data:
        raise AirflowSkipException("No data fetched from Bakery-System API")
    return data


def parse_bakery_system_data_task(**kwargs):
    data = kwargs["ti"].xcom_pull(task_ids="fetch_bakery-system")
    if not data:
        raise AirflowSkipException("No data to parse from Bakery-System API, skipping downstream tasks")
    parsed_data = parse_bakery_system_json(data)
    return parsed_data


def post_backup_to_stical_task(**kwargs):
    data = kwargs["ti"].xcom_pull(task_ids="parse_bakery_system_data")
    post_backup_data_to_stical_api(data)


def post_to_jde_task(**kwargs):
    data = kwargs["ti"].xcom_pull(task_ids="parse_bakery_system_data")
    response = post_data_to_jde(data)
    return response


fetch_bakery_system = PythonOperator(
    task_id='fetch_bakery-system',
    dag=dag,
    python_callable=fetch_bakery_system_task
)

parse_bakery_system_data = PythonOperator(
    task_id='parse_bakery_system_data',
    dag=dag,
    python_callable=parse_bakery_system_data_task
)

post_backup_to_stical = PythonOperator(
    task_id='post_backup_to_stical',
    dag=dag,
    python_callable=post_backup_to_stical_task
)

post_to_jde = PythonOperator(
    task_id='post_to_jde',
    dag=dag,
    python_callable=post_to_jde_task
)

# Set the pipeline order
fetch_bakery-system >> parse_bakery_system_data >> post_backup_to_stical >> post_to_jde
