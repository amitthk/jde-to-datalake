import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
from dotenv import load_dotenv
import logging
import os
import time
from psycopg2 import connect
from psycopg2.extras import execute_values
TIMEOUT = 3600  # 60 minutes in seconds
from psycopg2 import sql
import json
import base64
import hashlib
from urllib.parse import urlparse, parse_qs
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Unit conversion mapping for addition_unit from JDE to Data lake UM
unit_map = {
    'KG': 'kg',
    'EA': 'each',
    'LT': 'L',
    'M2': 'm2',
    'C2': 'c2',
    'PK': 'pack',
    'ST': 'ST',
    'FN': 'FN',
    'GR': 'g',
    'ML': 'mL'
}

# Reverse unit mapping: Data lake to JDE
reverse_unit_map = {v: k for k, v in unit_map.items()}

# Unit conversion mapping for addition_unit from JDE to Data lake UM
rate_unit_map = {
    'KG': 'g/L',
    'EA': 'each/L',
    'LT': 'mL/L',
    'M2': 'm2/L',
    'C2': 'c2/L',
    'PK': 'pack/L'
}

# Reverse unit mapping: Data lake to JDE
reverse_rate_unit_map = {v: k for k, v in rate_unit_map.items()}

def validate_unit(unit_value, field_name="unit"):
    """
    Validate that a unit exists in the unit_map.
    Raises ValueError if unit is not in the mapping.
    
    Parameters:
        unit_value: The unit value to validate
        field_name: Name of the field for error reporting
        
    Returns:
        None: Just validates, doesn't convert
        
    Raises:
        ValueError: If unit is not in unit_map
    """
    if not unit_value:
        return None
        
    # Check if the unit (in uppercase) exists in unit_map
    unit_upper = str(unit_value).upper()
    if unit_upper not in unit_map:
        available_units = list(unit_map.keys())
        raise ValueError(f"Unit '{unit_value}' for {field_name} is not in available mappings. Available units: {available_units}")
    
    # Just validate, don't convert
    return None
# Unit conversion factors (both directions)
conversion_factors = {
    ('KG', 'g'): 1000,
    ('KG', 'L'): 1,
    ('g', 'KG'): 0.001,
    ('L', 'KG'): 1,
    ('L', 'ml'): 1000,
    ('ml', 'L'): 0.001,
    ('EA', 'EA'): 1,
    ('each', 'EA'): 1,
    ('pack', 'PK'): 1,
    ('c2', 'M2'): 1,
    ('m2', 'C2'): 1,
    ('KG', 'kg'): 1,
    ('kg', 'KG'): 1
}

def convert_unit(unit, direction='from_jde'):
    """Convert a unit between Data lake and JDE formats."""
    if direction == 'from_jde':
        return unit_map.get(unit.upper(), unit.lower())
    elif direction == 'to_jde':
        # Try both original case and lowercase to handle mixed case reverse_unit_map
        return reverse_unit_map.get(unit, reverse_unit_map.get(unit.lower(), unit.upper()))

def convert_rate_unit(unit, direction='from_jde'):
    """Convert a unit between Data lake and JDE formats."""
    if direction == 'from_jde':
        return rate_unit_map.get(unit.upper(), unit.lower())
    elif direction == 'to_jde':
        return reverse_rate_unit_map.get(unit.lower(), unit.upper())


def is_jde(unit):
    return unit in unit_map.keys()

def convert_unit_quantity(source_unit, target_unit, quantity):
    normalized_source = source_unit.upper() if is_jde(source_unit) else source_unit.lower()
    normalized_target = target_unit.upper() if is_jde(target_unit) else target_unit.lower()

    if normalized_source == normalized_target:
        return quantity

    multiplier = conversion_factors.get(
        (normalized_source, normalized_target),
        1.0
    )

    try:
        return float(quantity) * multiplier
    except (ValueError, TypeError):
        return None
    


def get_db_connection():
    """Create and return a PostgreSQL connection"""
    PG_DATABASE_URL = os.getenv("PG_DATABASE_URL")
    if not PG_DATABASE_URL:
        raise ValueError("Missing environment variable: PG_DATABASE_URL")
    
    DB_NAME = os.getenv("DB_NAME") or "ingredient_db"
    schema_name = f"{DB_NAME}_schema"
    
    conn = connect(PG_DATABASE_URL)
    
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

def close_db_connection(conn):
    """
    Close a PostgreSQL database connection.

    This function closes the provided database connection.
    
    Parameters:
        conn (psycopg2.connection): The PostgreSQL database connection object.
    """
    
    if conn is not None:
        try:
            # Commit any pending changes to the database
            conn.commit()
        
        except Exception as e:
            print(f"Error committing changes to database: {e}")
        
        finally:
            # Close the database connection
            conn.close()


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
    """
    Create table with given columns supporting both legacy and new formats.
    
    Parameters:
        conn: Database connection
        table_name (str): Name of the table to create
        columns: Either a list of column names (legacy - all TEXT) or 
                dict with column definitions {'col_name': 'TYPE CONSTRAINTS'}
    """
    try:
        with conn.cursor() as cursor:
            if isinstance(columns, list):
                # Legacy format: list of column names (all TEXT type)
                column_definitions = [f'"{col}" TEXT' for col in columns]
            elif isinstance(columns, dict):
                # New format: dictionary with column names and their full definitions
                column_definitions = []
                for col_name, col_definition in columns.items():
                    column_definitions.append(f'"{col_name}" {col_definition}')
            else:
                raise ValueError("Columns must be either a list of names or a dictionary of definitions")
            
            create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(column_definitions)})'
            cursor.execute(create_sql)
            conn.commit()
            print(f"✅ Created table '{table_name}' with {len(column_definitions)} columns")
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

def get_columns_from_df(df):
    """
    Get column names from a DataFrame.

    This function returns a list of column names.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to extract column names from.

    Returns:
        list[str]: A list of column names as strings.
    """
    
    return [col for col in df.columns]



def retry_request_lru(url: str, headers: dict, method: str = 'POST', payload: dict = None, params: dict = None, auth: dict = None):
    """
    Retry HTTP request with support for GET, POST, PUT, and DELETE.
    
    This function also utilizes the LRU cache with a PostgreSQL database backend.
    The key is base64 hashed by URL, params (if not empty), and payload (if not empty).
    Only GET requests are cached; POST and PUT requests go through normally.

    Parameters:
        url (str): Target URL.
        headers (dict): Request headers.
        method (str): HTTP verb (GET, POST, PUT, or DELETE).
        payload (dict): Data to be sent in the request body (used for POST/PUT).
        params (dict): Query parameters (used for GET/DELETE).
        auth (dict): Authentication credentials.

    Returns:
        dict: Response JSON data if success (200/201), else None.
    """
    
    cache_key = _create_cache_key(url, params, payload)
    
    try:
        # Check the LRU cache first for GET requests
        if method == 'GET':
            cached_response = get_from_lru_cache(cache_key)
            if cached_response is not None:
                # Check if cached response is empty list or meaningless
                if cached_response == [] or (isinstance(cached_response, list) and len(cached_response) == 0):
                    logging.warning(f"[CACHE HIT] Retrieved empty response from cache, cleaning up and making fresh request")
                    delete_from_lru_cache(cache_key)
                    # Don't return cached empty response, make fresh request instead
                else:
                    print(f"[CACHE HIT] Retrieved response from cache: {json.dumps(cached_response)}")
                    if isinstance(cached_response, str):
                        try:
                            return json.loads(cached_response)
                        except json.JSONDecodeError:
                            logging.error(f"Invalid JSON in cache, removing cache entry")
                            delete_from_lru_cache(cache_key)
                            # Continue to make fresh request
                    else:
                        return cached_response

        # If not cached or cache was empty/invalid, proceed with the normal retry logic
        result = retry_request(url, headers, method=method, payload=payload, params=params, auth=auth)
        
        # Only cache non-empty, meaningful responses for GET requests
        if result is not None and method == 'GET':
            # Don't cache empty lists or meaningless responses
            if not (result == [] or (isinstance(result, list) and len(result) == 0)):
                set_in_lru_cache(cache_key, result)  # Save to cache (normalizes list responses)
            else:
                logging.warning(f"[EMPTY RESPONSE] Not caching empty response from {url}")

        return result
    
    except Exception as e:
        logging.error(f"Error during request: {e}")
        return None


def retry_request(url: str, headers: dict, method: str = 'GET', payload: dict = None, params: dict = None, auth: dict = None):
    """
    Retry HTTP request with support for GET, POST, PUT, and DELETE.

    Parameters:
        url (str): Target URL.
        headers (dict): Request headers.
        method (str): HTTP verb (GET, POST, PUT, or DELETE).
        payload (dict): Data to be sent in the request body (used for POST/PUT).
        params (dict): Query parameters (used for GET/DELETE).
        auth (dict): Authentication credentials.

    Returns:
        dict: Response JSON data if success (200/201), else None.
    """
    try:
        # Determine the correct HTTP method and construct the request
        if method == 'GET':
            response = requests.get(url=url, headers=headers, params=params, auth=auth, verify=False)
        elif method == 'POST':
            response = requests.post(url=url, headers=headers, json=payload, auth=auth, verify=False)
        elif method == 'PUT':
            response = requests.put(url=url, headers=headers, json=payload, auth=auth, verify=False)
        elif method == 'DELETE':
            response = requests.delete(url=url, headers=headers, params=params, auth=auth, verify=False)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        # Check for success status codes (200 or 201)
        if response.status_code in [200, 201]:
            try:
                data_json = response.json()
                
                # Check if response is empty list or contains no meaningful data
                if data_json == [] or (isinstance(data_json, list) and len(data_json) == 0):
                    logging.warning(f"[EMPTY RESPONSE] Received empty response from {url}")
                    return None
                
                print(f"[SUCCESS] Request successful. Response: {json.dumps(data_json)}")
                return data_json
                
            except json.JSONDecodeError:
                logging.error(f"[JSON ERROR] Failed to parse response as JSON: {response.text}")
                return None

        elif response.status_code in [429, 423]:
            try:
                parsed_response = json.loads(response.text)
                wait_seconds = parsed_response.get("metadata", {}).get("wait", 10)  # Default to 60 if not specified

                if wait_seconds > 0:
                    logging.warning(f"[RATE LIMIT] Retrying in {wait_seconds} seconds.")
                    time.sleep(wait_seconds)
                else:
                    logging.warning("[RATE LIMIT] No wait time specified, defaulting to 10-second retry interval.")
                    time.sleep(10)

            except (json.JSONDecodeError, KeyError) as e:
                logging.error(f"Error parsing rate limit response: {e}")
                logging.warning("[RATE LIMIT] Defaulting to 10-second retry interval.")
                time.sleep(10)

            # Retry the request using the same parameters
            return retry_request(url, headers, method=method, payload=payload, params=params, auth=auth)

        else:
            error_message = f"Request failed with status code {response.status_code}: {response.text}"
            logging.error(f"[FAILED] {error_message}")
            
            # For 400 errors (Bad Request), raise an exception with the API error details
            # This is especially important for duplicate errors and other validation issues
            if response.status_code == 400:
                try:
                    error_details = response.json()
                    if isinstance(error_details, dict) and 'msg' in error_details:
                        raise Exception(f"API Error (400): {error_details['msg']}")
                    else:
                        raise Exception(f"API Error (400): {response.text}")
                except json.JSONDecodeError:
                    raise Exception(f"API Error (400): {response.text}")
            
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error during request: {e}")
        return None


def _create_cache_key(url: str, params: dict = None, payload: dict = None):
    """
    Create a unique cache key based on URL, params, and payload with consistent serialization.
    
    This function ensures deterministic cache key generation by:
    1. Normalizing URL components
    2. Sorting dictionary keys consistently
    3. Using a hash for length consistency
    4. Handling edge cases properly
    
    Parameters:
        url (str): Target URL.
        params (dict): Query parameters (used for GET/DELETE).
        payload (dict): Data to be sent in the request body (used for POST/PUT).

    Returns:
        str: The unique cache key as a consistent hash string.
    """
    
    def normalize_dict(d):
        """Recursively normalize a dictionary for consistent serialization."""
        if d is None:
            return None
        if isinstance(d, dict):
            # Sort keys and recursively normalize values
            return {k: normalize_dict(v) for k, v in sorted(d.items())}
        elif isinstance(d, list):
            # Normalize list elements
            return [normalize_dict(item) for item in d]
        else:
            # Convert to string to handle type inconsistencies
            return str(d) if d is not None else None
    
    # Parse and normalize the URL
    parsed_url = urlparse(url)
    
    # Extract existing query parameters from URL
    url_params = parse_qs(parsed_url.query, keep_blank_values=True)
    
    # Merge URL params with provided params
    all_params = {}
    if url_params:
        # Flatten single-item lists from parse_qs
        for k, v in url_params.items():
            all_params[k] = v[0] if len(v) == 1 else v
    
    if params:
        all_params.update(params)
    
    # Normalize the components
    normalized_params = normalize_dict(all_params) if all_params else {}
    normalized_payload = normalize_dict(payload) if payload else {}
    
    # Create base URL without query parameters
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
    
    # Create deterministic JSON strings with sorted keys
    params_json = json.dumps(normalized_params, sort_keys=True, separators=(',', ':'))
    payload_json = json.dumps(normalized_payload, sort_keys=True, separators=(',', ':'))
    
    # Construct the cache key components
    cache_key_data = f"{base_url}||{params_json}||{payload_json}"
    
    # Use SHA-256 hash for consistent length and uniqueness
    cache_key_hash = hashlib.sha256(cache_key_data.encode('utf-8')).hexdigest()
    
    return cache_key_hash


def get_from_lru_cache(cache_key: str):
    """
    Retrieve a response from the LRU cache.
    
    Parameters:
        cache_key (str): The unique cache key as base64 string
        
    Returns:
        dict/list/None: The cached response if found and valid, None otherwise
    """
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cursor:            
            # Query for cached response (assuming 1 hour cache validity)
            query_sql = """
                SELECT response FROM app_requests_lru_cache 
                WHERE cache_key = %s AND timestamp > NOW() - INTERVAL '3600 seconds'
            """
            cursor.execute(query_sql, (cache_key,))
            
            result_row = cursor.fetchone()
            
            if result_row:
                # result_row is a tuple, get the first element (response)
                response_json = result_row[0]
                try:
                    parsed_response = json.loads(response_json)
                    # Check if cached response is empty and clean it up
                    if parsed_response == [] or (isinstance(parsed_response, list) and len(parsed_response) == 0):
                        logging.warning(f"Found empty cached response, cleaning up cache entry")
                        delete_from_lru_cache(cache_key)
                        return None
                    return parsed_response
                except json.JSONDecodeError as e:
                    logging.error(f"Error parsing cached JSON: {e}")
                    # Clean up invalid cache entry
                    delete_from_lru_cache(cache_key)
                    return None
            else:
                return None

    except Exception as e:
        logging.error(f"Error during LRU cache retrieval: {e}")
        return None

    finally:
        close_db_connection(conn)


def delete_from_lru_cache(cache_key: str):
    """
    Delete a response from the LRU cache.
    
    Parameters:
        cache_key (str): The unique cache key as base64 string
        
    Returns:
        bool: True if deletion was successful, False otherwise
    """
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cursor:            
            # Delete cached response
            query_sql = """
                DELETE FROM app_requests_lru_cache 
                WHERE cache_key = %s
            """
            cursor.execute(query_sql, (cache_key,))
            conn.commit()
            
            rows_affected = cursor.rowcount
            if rows_affected > 0:
                logging.info(f"Deleted {rows_affected} cache entries for key {cache_key}")
                return True
            else:
                logging.info(f"No cache entries found for key {cache_key}")
                return False

    except Exception as e:
        logging.error(f"Error during LRU cache deletion: {e}")
        return False

    finally:
        close_db_connection(conn)


def cleanup_empty_cache_entries():
    """
    Clean up empty cache entries from the LRU cache database.
    This removes entries where response is empty list '[]', empty string, or null.
    """
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cursor:
            # Delete empty cache entries
            query_sql = """
                DELETE FROM app_requests_lru_cache 
                WHERE response = '[]' OR response = '' OR response IS NULL
            """
            cursor.execute(query_sql)
            conn.commit()
            
            rows_affected = cursor.rowcount
            if rows_affected > 0:
                logging.info(f"Cleaned up {rows_affected} empty cache entries")
            else:
                logging.info("No empty cache entries found")

    except Exception as e:
        logging.error(f"Error cleaning up empty cache entries: {e}")

    finally:
        close_db_connection(conn)


def invalidate_lru_cache(url: str, headers: dict, method: str = 'POST', payload: dict = None, params: dict = None, auth: dict = None):
    """
    Invalidate LRU cache entry for a specific request.
    
    Parameters:
        url (str): Target URL.
        headers (dict): Request headers.
        method (str): HTTP verb (GET, POST, PUT, or DELETE).
        payload (dict): Data to be sent in the request body (used for POST/PUT).
        params (dict): Query parameters (used for GET/DELETE).
        auth (dict): Authentication credentials.
    """
    
    cache_key = _create_cache_key(url, params, payload)
    
    try:
        success = delete_from_lru_cache(cache_key=cache_key)
        if success:
            print(f'Cleared cache key {cache_key} for {url}')
        else:
            print(f'No cache entry found for {url}')
    except Exception as e:
        logging.error(f"Error during cache invalidation: {e}")
        return None


def safe_get_from_response(response_data, index=0):
    """
    Safely get data from response to avoid 'list index out of range' errors.
    
    Parameters:
        response_data: The response data (could be list, dict, or other)
        index: Index to access if response_data is a list
    
    Returns:
        The requested data or None if not available
    """
    if response_data is None:
        return None
    
    if isinstance(response_data, list):
        if len(response_data) > index:
            return response_data[index]
        else:
            logging.warning(f"Response list has {len(response_data)} items, cannot access index {index}")
            return None
    
    return response_data

def create_lru_cache_db():
    """
    Creates LRU cache db.
    
    Returns:
        None: 
    """
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cursor:
            # Create table if it doesn't exist
            table_sql = """
            CREATE TABLE IF NOT EXISTS app_requests_lru_cache (
                id SERIAL PRIMARY KEY,
                cache_key VARCHAR(255) UNIQUE NOT NULL,
                response TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(table_sql)
            conn.commit()
            print(f"app_requests_lru_cache table created.")
            return None

    except Exception as e:
        print(f"Error during LRU cache retrieval: {e}")
        return None

    finally:
        close_db_connection(conn)

def set_in_lru_cache(cache_key: str, response: dict):
    """
    Store a response in the LRU cache with list handling and conflict resolution.

    If the response is a single-element list, it will be saved as that element.
    Otherwise, the full response is stored. On duplicate cache keys, insertion is skipped.

    Parameters:
        cache_key (str): The unique cache key as base64 string.
        response: The JSON response from an HTTP request.
    """
    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:
            # Ensure table exists
            table_sql = """
            CREATE TABLE IF NOT EXISTS app_requests_lru_cache (
                id SERIAL PRIMARY KEY,
                cache_key VARCHAR(500) UNIQUE NOT NULL,
                response TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(table_sql)

            # Normalize the response: single-element list → just that item
            if isinstance(response, list) and len(response) == 1:
                normalized_response = response[0]
            else:
                normalized_response = response

            # Convert to JSON string for storage
            json_response = json.dumps(normalized_response)

            # Insert into DB with ON CONFLICT DO NOTHING
            insert_sql = """
                INSERT INTO app_requests_lru_cache
                (cache_key, response, timestamp)
                VALUES (%s, %s, NOW())
                ON CONFLICT (cache_key) DO NOTHING;
            """
            cursor.execute(insert_sql, (cache_key, json_response))
            conn.commit()

    except Exception as e:
        print(f"Error during LRU cache insertion: {e}")
        conn.rollback()

    finally:
        close_db_connection(conn)


def normalize_quantity_for_transaction_id(quantity_value):
    """
    Normalize quantity value for unique transaction ID generation.
    Preserves up to 9 decimal places and removes trailing zeros.
    
    Args:
        quantity_value: The quantity value (can be str, int, float, or Decimal)
        
    Returns:
        str: Normalized quantity string with up to 9 decimal places, trailing zeros removed
    """
    from decimal import Decimal
    
    try:
        # Convert to Decimal for precise handling
        decimal_value = Decimal(str(quantity_value))
        
        # Format to 9 decimal places, then strip trailing zeros and decimal point if needed
        normalized = f"{float(decimal_value):.9f}".rstrip('0').rstrip('.')
        
        return normalized
    except (ValueError, TypeError, Exception) as e:
        # Fallback for invalid values
        print(f"Warning: Could not normalize quantity '{quantity_value}': {e}")
        return str(quantity_value)


def preserve_quantity_precision(quantity_value, max_decimals=9):
    """
    Preserve the exact decimal precision of a quantity value up to max_decimals.
    Used for passing quantities through JDE <-> Bakery-System without losing precision.
    
    Args:
        quantity_value: The quantity value (can be str, int, float, or Decimal)
        max_decimals (int): Maximum number of decimal places to preserve (default 9)
        
    Returns:
        float: The quantity value with preserved precision
    """
    from decimal import Decimal, ROUND_HALF_UP
    
    try:
        # Convert to Decimal for precise handling
        decimal_value = Decimal(str(quantity_value))
        
        # Create quantizer for the specified decimal places
        quantizer = Decimal('0.' + '0' * max_decimals)
        
        # Round to max_decimals and convert back to float
        rounded_value = decimal_value.quantize(quantizer, rounding=ROUND_HALF_UP)
        
        return float(rounded_value)
    except (ValueError, TypeError, Exception) as e:
        # Fallback for invalid values
        print(f"Warning: Could not preserve precision for quantity '{quantity_value}': {e}")
        try:
            return float(quantity_value)
        except:
            return 0.0
