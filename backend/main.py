from fastapi import FastAPI, HTTPException, Depends, Body, Request
import pandas as pd
import psycopg2
from psycopg2 import sql
import os
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import numpy as np
from datetime import datetime, timedelta
import traceback
from fastapi.responses import JSONResponse
import requests

# Load environment variables BEFORE importing modules that need them
load_dotenv()

from jde_helper import get_latest_jde_cardex, submit_ingredient_batch_action, get_jde_item_master, fetch_or_create_ingredient_from_item_master
from bakery_ops_helper import get_data_from_bakery_operations, create_product_in_bakery_operations, dispatch_to_bakery_operations
from auth import AuthMiddleware, get_token, TokenRequest, TokenData
from s3_helper import s3_helper
from schema_manager import schema_manager

# Helper to convert numpy types to native Python types
def convert_numpy_types(obj):
    import numpy as np
    if isinstance(obj, np.generic):
        return obj.item()
    elif isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(v) for v in obj]
    else:
        return obj

origins = [
    "http://localhost:3000",        # Development frontend
    "http://localhost:9999",        # Production frontend (localhost)
    "http://127.0.0.1:9999",       # Production frontend (127.0.0.1)
    "http://sticalws01.stical.com.au:9999",        # Production frontend (server hostname)
]

# In production, allow all origins for now to debug CORS issues
# You should restrict this in actual production for security
if os.getenv("ENVIRONMENT") == "production":
    origins = ["*"]  # Allow all origins in production

app = FastAPI()

# Add CORS middleware FIRST - this must come before authentication middleware
# to handle preflight requests properly
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,  # Allow cookies, authorization headers, etc.
    allow_methods=["*"],     # Allow all HTTP methods (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],     # Allow all headers in the request
)

# Add authentication middleware AFTER CORS
app.add_middleware(AuthMiddleware)

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    print("Exception:", exc)
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "traceback": traceback.format_exc()},
    )

# Authentication endpoint
@app.post("/token", response_model=TokenData)
async def login(request: TokenRequest):
    return await get_token(request)

# Health check endpoint (no authentication required)
@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "Backend is running"}

# ------------------------
# 1. Connect to PostgreSQL
# ------------------------

def get_db_connection():
    """Create and return a PostgreSQL connection"""
    load_dotenv()
    PG_DATABASE_URL = os.getenv("PG_DATABASE_URL")
    if not PG_DATABASE_URL:
        raise ValueError("Missing environment variable: PG_DATABASE_URL")

    DB_NAME = os.getenv("DB_NAME") or "inventory_backup_db"
    schema_name = f"{DB_NAME}_schema"

    conn = psycopg2.connect(PG_DATABASE_URL)

    try:
        with conn.cursor() as cursor:
            # Create schema if it doesn't exist
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
            # Set search path to the schema
            cursor.execute(f'SET search_path TO "{schema_name}"')
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise

    return conn

# ------------------------
# 2. Read Data from PostgreSQL
# ------------------------

def read_table(conn, table_name):
    """Read data from a PostgreSQL table into pandas DataFrame"""
    schema_name = os.getenv("DB_NAME") or "inventory_backup_db"
    schema_name = f"{schema_name}_schema"
    query = f'SELECT * FROM "{schema_name}"."{table_name}"'
    
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data from '{table_name}': {e}")

def expand_json_columns(df, columns):
    """Expand JSON fields into separate columns with original column as prefix"""
    for col in columns:
        if col not in df.columns or df[col].dtype != object:
            continue

        def parse_json(row):
            try:
                parsed = json.loads(row) if pd.notnull(row) else {}
                return parsed
            except (json.JSONDecodeError, TypeError):
                return {}

        # Parse the JSON strings into dictionaries
        parsed_series = df[col].apply(parse_json)

        # Create a DataFrame with prefixed column names
        expanded_df = pd.json_normalize(parsed_series).add_prefix(f"{col}_")

        # Combine the expanded columns with the original DataFrame
        df = pd.concat([df.drop(columns=[col]), expanded_df], axis=1)

    return df


# Convert DataFrame to dictionary with proper JSON handling
def safe_convert_value(value):
    """Convert value to JSON-safe format"""
    if pd.isnull(value) or value is None:
        return None
    elif isinstance(value, (np.integer, np.floating)):
        if np.isnan(value) or np.isinf(value):
            return None
        return value.item()  # Convert numpy types to Python types
    elif isinstance(value, dict):
        # Clean nested dictionaries
        return {k: safe_convert_value(v) for k, v in value.items()}
    else:
        return value

def to_dict_safe(df):
    """Convert DataFrame to list of dictionaries with JSON-safe values"""
    result = []
    for _, row in df.iterrows():
        row_dict = {}
        for col, value in row.items():
            row_dict[col] = safe_convert_value(value)
        result.append(row_dict)
    return result



# ------------------------
# 3. Main Logic: Process and Display Data
# ------------------------

@app.get("/data/joined_df")
async def get_joined_df():
    try:
        # Connect to the database
        conn = get_db_connection()

        # Read tables into DataFrames
        df_f4101 = read_table(conn, "F4101")
        df_f41021 = read_table(conn, "F41021")

        # Clean numeric columns
        for df in [df_f4101, df_f41021]:
            if "Short Item No" in df.columns:
                df["Short Item No"] = pd.to_numeric(df["Short Item No"], errors="coerce")
        
        # Drop invalid rows
        for df in [df_f4101, df_f41021]:
            df.dropna(subset=["Short Item No"], inplace=True)

        # Join F4101 and F41021
        joined_df = pd.merge(
            df_f4101,
            df_f41021,
            how="inner",
            left_on="Short Item No",
            right_on="Short Item No"
        )

        # Drop duplicate columns
        if "Short Item No_x" in joined_df.columns:
            joined_df = joined_df.drop(columns=["Short Item No_x"])
        
        if "Short Item No_y" in joined_df.columns:
            joined_df = joined_df.drop(columns=["Short Item No_y"])

        return {"data": joined_df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/data/df_bakery_ops_expanded")
async def get_df_bakery_ops_expanded():
    try:
        # Connect to the database
        conn = get_db_connection()

        # Read tables into DataFrames
        df_bakery_ops = read_table(conn, "bakery_ops_products")

        if "product_id" in df_bakery_ops.columns:
            df_bakery_ops["product_id"] = pd.to_numeric(df_bakery_ops["product_id"], errors="coerce")

        # Drop invalid rows
        df_bakery_ops.dropna(subset=["product_id"], inplace=True)

        # Expand JSON columns in bakery operations data
        df_bakery_ops_expanded = expand_json_columns(df_bakery_ops, ["configuration", "tags"])

        return {"data": to_dict_safe(df_bakery_ops_expanded)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/data/joined_df2")
async def get_joined_df2():
    try:
        # Connect to the database
        conn = get_db_connection()

        # Read tables into DataFrames
        df_f4101 = read_table(conn, "F4101")
        df_f41021 = read_table(conn, "F41021")
        df_bakery_system = read_table(conn, "bakery_system_dry_goods_inventory")

        # Clean numeric columns
        for df in [df_f4101, df_f41021]:
            if "Short Item No" in df.columns:
                df["Short Item No"] = pd.to_numeric(df["Short Item No"], errors="coerce")
        
        if "_id" in df_bakery_system.columns:
            df_bakery_system["_id"] = pd.to_numeric(df_bakery_system["_id"], errors="coerce")

        # Drop invalid rows
        for df in [df_f4101, df_f41021]:
            df.dropna(subset=["Short Item No"], inplace=True)
        
        df_bakery_system.dropna(subset=["_id"], inplace=True)

        # Join F4101 and F41021
        joined_df = pd.merge(
            df_f4101,
            df_f41021,
            how="inner",
            left_on="Short Item No",
            right_on="Short Item No"
        )

        # Drop duplicate columns
        if "Short Item No_x" in joined_df.columns:
            joined_df = joined_df.drop(columns=["Short Item No_x"])
        
        if "Short Item No_y" in joined_df.columns:
            joined_df = joined_df.drop(columns=["Short Item No_y"])

        # Expand JSON columns in bakery-system data
        df_bakeryops_expanded = expand_json_columns(df_bakery_system, ["onHand", "categoryFields"])

        # Clean up column names
        if "Description " in joined_df.columns:
            joined_df = joined_df.rename(columns={"Description ": "Description"})
        
        if "Description" in joined_df.columns:
            joined_df = joined_df.rename(columns={"Description": "description"})
        
        if "name" in df_bakeryops_expanded.columns:
            df_bakeryops_expanded = df_bakeryops_expanded.rename(columns={"name": "description"})

        # Merge DataFrames
        joined_df2 = pd.merge(
            joined_df,
            df_bakeryops_expanded,
            how="outer",
            left_on="description",
            right_on="description"
        )

        # Clean the DataFrame for JSON serialization
        # First replace infinite values with NaN
        joined_df2.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # Then fill NaN values with None (which serializes to null in JSON)
        joined_df2 = joined_df2.where(pd.notnull(joined_df2), None)

        # Convert DataFrame to JSON-safe format
        return {"data": to_dict_safe(joined_df2)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            conn.close()

@app.get("/data/pivot_report")
async def get_pivot_report():
    try:
        # Connect to the database
        conn = get_db_connection()

        # Read tables into DataFrames
        df_f4101 = read_table(conn, "F4101")
        df_f41021 = read_table(conn, "F41021")
        df_bakery_system = read_table(conn, "bakery_system_dry_goods_inventory")

        # Clean numeric columns
        for df in [df_f4101, df_f41021]:
            if "Short Item No" in df.columns:
                df["Short Item No"] = pd.to_numeric(df["Short Item No"], errors="coerce")
        
        if "_id" in df_bakery_system.columns:
            df_bakery_system["_id"] = pd.to_numeric(df_bakery_system["_id"], errors="coerce")

        # Drop invalid rows
        for df in [df_f4101, df_f41021]:
            df.dropna(subset=["Short Item No"], inplace=True)
        
        df_bakery_system.dropna(subset=["_id"], inplace=True)

        # Join F4101 and F41021
        joined_df = pd.merge(
            df_f4101,
            df_f41021,
            how="inner",
            left_on="Short Item No",
            right_on="Short Item No"
        )

        # Drop duplicate columns
        if "Short Item No_x" in joined_df.columns:
            joined_df = joined_df.drop(columns=["Short Item No_x"])
        
        if "Short Item No_y" in joined_df.columns:
            joined_df = joined_df.drop(columns=["Short Item No_y"])

        # Expand JSON columns in Bakery-System data
        df_bakeryops_expanded = expand_json_columns(df_bakery_system, ["onHand", "categoryFields"])

        joined_df = joined_df.rename(columns={"Description ": "Description"})
        joined_df = joined_df.rename(columns={"Description": "description"})
        df_bakeryops_expanded = df_bakeryops_expanded.rename(columns={"name": "description"})

        joined_df2 = pd.merge(
            joined_df,
            df_bakeryops_expanded,
            how="outer",
            left_on="description",
            right_on="description"
        )

        # Create pivot report
        pivot_report = joined_df2.groupby(["description"]).agg(
            jde_qoh=pd.NamedAgg(column="Quantity On Hand", aggfunc="first"),
            bakery_system_onhand_amount=pd.NamedAgg(column="onHand_amount", aggfunc="first"),
            bakery_system_batches=pd.NamedAgg(column="onHand_batches", aggfunc="first")
        ).reset_index()

        pivot_report = pivot_report.rename(columns={"Quantity On Hand": "jde_qoh"})

        # Determine status
        def determine_status(row):
            jde_val = row['jde_qoh']
            bakery_system_val = row['bakery_system_onhand_amount']
            
            # Handle None values
            if pd.isna(jde_val) or pd.isna(bakery_system_val):
                return 'Missing Data'
            
            try:
                jde_str = str(jde_val)
                bakery_system_str = str(bakery_system_val)
                return 'Match' if jde_str == bakery_system_str else 'Mismatch'
            except:
                return 'Error'

        pivot_report['status'] = pivot_report.apply(determine_status, axis=1)

        return {"data": to_dict_safe(pivot_report)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/live-data")
async def get_live_data_alias(days_back: int = 5):
    """Alias for get_joined_df3 for backward compatibility"""
    return await get_joined_df3(days_back)

@app.get("/data/joined_df3")
async def get_joined_df3(days_back: int = 5):
    """Get live data comparison between JDE and Bakery-System with dispatch capability"""
    try:
        # Ensure environment variables are loaded
        load_dotenv()
        
        # Get live data from JDE with configurable days back
        today = datetime.now()
        start_date = today - timedelta(days=days_back)
        date_str = start_date.strftime('%d/%m/%Y')
        bu = os.getenv('JDE_BUSINESS_UNIT', '1110')  # Use environment variable or default
        
        print(f"Fetching JDE data for {days_back} days back (since {date_str})")
        
        jde_data = get_latest_jde_cardex(bu, date_str)
        if not jde_data or 'ServiceRequest1' not in jde_data:
            raise HTTPException(status_code=500, detail="Failed to fetch JDE data")
        
        # Extract JDE transaction data
        jde_transactions = jde_data['ServiceRequest1']['fs_DATABROWSE_V4111A']['data']['gridData']['rowset']
        df_jde = pd.DataFrame(jde_transactions)
        
        # Get live data from Bakery Operations
        facility_id = os.getenv("FACILITY_ID")
        bakery_ops_base_url = os.getenv("BAKERY_OPS_BASE_URL")
        bakery_ops_api_token = os.getenv("BAKERY_OPS_API_TOKEN")
        
        if not all([facility_id, bakery_ops_base_url, bakery_ops_api_token]):
            raise HTTPException(status_code=500, detail="Missing required environment variables for Bakery Operations API")
        
        bakery_ops_data = get_data_from_bakery_operations()
        if not bakery_ops_data:
            raise HTTPException(status_code=500, detail="Failed to fetch Bakery Operations data")
        
        df_bakery_ops = pd.DataFrame(bakery_ops_data)
        # Calculate total bakery ops quantity on hand for each product name
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
        # Calculate total JDE quantity for each product name
        total_jde_quantity_map = {}
        for _, jde_row in df_jde.iterrows():
            product_name = str(jde_row['F4111_LITM']) if pd.notnull(jde_row['F4111_LITM']) else None
            from utility import preserve_quantity_precision
            jde_quantity = preserve_quantity_precision(jde_row['F4111_TRQT']) if pd.notnull(jde_row['F4111_TRQT']) else 0
            if product_name:
                total_jde_quantity_map[product_name.lower()] = total_jde_quantity_map.get(product_name.lower(), 0) + jde_quantity
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
            # Add total_jde_quantity and total_bakery_ops_quantity columns
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
        
        return {"data": convert_numpy_types(comparison_data)}
        
    except Exception as e:
        raise e #HTTPException(status_code=500, detail=f"Error in joined_df3: {str(e)}")

@app.post("/prepare_transaction_payload")
async def prepare_transaction_payload(request_data: dict):
    """
    Prepare transaction payload for review without dispatching
    """
    try:
        transaction_id = request_data.get('transaction_id')
        raw_jde_data = request_data.get('raw_jde_data')
        
        if not transaction_id or not raw_jde_data:
            raise HTTPException(status_code=400, detail="Missing transaction_id or raw_jde_data")
        
        # Create a mock JDE response format for submit_ingredient_batch_action (same as dispatch)
        jde_mock_response = {
            'ServiceRequest1': {
                'fs_DATABROWSE_V4111A': {
                    'data': {
                        'gridData': {
                            'rowset': [raw_jde_data]
                        }
                    }
                }
            }
        }
        
        # Return the payload that would be sent for review
        return {
            "success": True,
            "payload": jde_mock_response,
            "transaction_id": transaction_id,
            "raw_jde_data": raw_jde_data,
            "payload_summary": {
                "product_name": raw_jde_data.get('F4111_ITM', 'N/A'),
                "quantity": raw_jde_data.get('F4111_UORG', 'N/A'),
                "unit": raw_jde_data.get('F4111_UOM', 'N/A'),
                "lot_number": raw_jde_data.get('F4111_LOTN', 'N/A'),
                "document_number": raw_jde_data.get('F4111_DOC', 'N/A')
            }
        }
            
    except Exception as e:
        error_details = f"Error preparing transaction payload: {str(e)}"
        print(error_details)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")


@app.post("/prepare_ingredient_payload")
async def prepare_ingredient_payload(request_data: dict):
    """
    Prepare ingredient creation payload for review without creating
    """
    try:
        raw_jde_data = request_data.get('raw_jde_data')
        
        if not raw_jde_data:
            raise HTTPException(status_code=400, detail="Missing raw_jde_data")
        
        # Create a pandas Series from the raw data for compatibility
        jde_row = pd.Series(raw_jde_data)
        
        # For item master data, use F4102_LITM (short item number) as product name
        product_name = str(jde_row['F4102_LITM']) if pd.notnull(jde_row['F4102_LITM']) else None
        
        if not product_name:
            raise HTTPException(status_code=400, detail="Missing product name (F4102_LITM) in JDE data")
        
        # Prepare the payload that would be sent to Bakery-System for ingredient creation
        ingredient_payload = {
            "product_name": product_name,
            "item_data": raw_jde_data,
            "payload_summary": {
                "item_number": raw_jde_data.get('F4102_LITM', 'N/A'),
                "description": raw_jde_data.get('F4102_DSC1', 'N/A'),
                "unit_of_measure": raw_jde_data.get('F4102_UOM1', 'N/A'),
                "item_class": raw_jde_data.get('F4102_STNC', 'N/A'),
                "business_unit": raw_jde_data.get('F4102_MCU', 'N/A')
            }
        }
        
        return {
            "success": True,
            "payload": ingredient_payload,
            "product_name": product_name,
            "raw_jde_data": raw_jde_data
        }
            
    except Exception as e:
        error_details = f"Error preparing ingredient payload: {str(e)}"
        print(error_details)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")


@app.post("/dispatch/prepared_transaction")
async def dispatch_prepared_transaction(request_data: dict):
    """
    Dispatch a pre-prepared transaction payload
    """
    try:
        jde_payload = request_data.get('jde_payload')
        transaction_id = request_data.get('transaction_id')
        
        if not jde_payload or not transaction_id:
            raise HTTPException(status_code=400, detail="Missing jde_payload or transaction_id")
        
        # Submit the transaction using the prepared payload
        result = submit_ingredient_batch_action(jde_payload)
        
        if result:
            return {
                "success": True,
                "message": f"Transaction {transaction_id} dispatched successfully",
                "result": result
            }
        else:
            return {
                "success": False,
                "message": f"Failed to dispatch transaction {transaction_id}"
            }
            
    except Exception as e:
        error_details = f"Error dispatching prepared transaction: {str(e)}"
        print(error_details)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")


@app.post("/create/prepared_ingredient")
async def create_prepared_ingredient(request_data: dict):
    """
    Create ingredient using a pre-prepared payload
    """
    try:
        product_name = request_data.get('product_name')
        raw_jde_data = request_data.get('raw_jde_data')
        
        if not product_name or not raw_jde_data:
            raise HTTPException(status_code=400, detail="Missing product_name or raw_jde_data")
        
        # Create a pandas Series from the raw data for compatibility
        jde_row = pd.Series(raw_jde_data)
        
        # Use the specialized function for item master data
        result = fetch_or_create_ingredient_from_item_master(product_name, jde_row)
        
        if result:
            return {
                "success": True,
                "message": f"Ingredient {product_name} created successfully",
                "result": result
            }
        else:
            return {
                "success": False,
                "message": f"Failed to create ingredient {product_name}"
            }
            
    except Exception as e:
        error_details = f"Error creating prepared ingredient: {str(e)}"
        print(error_details)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")


@app.post("/dispatch/transaction")
async def dispatch_transaction(request_data: dict):
    """Dispatch a JDE transaction to Bakery-System"""
    try:
        transaction_id = request_data.get('transaction_id')
        raw_jde_data = request_data.get('raw_jde_data')
        
        if not transaction_id or not raw_jde_data:
            raise HTTPException(status_code=400, detail="Missing transaction_id or raw_jde_data")
        
        # Create a mock JDE response format for submit_ingredient_batch_action
        jde_mock_response = {
            'ServiceRequest1': {
                'fs_DATABROWSE_V4111A': {
                    'data': {
                        'gridData': {
                            'rowset': [raw_jde_data]
                        }
                    }
                }
            }
        }
        
        # Submit the transaction
        result = submit_ingredient_batch_action(jde_mock_response)
        
        if result:
            return {
                "success": True,
                "message": f"Transaction {transaction_id} dispatched successfully",
                "result": result
            }
        else:
            return {
                "success": False,
                "message": f"Failed to dispatch transaction {transaction_id}"
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error dispatching transaction: {str(e)}")

@app.get("/data/jde_item_master_review")
async def get_jde_item_master_review(days_back: int = 30, bu: str = None, gl_cat: str = "WA01"):
    """Get JDE Item Master data and compare with Bakery Operations ingredients"""
    try:
        # Ensure environment variables are loaded
        load_dotenv()
        
        # Get JDE Item Master data with configurable parameters
        today = datetime.now()
        start_date = today - timedelta(days=days_back)
        date_str = start_date.strftime('%d/%m/%Y')
        bu = bu or os.getenv('JDE_BUSINESS_UNIT', '1110')  # Use provided bu or environment default
        
        print(f"Debug - Calling get_jde_item_master with bu={bu}, date_str={date_str}, gl_cat={gl_cat}, days_back={days_back}")
        
        jde_data = get_jde_item_master(bu, date_str, gl_cat)
        print(f"Debug - JDE data result: {jde_data}")
        
        if not jde_data:
            raise HTTPException(status_code=500, detail=f"Failed to fetch JDE Item Master data. Check environment variables: JDE_ITEM_MASTER_UPDATES_URL, JDE_CARDEX_USERNAME, JDE_CARDEX_PASSWORD. Called with bu={bu}, date={date_str}, gl_cat={gl_cat}")
        
        if 'ServiceRequest1' not in jde_data:
            raise HTTPException(status_code=500, detail=f"Invalid JDE response format. Expected 'ServiceRequest1' key. Received: {json.dumps(jde_data, indent=2)}")
        
        # Check if the expected data structure exists
        service_request = jde_data['ServiceRequest1']
        if 'fs_DATABROWSE_V564102A' not in service_request:
            raise HTTPException(status_code=500, detail=f"Invalid JDE response structure. Expected 'fs_DATABROWSE_V564102A' key. Available keys: {list(service_request.keys())}. Full response: {json.dumps(jde_data, indent=2)}")
        
        # Extract JDE item master data
        try:
            df_json = service_request['fs_DATABROWSE_V564102A']['data']['gridData']['rowset']
            df_jde_items = pd.DataFrame([row for row in df_json])
            print(f"Debug - JDE items count: {len(df_jde_items)}")
            
            # Debug: Print available columns to see what fields we actually have
            if not df_jde_items.empty:
                print(f"Debug - Available JDE columns: {list(df_jde_items.columns)}")
                print(f"Debug - First row sample: {df_jde_items.iloc[0].to_dict()}")
                
        except KeyError as ke:
            raise HTTPException(status_code=500, detail=f"Error accessing JDE data structure: {ke}. Full JDE response: {json.dumps(jde_data, indent=2)}")
        
        # Get live data from Bakery Operations
        facility_id = os.getenv("FACILITY_ID")
        bakery_ops_base_url = os.getenv("BAKERY_OPS_BASE_URL")
        bakery_ops_api_token = os.getenv("BAKERY_OPS_API_TOKEN")
        
        if not all([facility_id, bakery_ops_base_url, bakery_ops_api_token]):
            missing_vars = []
            if not facility_id: missing_vars.append("FACILITY_ID")
            if not bakery_ops_base_url: missing_vars.append("BAKERY_OPS_BASE_URL")
            if not bakery_ops_api_token: missing_vars.append("BAKERY_OPS_API_TOKEN")
            raise HTTPException(status_code=500, detail=f"Missing required environment variables for Bakery Operations API: {', '.join(missing_vars)}")

        print("Debug - Calling get_data_from_bakery_operations")
        bakery_ops_data = get_data_from_bakery_operations()
        
        if not bakery_ops_data:
            raise HTTPException(status_code=500, detail="Failed to fetch Bakery Operations data. Check Bakery Operations API connectivity and credentials.")
        
        df_bakery_ops = pd.DataFrame(bakery_ops_data)
        print(f"Debug - Bakery Operations items count: {len(df_bakery_ops)}")
        
        # Create a mapping of existing Bakery Operations products (name -> product data)
        existing_bakery_ops_products = {}
        if not df_bakery_ops.empty and 'productName' in df_bakery_ops.columns:
            for _, bakery_ops_row in df_bakery_ops.iterrows():
                product_name = str(bakery_ops_row.get('productName', '')).lower()
                if product_name:
                    existing_bakery_ops_products[product_name] = {
                        'id': bakery_ops_row.get('product_id'),
                        'name': bakery_ops_row.get('productName'),
                        'archived': bakery_ops_row.get('archived', False)
                    }
        
        # Process and compare data
        comparison_data = []
        
        # Helper function to safely get field values
        def safe_get_field(row, field_name, default_value=None):
            """Safely get field value from row, handling missing fields"""
            try:
                if field_name in row and pd.notnull(row[field_name]):
                    return str(row[field_name])
                return default_value
            except Exception as e:
                print(f"Error accessing field {field_name}: {e}")
                return default_value
        
        for _, jde_row in df_jde_items.iterrows():
            # Use the same field names as in fetch_or_create_ingredient_from_item_master
            item_number = safe_get_field(jde_row, 'F4102_ITM')
            short_item_number = safe_get_field(jde_row, 'F4102_LITM')  # This is the product name
            description = safe_get_field(jde_row, 'F4101_DSC1')
            jde_uom = safe_get_field(jde_row, 'F4101_UOM1')
            
            # Use short item number (LITM) as product name for comparison, just like in the helper function
            product_name = short_item_number
            
            # Check if this product exists in Bakery Operations and get its ID
            bakery_ops_match = existing_bakery_ops_products.get(product_name.lower() if product_name else '')
            exists_in_bakery_ops = bakery_ops_match is not None
            product_id = bakery_ops_match.get('id') if bakery_ops_match else None
            
            # Determine status
            status = "Exists in Bakery Ops" if exists_in_bakery_ops else "Missing in Bakery Ops"
            
            comparison_data.append({
                'item_number': item_number,
                'short_item_number': short_item_number,
                'product_name': product_name,
                'description': description,
                'jde_stocking_type': safe_get_field(jde_row, 'F4101_STKT'),
                'jde_item_type': safe_get_field(jde_row, 'F4101_SITMTYP'),
                'jde_gl_class': safe_get_field(jde_row, 'F4102_GLPT'),
                'jde_uom': jde_uom,
                'status': status,
                'exists_in_bakery_ops': exists_in_bakery_ops,
                'product_id': product_id,
                'can_create': not exists_in_bakery_ops and product_name is not None,
                'raw_jde_data': jde_row.to_dict()
            })
        
        print(f"Debug - Returning {len(comparison_data)} comparison items")
        return {"data": convert_numpy_types(comparison_data)}
        
    except HTTPException:
        # Re-raise HTTPExceptions as-is
        raise
    except Exception as e:
        # Capture any other unexpected errors with full details
        error_details = f"Unexpected error in jde_item_master_review: {str(e)}"
        print(f"Error details: {error_details}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")

@app.post("/create/ingredient")
async def create_ingredient(request_data: dict):
    """Create a new ingredient in Bakery-System from JDE Item Master data"""
    try:
        raw_jde_data = request_data.get('raw_jde_data')
        
        if not raw_jde_data:
            raise HTTPException(status_code=400, detail="Missing raw_jde_data")
        
        # Create a pandas Series from the raw data for compatibility
        jde_row = pd.Series(raw_jde_data)
        
        # For item master data, use F4102_LITM (short item number) as product name
        product_name = str(jde_row['F4102_LITM']) if pd.notnull(jde_row['F4102_LITM']) else None
        
        if not product_name:
            raise HTTPException(status_code=400, detail="Missing product name (F4102_LITM) in JDE data")
        
        # Use the specialized function for item master data
        result = fetch_or_create_ingredient_from_item_master(product_name, jde_row)
        
        if result:
            return {
                "success": True,
                "message": f"Ingredient {product_name} created successfully",
                "result": result
            }
        else:
            return {
                "success": False,
                "message": f"Failed to create ingredient {product_name}"
            }
            
    except Exception as e:
        error_details = f"Error creating ingredient: {str(e)}"
        print(f"Error details: {error_details}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")

@app.patch("/patch/ingredient")
async def patch_Ingredient(request_data: dict):
    """Patch an Ingredient to set addition rate value and addition rate to None"""
    try:
        raw_jde_data = request_data.get('raw_jde_data')
        
        if not raw_jde_data:
            raise HTTPException(status_code=400, detail="Missing raw_jde_data")
        
        # Import the patch function
        from jde_helper import patch_one_item
        
        # Create a pandas Series from the raw data for compatibility
        jde_row = pd.Series(raw_jde_data)
        
        # Get the product name - check both possible sources
        product_name = None
        if 'F4102_LITM' in raw_jde_data and pd.notnull(raw_jde_data['F4102_LITM']):
            # Item Master data
            product_name = str(raw_jde_data['F4102_LITM'])
        elif 'F4111_LITM' in raw_jde_data and pd.notnull(raw_jde_data['F4111_LITM']):
            # CARDEX data
            product_name = str(raw_jde_data['F4111_LITM'])
        
        if not product_name:
            raise HTTPException(status_code=400, detail=f"Missing product name in JDE data. Available keys: {list(raw_jde_data.keys())}")
        
        # Use the patch function
        result = patch_one_item(jde_row.to_dict())
        
        if result:
            return {
                "success": True,
                "message": f"Ingredient {product_name} patched successfully (rate values set to None)",
                "result": result
            }
        else:
            return {
                "success": False,
                "message": f"Failed to patch Ingredient {product_name}"
            }
            
    except Exception as e:
        error_details = f"Error patching Ingredient: {str(e)}"
        print(f"Error details: {error_details}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")

@app.delete("/delete/ingredient/{ingredient_id}")
async def delete_Ingredient(ingredient_id: str):
    """Delete an Ingredient from Bakery-System"""
    try:
        # Load environment variables
        outlet_id = os.getenv("OUTLET_ID")
        bakery_system_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
        bakery_system_api_token = os.getenv("BAKERY_SYSTEM_TOKEN")  # Use BAKERY_SYSTEM_TOKEN to match read operations
        
        if not all([outlet_id, bakery_system_base_url, bakery_system_api_token]):
            missing_vars = []
            if not outlet_id: missing_vars.append("OUTLET_ID")
            if not bakery_system_base_url: missing_vars.append("BAKERY_SYSTEM_BASE_URL")
            if not bakery_system_api_token: missing_vars.append("BAKERY_SYSTEM_TOKEN")
            raise HTTPException(status_code=500, detail=f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Import requests here to avoid dependency issues
        
        # Construct the delete URL
        delete_url = f"{bakery_system_base_url}/outlets/{outlet_id}/ingredients/{ingredient_id}"
        
        # Set up headers
        headers = {
            'Authorization': f'Access-Token {bakery_system_api_token}',
            'Content-Type': 'application/json'
        }
        
        print(f"Attempting to delete Ingredient {ingredient_id} from URL: {delete_url}")
        
        # Make the DELETE request
        response = requests.delete(delete_url, headers=headers, timeout=30)
        
        print(f"Delete response status: {response.status_code}")
        print(f"Delete response headers: {dict(response.headers)}")
        
        if response.status_code == 200 or response.status_code == 204:
            # Success - Ingredient was deleted
            return {
                "success": True,
                "message": f"Ingredient {ingredient_id} deleted successfully",
                "status_code": response.status_code
            }
        elif response.status_code == 404:
            # Ingredient not found
            return {
                "success": False,
                "message": f"Ingredient {ingredient_id} not found in Bakery-System",
                "status_code": response.status_code,
                "error": response.text
            }
        else:
            # Other error
            error_text = response.text
            print(f"Delete failed with status {response.status_code}: {error_text}")
            return {
                "success": False,
                "message": f"Failed to delete Ingredient {ingredient_id}",
                "status_code": response.status_code,
                "error": error_text
            }
            
    except requests.exceptions.RequestException as req_error:
        error_details = f"Network error deleting Ingredient {ingredient_id}: {str(req_error)}"
        print(f"Request error: {error_details}")
        raise HTTPException(status_code=500, detail=error_details)
    except Exception as e:
        error_details = f"Error deleting Ingredient {ingredient_id}: {str(e)}"
        print(f"Error details: {error_details}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")

# ------------------------
# Bakery-System to JDE Endpoints
# ------------------------

@app.get("/data/bakery_system_to_jde_actions")
async def get_bakery_system_to_jde_actions(days_back: int = 3):
    """Fetch Bakery-System actions (depletions) data - streamlined version"""
    try:
        from bakery_helper import get_streamlined_action_data
        
        # Calculate start date based on days_back parameter
        today = datetime.now()
        start_date_obj = today - timedelta(days=days_back)
        start_date = start_date_obj.strftime('%Y-%m-%d')
        
        print(f"Fetching streamlined Bakery-System action data for {days_back} days back (since {start_date})")
        
        # Get streamlined data with individual batches
        batch_records = get_streamlined_action_data(start_date=start_date)
        
        if not batch_records:
            return JSONResponse(
                status_code=404,
                content={"error": "No action data found"}
            )
        
        print(f"Successfully processed {len(batch_records)} batch records")
        
        return JSONResponse(content={
            "success": True,
            "data": batch_records,
            "total_records": len(batch_records)
        })
        
    except Exception as e:
        error_details = f"Error fetching Bakery-System actions: {str(e)}"
        print(error_details)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")


@app.post("/prepare_jde_payload")
async def prepare_jde_payload_endpoint(request_data: dict):
    """
    Prepare JDE payload for preview without dispatching
    
    Expected payload: same as dispatch but for preparation only
    """
    try:
        from jde_helper import prepare_jde_payload
        
        # Validate required fields
        required_fields = ['action_id', 'ingredient_id', 'ingredient_name', 'batch_id', 'quantity', 'unit']
        missing_fields = [field for field in required_fields if not request_data.get(field)]
        
        if missing_fields:
            raise HTTPException(
                status_code=400, 
                detail=f"Missing required fields: {', '.join(missing_fields)}"
            )
        
        print(f"Preparing JDE payload for batch {request_data['batch_id']}")
        
        # Prepare payload for preview
        result = prepare_jde_payload(request_data)
        
        return JSONResponse(content=result)
            
    except Exception as e:
        error_details = f"Error preparing JDE payload: {str(e)}"
        print(error_details)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")


@app.post("/dispatch/prepared_payload_to_jde")
async def dispatch_prepared_payload_to_jde_endpoint(request_data: dict):
    """
    Dispatch a pre-prepared and potentially edited JDE payload
    
    Expected payload:
    {
        "jde_payload": {...},  // The JDE payload to send
        "batch_data": {...}    // Original batch data for logging
    }
    """
    try:
        from jde_helper import dispatch_prepared_payload_to_jde
        
        jde_payload = request_data.get('jde_payload')
        batch_data = request_data.get('batch_data')
        
        if not jde_payload or not batch_data:
            raise HTTPException(
                status_code=400, 
                detail="Both 'jde_payload' and 'batch_data' are required"
            )
        
        print(f"Dispatching prepared payload for batch {batch_data.get('batch_id')}")
        
        # Dispatch the prepared payload
        result = dispatch_prepared_payload_to_jde(jde_payload, batch_data)
        
        if result.get("success"):
            return JSONResponse(content=result)
        else:
            return JSONResponse(
                status_code=400,
                content=result
            )
            
    except Exception as e:
        error_details = f"Error dispatching prepared payload: {str(e)}"
        print(error_details)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")


@app.post("/dispatch/batch_to_jde")
async def dispatch_batch_to_jde(request_data: dict):
    """
    Dispatch a single batch to JDE
    
    Expected payload:
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
    try:
        from jde_helper import dispatch_single_batch_to_jde
        
        # Validate required fields
        required_fields = ['action_id', 'ingredient_id', 'ingredient_name', 'batch_id', 'quantity', 'unit']
        missing_fields = [field for field in required_fields if not request_data.get(field)]
        
        if missing_fields:
            raise HTTPException(
                status_code=400, 
                detail=f"Missing required fields: {', '.join(missing_fields)}"
            )
        
        print(f"Dispatching batch {request_data['batch_id']} to JDE")
        
        # Dispatch to JDE
        result = dispatch_single_batch_to_jde(request_data)
        
        if result.get("success"):
            return JSONResponse(content=result)
        else:
            return JSONResponse(
                status_code=400,
                content=result
            )
            
    except Exception as e:
        error_details = f"Error dispatching batch to JDE: {str(e)}"
        print(error_details)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")


# Legacy Endpoint (kept for backwards compatibility)
@app.get("/data/bakery_system_to_jde_actions_legacy")
async def get_bakery_system_to_jde_actions():
    """Fetch Bakery-System actions (depletions) data"""
    try:
        from bakery_helper import fetch_action_data_from_bakery_system_api, parse_bakery_system_action_data
        from datetime import datetime, timedelta
        
        # Get last 30 days of data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3)
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        print(f"Fetching Bakery-System actions from {start_date_str}")
        
        # Fetch raw action data
        raw_data = fetch_action_data_from_bakery_system_api(start_date=start_date_str)
        
        if not raw_data:
            return JSONResponse(
                status_code=404,
                content={"error": "No action data found"}
            )
        
        # Parse the action data
        parsed_data = parse_bakery_system_action_data(raw_data)
        
        # Convert to list for frontend
        actions_list = json.loads(parsed_data)
        
        print(f"Successfully processed {len(actions_list)} action records")
        
        return JSONResponse(content={
            "data": actions_list,
            "total_records": len(actions_list),
            "date_range": {
                "start": start_date_str,
                "end": end_date.strftime('%Y-%m-%d')
            }
        })
        
    except Exception as e:
        error_details = f"Error fetching Bakery-System action data: {str(e)}"
        print(f"Error details: {error_details}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")

# ------------------------
# Patch Ingredient Endpoints
# ------------------------

@app.get("/search/ingredient")
async def search_ingredient_by_name(name: str):
    """Search for an Ingredient by name in Bakery-System"""
    try:
        from jde_helper import fetch_existing_ingredient
        
        if not name or not name.strip():
            raise HTTPException(status_code=400, detail="Ingredient name is required")
        
        print(f"Searching for Ingredient: {name}")
        
        # Search for the Ingredient
        result = fetch_existing_ingredient(name.strip())
        
        if result is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"Ingredient '{name}' not found in Bakery-System"}
            )
        
        print(f"Found Ingredient: {result.get('_id', 'N/A')}")
        
        return JSONResponse(content={
            "success": True,
            "Ingredient": result
        })
        
    except Exception as e:
        error_details = f"Error searching for Ingredient '{name}': {str(e)}"
        print(f"Error details: {error_details}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")

@app.get("/test/units")
async def test_unit_endpoints():
    """Test endpoint to show available unit patch endpoints"""
    return {
        "available_endpoints": {
            "/patch/ingredient": {
                "description": "Basic patch - follows JDE data validation and conversion rules",
                "unit_handling": "Validates and converts units using utility.convert_unit()"
            },
            "/patch/ingredient/enhanced": {
                "description": "Enhanced patch by Ingredient ID - NO CONVERSION",
                "unit_handling": "Uses inventory_unit and addition_unit AS-IS without validation or conversion",
                "parameters": ["ingredient_id", "product_name", "inventory_unit", "addition_unit"]
            },
            "/patch/ingredient/advanced": {
                "description": "Advanced patch by Ingredient name - NO CONVERSION", 
                "unit_handling": "Uses new_inventory_unit and new_addition_unit AS-IS without validation or conversion",
                "parameters": ["ingredient_name", "new_name", "new_inventory_unit", "new_addition_unit"]
            }
        },
        "example_usage": {
            "enhanced": {
                "url": "/patch/ingredient/enhanced",
                "method": "POST",
                "body": {
                    "ingredient_id": "your_ingredient_id",
                    "product_name": "New Product Name",
                    "inventory_unit": "L",
                    "addition_unit": "mL"
                }
            },
            "advanced": {
                "url": "/patch/ingredient/advanced", 
                "method": "POST",
                "body": {
                    "ingredient_name": "Current Product Name",
                    "new_name": "New Product Name",
                    "new_inventory_unit": "KG",
                    "new_addition_unit": "G"
                }
            }
        }
    }

@app.post("/patch/ingredient/enhanced")
async def patch_ingredient_enhanced(request_data: dict):
    """
    Enhanced patch for ingredients with editable name and units (NO CONVERSION)
    Units are sent AS-IS to Bakery-System without any validation or conversion
    Expects: {
        "ingredient_id": str,
        "product_name": str (optional),
        "inventory_unit": str (optional - used as-is),
        "addition_unit": str (optional - used as-is),
        "raw_jde_data": dict (optional, for fallback compatibility)
    }
    """
    try:
        from bakery_helper import fetch_existing_ingredient_by_id
        
        # Get parameters
        ingredient_id = request_data.get("ingredient_id")
        product_name = request_data.get("product_name")
        inventory_unit = request_data.get("inventory_unit")
        addition_unit = request_data.get("addition_unit")
        raw_jde_data = request_data.get("raw_jde_data", {})
        
        if not ingredient_id:
            raise HTTPException(status_code=400, detail="ingredient_id is required")
        
        print(f"Enhanced patching Ingredient ID: {ingredient_id}")
        
        # Find the existing Ingredient by ID
        result = fetch_existing_ingredient_by_id(ingredient_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Ingredient with ID '{ingredient_id}' not found")
        
        # Make a copy to modify
        result = dict(result)
        
        # Track what was updated
        updates_made = []
        
        # Update product name if provided
        if product_name and product_name.strip():
            old_name = result.get('name', '')
            result['name'] = product_name.strip()
            updates_made.append(f"name: '{old_name}' → '{product_name.strip()}'")
        
        # Update inventory unit if provided (NO CONVERSION - use as-is)
        if inventory_unit and inventory_unit.strip():
            old_unit = result.get('inventoryUnit', '')
            result['inventoryUnit'] = inventory_unit.strip()
            updates_made.append(f"inventoryUnit: '{old_unit}' → '{inventory_unit.strip()}'")
        
        # Update addition unit if provided (NO CONVERSION - use as-is)
        final_addition_unit = addition_unit.strip() if addition_unit and addition_unit.strip() else result.get('inventoryUnit', '')
        
        # Ensure categoryFields exists
        if 'categoryFields' not in result:
            result['categoryFields'] = {}
        
        old_addition_unit = result['categoryFields'].get('additionUnit', '')
        result['categoryFields']['additionUnit'] = final_addition_unit
        result['categoryFields']['additionRateUnit'] = None
        result['categoryFields']['additionRateValue'] = None
        
        if addition_unit and addition_unit.strip():
            updates_made.append(f"additionUnit: '{old_addition_unit}' → '{final_addition_unit}'")
        
        # Clean up fields
        result['defaultVendorId'] = None
        result['defaultVendor'] = None
        result['indicators'] = []
        
        # Make the API call to update
        load_dotenv()
        outlet_id = os.getenv("OUTLET_ID")
        bakeryops_token = os.getenv("BAKERY_SYSTEM_TOKEN")
        bakeryops_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
        
        headers = {'Content-Type': 'application/json', 'Authorization': f'Access-Token {bakeryops_token}'}
        url = f'{bakeryops_base_url}/outlets/{outlet_id}/ingredients/{ingredient_id}'
        
        print(f"Sending enhanced patch request to: {url}")
        print(f"Updates made: {updates_made}")
        
        # Use retry_request for reliable API call
        from utility import retry_request
        upd_result = retry_request(url=url, headers=headers, method='PUT', payload=result)
        
        return {
            "success": True,
            "message": f"Ingredient {ingredient_id} patched successfully",
            "updates_made": updates_made,
            "result": upd_result
        }
        
    except Exception as e:
        error_details = f"Error in enhanced patch: {str(e)}"
        print(f"Error details: {error_details}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")

@app.post("/patch/ingredient/advanced")
async def patch_ingredient_advanced(request_data: dict):
    """
    Advanced patch for an Ingredient with custom name and unit modifications
    Units are used AS-IS without any validation or conversion
    Expects: {
        "ingredient_name": str,
        "new_name": str (optional),
        "new_inventory_unit": str (optional - used as-is),
        "new_addition_unit": str (optional - used as-is)
    }
    """
    try:
        from jde_helper import fetch_existing_ingredient
        from utility import retry_request
        
        # Validate input
        ingredient_name = request_data.get("ingredient_name")
        new_name = request_data.get("new_name")
        new_inventory_unit = request_data.get("new_inventory_unit") 
        new_addition_unit = request_data.get("new_addition_unit")
        
        if not ingredient_name or not ingredient_name.strip():
            raise HTTPException(status_code=400, detail="ingredient_name is required")
        
        # At least one field must be provided to update, or allow clearing rate fields
        if not any([new_name, new_inventory_unit, new_addition_unit]):
            # Allow the operation if we're just clearing rate fields (which is still an update)
            print(f"Advanced patch proceeding to clear additionRateUnit and additionRateValue for: {ingredient_name}")
        
        print(f"Advanced patching Ingredient: {ingredient_name} - NO UNIT CONVERSION")
        
        # Find the existing Ingredient
        result = fetch_existing_ingredient(ingredient_name.strip())
        if result is None:
            raise HTTPException(status_code=404, detail=f"Ingredient '{ingredient_name}' not found in Bakery-System")
        
        # Make a copy to modify
        result = dict(result)
        ingredient_id = result['_id']
        
        # Track what was updated
        updates_made = []
        
        # Update name if provided
        if new_name and new_name.strip() and new_name.strip() != result.get('name'):
            result['name'] = new_name.strip()
            updates_made.append(f"name: '{result.get('name')}' → '{new_name.strip()}'")
        
        # Update inventory unit if provided (NO CONVERSION - use as-is)
        if new_inventory_unit and new_inventory_unit.strip():
            old_inventory_unit = result.get('inventoryUnit', '')
            result['inventoryUnit'] = new_inventory_unit.strip()
            updates_made.append(f"inventoryUnit: '{old_inventory_unit}' → '{new_inventory_unit.strip()}'")
        
        # Set up the categoryFields with proper structure
        inventory_unit_final = result['inventoryUnit']
        
        # Update addition unit if provided (NO CONVERSION - use as-is) or default to inventory unit
        if new_addition_unit and new_addition_unit.strip():
            converted_addition_unit = new_addition_unit.strip()  # Use as-is without conversion
            updates_made.append(f"additionUnit: '{result.get('categoryFields', {}).get('additionUnit')}' → '{converted_addition_unit}'")
        else:
            # Default to inventory unit if not specified
            converted_addition_unit = inventory_unit_final
            updates_made.append(f"additionUnit set to match inventoryUnit: '{converted_addition_unit}'")
        
        # Set categoryFields with the complete structure
        result['categoryFields'] = {
            "additionUnit": converted_addition_unit,
            "additionRateUnit": None,
            "additionRateValue": None,
            "additionCustomUnit": False,
            "concentration": None,
            "instructions": ""
        }
        
        result['additionCustomUnit'] = {
            "additionUnit": converted_addition_unit,
            "additionRateUnit": None,
            "additionRateValue": None,
            "additionCustomUnit": False,
            "concentration": None,
            "instructions": ""
        }

        # Clean up fields that shouldn't be sent in update
        result['defaultVendorId'] = None
        result['defaultVendor'] = None
        result['indicators'] = []
        
        # Make the API call to update
        load_dotenv()
        outlet_id = os.getenv("OUTLET_ID")
        bakeryops_token = os.getenv("BAKERY_SYSTEM_TOKEN")
        bakeryops_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
        
        if not all([outlet_id, bakeryops_token, bakeryops_base_url]):
            raise HTTPException(status_code=500, detail="Missing Bakery-System API configuration")
        
        url = f"{bakeryops_base_url}/outlets/{outlet_id}/ingredients/{ingredient_id}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Access-Token {bakeryops_token}'
        }
        
        print(f"Updating Ingredient {ingredient_id} with changes: {updates_made}")
        
        # Use retry_request for the update
        upd_result = retry_request(url=url, headers=headers, method='PUT', payload=result)
        
        if upd_result is None:
            raise HTTPException(status_code=500, detail="Failed to update Ingredient - API returned None")
        
        return JSONResponse(content={
            "success": True,
            "message": f"Successfully updated Ingredient '{ingredient_name}'",
            "ingredient_id": ingredient_id,
            "updates_made": updates_made,
            "updated_Ingredient": upd_result
        })
        
    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except Exception as e:
        error_details = f"Error in advanced patch for Ingredient '{ingredient_name}': {str(e)}"
        print(f"Error details: {error_details}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{error_details}\n\nTraceback:\n{traceback.format_exc()}")

@app.post("/batch_review/create_session")
async def create_batch_review_session_endpoint(request: Request):
    """
    Create a session for batch review
    
    Expected payload: Array of batch data objects directly
    """
    try:
        from session_helper import create_batch_review_session
        
        # Parse the JSON body directly
        batch_data = await request.json()
        
        if not batch_data or not isinstance(batch_data, list):
            raise HTTPException(
                status_code=400,
                detail="Request body must be a non-empty array of batch data objects"
            )
        
        result = create_batch_review_session(batch_data)
        
        if result.get("success"):
            return JSONResponse(content=result)
        else:
            return JSONResponse(
                status_code=400,
                content=result
            )
            
    except Exception as e:
        error_details = f"Error creating batch review session: {str(e)}"
        print(error_details)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=error_details)

@app.get("/batch_review/get_session/{session_id}")
async def get_batch_review_session_endpoint(session_id: str):
    """
    Get batch review data from session
    """
    try:
        from session_helper import get_batch_review_session
        
        result = get_batch_review_session(session_id)
        
        if result.get("success"):
            return JSONResponse(content=result)
        else:
            return JSONResponse(
                status_code=404,
                content=result
            )
            
    except Exception as e:
        error_details = f"Error retrieving batch review session: {str(e)}"
        print(error_details)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=error_details)

@app.delete("/batch_review/delete_session/{session_id}")
async def delete_batch_review_session_endpoint(session_id: str):
    """
    Delete batch review session
    """
    try:
        from session_helper import delete_batch_review_session
        
        result = delete_batch_review_session(session_id)
        
        if result.get("success"):
            return JSONResponse(content=result)
        else:
            return JSONResponse(
                status_code=400,
                content=result
            )
            
    except Exception as e:
        error_details = f"Error deleting batch review session: {str(e)}"
        print(error_details)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=error_details)

# Run the server with:
# uvicorn main:app --reload

# ------------------------
# Internal Bakery Operations Endpoints
# ------------------------

# Mock data store for demonstration - in production this would connect to a real database
bakery_ops_products = []
bakery_ops_movements = []

@app.get("/bakeryops/facilities/{facility_id}/products")
async def get_bakery_ops_products(
    facility_id: str,
    archived: bool = False,
    includeAccess: bool = True,
    includeBatches: bool = True,
    includeNotes: bool = True,
    offset: int = 0,
    productCategory: str = "Ingredient",
    size: int = 100000,
    sort: str = "productName:1"
):
    """Internal bakery ops endpoint to get products"""
    try:
        # Filter products based on parameters
        filtered_products = [
            product for product in bakery_ops_products 
            if (product.get("facility_id") == facility_id and 
                product.get("archived", False) == archived and
                product.get("productCategory") == productCategory)
        ]
        
        # Sort products
        if sort == "productName:1":
            filtered_products.sort(key=lambda x: x.get("productName", ""))
        
        # Apply pagination
        paginated_products = filtered_products[offset:offset + size]
        
        return paginated_products
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/bakeryops/facilities/{facility_id}/products")
async def create_bakery_ops_product(facility_id: str, product_data: dict):
    """Internal bakery ops endpoint to create products"""
    try:
        # Generate a unique product ID
        product_id = f"prod_{len(bakery_ops_products) + 1}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Create the product record
        new_product = {
            "_id": product_id,
            "facility_id": facility_id,
            "productName": product_data.get("productName"),
            "description": product_data.get("description", ""),
            "productCategory": product_data.get("productCategory", "Ingredient"),
            "inventoryUnit": product_data.get("inventoryUnit", "EA"),
            "defaultVendor": product_data.get("defaultVendor", {}),
            "notes": product_data.get("notes", []),
            "archived": product_data.get("archived", False),
            "onHand": {
                "amount": 0,
                "batches": []
            },
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Add to the mock storage
        bakery_ops_products.append(new_product)
        
        # Store in S3 for audit trail
        try:
            s3_helper.store_jde_dispatch([new_product], 'bakery_ops_product_creations')
        except Exception as s3_error:
            print(f"Warning: Failed to log product creation to S3: {s3_error}")
        
        return new_product
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/bakeryops/facilities/{facility_id}/inventory-adjustments")
async def create_inventory_adjustment(facility_id: str, adjustment_data: dict):
    """Internal bakery ops endpoint for inventory adjustments"""
    try:
        # Generate a unique adjustment ID
        adjustment_id = f"adj_{len(bakery_ops_movements) + 1}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Create the adjustment record
        adjustment = {
            "_id": adjustment_id,
            "facility_id": facility_id,
            "productId": adjustment_data.get("productId"),
            "batchNumber": adjustment_data.get("batchNumber"),
            "quantity": adjustment_data.get("quantity"),
            "unit": adjustment_data.get("unit"),
            "adjustmentType": adjustment_data.get("adjustmentType", "USAGE"),
            "reason": adjustment_data.get("reason"),
            "adjustmentDate": adjustment_data.get("adjustmentDate", datetime.now().isoformat()),
            "vesselCode": adjustment_data.get("vesselCode", ""),
            "lotNumber": adjustment_data.get("lotNumber", ""),
            "notes": adjustment_data.get("notes", ""),
            "created_at": datetime.now().isoformat()
        }
        
        # Add to movements storage
        bakery_ops_movements.append(adjustment)
        
        # Update product on-hand quantity if product exists
        product = next((p for p in bakery_ops_products if p["_id"] == adjustment_data.get("productId")), None)
        if product:
            # Update the on-hand amount (subtract for USAGE)
            if adjustment["adjustmentType"] == "USAGE":
                product["onHand"]["amount"] = max(0, product["onHand"]["amount"] - adjustment["quantity"])
            else:
                product["onHand"]["amount"] += adjustment["quantity"]
            
            # Add/update batch information
            batch_info = {
                "_id": f"batch_{adjustment_id}",
                "batchNumber": adjustment["batchNumber"],
                "lotNumber": adjustment["lotNumber"],
                "quantity": adjustment["quantity"],
                "unit": adjustment["unit"],
                "vesselCode": adjustment["vesselCode"]
            }
            product["onHand"]["batches"].append(batch_info)
            product["updated_at"] = datetime.now().isoformat()
        
        # Store in S3 for audit trail
        try:
            s3_helper.store_jde_dispatch([adjustment], 'bakery_ops_inventory_adjustments')
        except Exception as s3_error:
            print(f"Warning: Failed to log adjustment to S3: {s3_error}")
        
        return adjustment
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/bakeryops/facilities/{facility_id}/inventory-movements")
async def get_inventory_movements(
    facility_id: str,
    movementTypes: str = None,
    includeProductDetails: bool = True,
    startDate: str = None,
    sort: str = "movementDate:1"
):
    """Internal bakery ops endpoint to get inventory movements"""
    try:
        # Filter movements based on parameters
        filtered_movements = [
            movement for movement in bakery_ops_movements 
            if movement.get("facility_id") == facility_id
        ]
        
        # Filter by movement types if specified
        if movementTypes:
            movement_type_list = movementTypes.split(',')
            filtered_movements = [
                movement for movement in filtered_movements
                if movement.get("adjustmentType") in movement_type_list
            ]
        
        # Filter by start date if specified
        if startDate:
            try:
                start_date_obj = datetime.fromisoformat(startDate.replace('Z', ''))
                filtered_movements = [
                    movement for movement in filtered_movements
                    if datetime.fromisoformat(movement.get("adjustmentDate", "").replace('Z', '')) >= start_date_obj
                ]
            except Exception as date_error:
                print(f"Date parsing error: {date_error}")
        
        # Include product details if requested
        if includeProductDetails:
            for movement in filtered_movements:
                product_id = movement.get("productId")
                product = next((p for p in bakery_ops_products if p["_id"] == product_id), None)
                if product:
                    movement["product"] = {
                        "_id": product["_id"],
                        "productName": product["productName"],
                        "inventoryUnit": product["inventoryUnit"],
                        "productCategory": product["productCategory"]
                    }
                    
                    # Add batch information
                    movement["batches"] = [{
                        "batch": {
                            "_id": f"batch_{movement['_id']}",
                            "batchNumber": movement.get("batchNumber", ""),
                            "lotNumber": movement.get("lotNumber", "")
                        },
                        "quantityUsed": movement.get("quantity", 0),
                        "unit": movement.get("unit", "EA")
                    }]
        
        # Sort movements
        if sort == "movementDate:1":
            filtered_movements.sort(key=lambda x: x.get("adjustmentDate", ""))
        
        # Store fetch operation in S3 for audit
        try:
            fetch_record = {
                'action': 'fetch_movements',
                'facility_id': facility_id,
                'filter_params': {
                    'movementTypes': movementTypes,
                    'startDate': startDate,
                    'includeProductDetails': includeProductDetails
                },
                'result_count': len(filtered_movements),
                'fetch_date': datetime.now().isoformat()
            }
            s3_helper.store_jde_dispatch([fetch_record], 'bakery_ops_movement_fetches')
        except Exception as s3_error:
            print(f"Warning: Failed to log movement fetch to S3: {s3_error}")
        
        return filtered_movements
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/bakeryops/facilities/{facility_id}/batch-data")
async def add_sample_batch_data(facility_id: str):
    """Helper endpoint to add sample data for testing"""
    try:
        # Add some sample products
        sample_products = [
            {
                "_id": "prod_001",
                "facility_id": facility_id,
                "productName": "Flour",
                "description": "All-purpose flour",
                "productCategory": "Ingredient",
                "inventoryUnit": "KG",
                "onHand": {"amount": 100, "batches": []},
                "archived": False,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            },
            {
                "_id": "prod_002", 
                "facility_id": facility_id,
                "productName": "Sugar",
                "description": "White granulated sugar",
                "productCategory": "Ingredient",
                "inventoryUnit": "KG",
                "onHand": {"amount": 50, "batches": []},
                "archived": False,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
        ]
        
        # Add sample movements
        sample_movements = [
            {
                "_id": "mov_001",
                "facility_id": facility_id,
                "productId": "prod_001",
                "batchNumber": "FLOUR_001",
                "quantity": 10,
                "unit": "KG",
                "adjustmentType": "USAGE",
                "reason": "Production batch 001",
                "adjustmentDate": datetime.now().isoformat(),
                "vesselCode": "V001",
                "lotNumber": "LOT001",
                "created_at": datetime.now().isoformat()
            },
            {
                "_id": "mov_002",
                "facility_id": facility_id, 
                "productId": "prod_002",
                "batchNumber": "SUGAR_001",
                "quantity": 5,
                "unit": "KG",
                "adjustmentType": "USAGE",
                "reason": "Production batch 001",
                "adjustmentDate": datetime.now().isoformat(),
                "vesselCode": "V001",
                "lotNumber": "LOT002",
                "created_at": datetime.now().isoformat()
            }
        ]
        
        # Clear existing data and add samples
        global bakery_ops_products, bakery_ops_movements
        bakery_ops_products.clear()
        bakery_ops_movements.clear()
        
        bakery_ops_products.extend(sample_products)
        bakery_ops_movements.extend(sample_movements)
        
        return {
            "success": True,
            "message": "Sample data added successfully",
            "products_added": len(sample_products),
            "movements_added": len(sample_movements)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------
# Updated Bakery Ops Helper Function Integration
# ------------------------

@app.get("/data/internal_bakery_ops_expanded")
async def get_internal_bakery_ops_expanded():
    """Get bakery ops data from internal endpoints instead of external API"""
    try:
        facility_id = os.getenv("FACILITY_ID", "default_facility")
        
        # Call our internal endpoint
        products = await get_bakery_ops_products(
            facility_id=facility_id,
            archived=False,
            productCategory="Ingredient"
        )
        
        # Convert to DataFrame for processing
        df_bakery_ops = pd.DataFrame(products)
        
        if df_bakery_ops.empty:
            return {"data": [], "message": "No products found"}
        
        # Convert product IDs to numeric if possible
        if "_id" in df_bakery_ops.columns:
            # Extract numeric part from product IDs for compatibility
            df_bakery_ops["product_id"] = df_bakery_ops["_id"].apply(
                lambda x: hash(str(x)) % 1000000 if x else 0
            )
        
        # Expand JSON-like columns
        df_expanded = expand_json_columns(df_bakery_ops, ["onHand"])
        
        return {"data": to_dict_safe(df_expanded)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------
# Test and Development Endpoints
# ------------------------

@app.post("/dev/initialize-sample-data")
async def initialize_sample_data():
    """Initialize the system with sample data for testing"""
    try:
        facility_id = os.getenv("FACILITY_ID", "default_facility")
        
        # Call our internal endpoint to add sample data
        result = await add_sample_batch_data(facility_id)
        
        return {
            "success": True,
            "message": "Sample data initialized successfully",
            "facility_id": facility_id,
            "result": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dev/test-internal-bakery-ops")
async def test_internal_bakery_ops():
    """Test the internal bakery ops endpoints"""
    try:
        facility_id = os.getenv("FACILITY_ID", "default_facility")
        
        # First, initialize sample data
        await add_sample_batch_data(facility_id)
        
        # Test getting products
        products = await get_bakery_ops_products(
            facility_id=facility_id,
            productCategory="Ingredient"
        )
        
        # Test getting movements
        movements = await get_inventory_movements(
            facility_id=facility_id,
            movementTypes="USAGE",
            includeProductDetails=True
        )
        
        return {
            "success": True,
            "facility_id": facility_id,
            "products_count": len(products),
            "movements_count": len(movements),
            "products": products[:2],  # Show first 2 products as example
            "movements": movements[:2]  # Show first 2 movements as example
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
