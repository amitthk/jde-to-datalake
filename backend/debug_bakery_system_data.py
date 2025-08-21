#!/usr/bin/env python3

import json
import sys
from pathlib import Path

# Add the current directory to Python path to import local modules
sys.path.append(str(Path(__file__).parent))

from backend.bakery_helper import fetch_action_data_from_bakery_system_api
from datetime import datetime, timedelta

def debug_bakery_system_data():
    """Debug function to examine the actual structure of Bakery-System data"""
    
    # Get last 3 days of data
    today = datetime.now()
    start_date = (today - timedelta(days=3)).strftime('%Y-%m-%d')
    
    print(f"Fetching raw data since: {start_date}")
    
    try:
        raw_data = fetch_action_data_from_bakery_system_api(start_date=start_date)
        if not raw_data:
            print("No raw data received")
            return
        
        data = json.loads(raw_data)
        print(f"Found {len(data)} actions")
        
        # Examine first few actions to understand structure
        for i, entry in enumerate(data[:2]):  # Look at first 2 actions
            if entry.get("actionType") != "ADDITION":
                continue
                
            print(f"\n=== ACTION {i+1} ===")
            print(f"Action ID: {entry.get('_id')}")
            print(f"Action Type: {entry.get('actionType')}")
            
            action_data = entry.get("actionData", {})
            print(f"ActionData keys: {list(action_data.keys())}")
            
            vessel = action_data.get("vessel", {})
            print(f"Vessel keys: {list(vessel.keys())}")
            print(f"Vessel name: {vessel.get('name', 'N/A')}")
            
            vessel_additions = vessel.get("additions", {})
            print(f"Vessel additions: {vessel_additions}")
            print(f"Vessel additions type: {type(vessel_additions)}")
            
            ingredients = action_data.get("ingredients", [])
            print(f"Number of ingredients: {len(ingredients)}")
            
            for j, ingredient_entry in enumerate(ingredients[:1]):  # Look at first Ingredient
                print(f"\n  --- Ingredient {j+1} ---")
                print(f"  Ingredient entry keys: {list(ingredient_entry.keys())}")
                
                Ingredient = ingredient_entry.get("Ingredient", {})
                print(f"  Ingredient ID: {Ingredient.get('_id')}")
                print(f"  Ingredient name: {Ingredient.get('productName')}")
                print(f"  Addition unit: {Ingredient.get('additionUnit')}")
                
                batches = ingredient_entry.get("batches", [])
                print(f"  Number of batches: {len(batches)}")
                
                for k, batch_entry in enumerate(batches[:2]):  # Look at first 2 batches
                    print(f"\n    ... BATCH {k+1} ...")
                    print(f"    Batch entry keys: {list(batch_entry.keys())}")
                    
                    batch = batch_entry.get("batch", {})
                    print(f"    Batch ID: {batch.get('_id')}")
                    print(f"    Batch number: {batch.get('batchNumber')}")
                    print(f"    Batch keys: {list(batch.keys())}")
                    
                    # Look for any quantity-related fields
                    for key, value in batch_entry.items():
                        if any(qty_word in key.lower() for qty_word in ['amount', 'quantity', 'size', 'value', 'depleted']):
                            print(f"    Found quantity field in batch_entry: {key} = {value}")
                    
                    for key, value in batch.items():
                        if any(qty_word in key.lower() for qty_word in ['amount', 'quantity', 'size', 'value', 'depleted']):
                            print(f"    Found quantity field in batch: {key} = {value}")
                
                # Check if ingredient_entry itself has quantity info
                for key, value in ingredient_entry.items():
                    if any(qty_word in key.lower() for qty_word in ['amount', 'quantity', 'size', 'value', 'depleted']):
                        print(f"  Found quantity field in ingredient_entry: {key} = {value}")
            
            break  # Just examine first ADDITION action
            
    except Exception as e:
        print(f"Error in debug: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_bakery_system_data()
