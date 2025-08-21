#!/usr/bin/env python3
"""
Test script to debug JDE Item Master data structure
"""

import sys
import os
import json
from pathlib import Path
import pandas as pd

# Add the backend directory to the path so we can import modules
sys.path.append(str(Path(__file__).parent))

from jde_helper import get_jde_item_master
from dotenv import load_dotenv
from datetime import datetime, timedelta

def test_jde_item_master_structure():
    """Test the JDE Item Master API to see what data structure we actually get"""
    load_dotenv()
    
    # Use the same parameters as in the main endpoint
    today = datetime.now()
    yesterday = today - timedelta(days=30)
    date_str = yesterday.strftime('%d/%m/%Y')
    bu = os.getenv('JDE_BUSINESS_UNIT', '1110')
    gl_cat = 'WA01'
    
    print(f"Testing JDE Item Master with parameters:")
    print(f"  bu: {bu}")
    print(f"  date_str: {date_str}")
    print(f"  gl_cat: {gl_cat}")
    print("-" * 50)
    
    try:
        # Call the JDE API
        jde_data = get_jde_item_master(bu, date_str, gl_cat)
        
        if not jde_data:
            print("❌ No data returned from JDE API")
            return
        
        print("✅ JDE API returned data")
        print(f"Top-level keys: {list(jde_data.keys())}")
        
        if 'ServiceRequest1' in jde_data:
            service_request = jde_data['ServiceRequest1']
            print(f"ServiceRequest1 keys: {list(service_request.keys())}")
            
            # Check different possible data structure paths
            possible_paths = [
                'fs_DATABROWSE_V564102A',
                'fs_DATABROWSE_F4101',
                'fs_DATABROWSE_V4101',
                'data'
            ]
            
            for path in possible_paths:
                if path in service_request:
                    print(f"✅ Found data path: {path}")
                    try:
                        data_section = service_request[path]
                        if isinstance(data_section, dict) and 'data' in data_section:
                            grid_data = data_section['data']
                            if isinstance(grid_data, dict) and 'gridData' in grid_data:
                                rowset = grid_data['gridData'].get('rowset', [])
                                print(f"Found {len(rowset)} rows of data")
                                
                                if rowset:
                                    # Create DataFrame and show structure
                                    df = pd.DataFrame(rowset)
                                    print(f"Available columns: {list(df.columns)}")
                                    print(f"First row sample:")
                                    for col, val in df.iloc[0].items():
                                        print(f"  {col}: {val}")
                                    
                                    # Check for the fields we need
                                    required_fields = ['F4101_ITM', 'F4101_LITM', 'F4101_DSC1', 'F4101_UOM1']
                                    print(f"\nField availability check:")
                                    for field in required_fields:
                                        if field in df.columns:
                                            print(f"  ✅ {field}: Available")
                                        else:
                                            print(f"  ❌ {field}: Missing")
                                
                                return
                                
                    except Exception as e:
                        print(f"❌ Error processing {path}: {e}")
            
            print("❌ Could not find expected data structure")
        else:
            print("❌ No ServiceRequest1 key found")
        
        # Print full response for debugging
        print("\nFull response structure:")
        print(json.dumps(jde_data, indent=2, default=str))
        
    except Exception as e:
        print(f"❌ Error testing JDE Item Master: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_jde_item_master_structure()
