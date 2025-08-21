#!/usr/bin/env python3
"""
Test script to verify the unique_transaction_id functionality is working correctly.
"""

import sys
import os
from pathlib import Path

# Add the backend directory to the Python path
backend_dir = Path(__file__).parent
sys.path.append(str(backend_dir))

def test_unique_transaction_id():
    """Test the unique transaction ID functionality"""
    print("Testing unique_transaction_id functionality...")
    
    # Test data
    test_batch_data = {
        'action_id': 'test_action_123',
        'ingredient_id': 'test_ingredient_456', 
        'ingredient_name': 'TestProduct',
        'batch_id': 'test_batch_789',
        'batch_number': 'TestProduct_LOT123',
        'lot_number': 'LOT123',
        'quantity': 5.0,
        'unit': 'L',
        'vessel_code': 'V001'
    }
    
    # Expected unique transaction ID
    expected_unique_id = f"{test_batch_data['ingredient_name']}_{test_batch_data['lot_number']}_{test_batch_data['vessel_code']}"
    print(f"Expected unique transaction ID: {expected_unique_id}")
    
    # Test the prepare_jde_payload function
    try:
        from jde_helper import prepare_jde_payload
        
        result = prepare_jde_payload(test_batch_data)
        
        if result.get('success'):
            print("✅ prepare_jde_payload test passed")
            print(f"Generated JDE payload: {result['jde_payload']['Explanation']}")
        else:
            print(f"❌ prepare_jde_payload failed: {result.get('error')}")
            
    except Exception as e:
        print(f"❌ Error testing prepare_jde_payload: {e}")
    
    # Test the streamlined action data
    try:
        from backend.bakery_helper import get_streamlined_action_data
        
        print("\nTesting streamlined action data...")
        # This will test with default date range
        batches = get_streamlined_action_data()
        
        if batches:
            print(f"✅ Found {len(batches)} batch records")
            
            # Check if unique_transaction_id is in the records
            sample_batch = batches[0] if batches else {}
            if 'unique_transaction_id' in sample_batch:
                print(f"✅ unique_transaction_id field present: {sample_batch['unique_transaction_id']}")
            else:
                print("❌ unique_transaction_id field missing from batch records")
        else:
            print("ℹ️  No batch records found (this might be normal if there's no recent data)")
            
    except Exception as e:
        print(f"❌ Error testing streamlined action data: {e}")

    print("\nTest completed!")

if __name__ == "__main__":
    test_unique_transaction_id()
