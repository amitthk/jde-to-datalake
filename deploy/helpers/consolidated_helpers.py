#!/usr/bin/env python3
"""
Consolidated Helper Script Manager
=================================

This script consolidates all helper scripts into a single, comprehensive tool.
It provides unified access to all helper functions across the system.

Usage:
    python consolidated_helpers.py [--helper=HELPER] [--action=ACTION] [options...]

Helpers Available:
    jde               - JDE system integration functions
    bakery_system     - Bakery system API interactions  
    bakery_ops        - Bakery operations helper functions
    session           - Session management functions
    s3                - S3 data lake operations
    utility           - General utility functions
    
Actions:
    list              - List available functions in a helper
    test              - Run basic connectivity tests
    fetch_data        - Fetch data using the helper
    info              - Show helper information

Examples:
    python consolidated_helpers.py --helper=jde --action=test
    python consolidated_helpers.py --helper=bakery_system --action=fetch_data  
    python consolidated_helpers.py --helper=s3 --action=list
    python consolidated_helpers.py --action=info                      # Show all helpers info

Options:
    --dry-run         - Show what would be done without executing
    --verbose         - Enable verbose output
    --config=FILE     - Use custom configuration file
"""

import os
import sys
import argparse
import importlib.util
from pathlib import Path
from datetime import datetime

class HelperManager:
    def __init__(self, verbose=False, dry_run=False):
        self.verbose = verbose
        self.dry_run = dry_run
        self.current_dir = Path(__file__).parent.parent
        self.backend_dir = self.current_dir / 'backend'
        self.helpers_dir = Path(__file__).parent
        
        # Define available helpers and their descriptions
        self.helpers = {
            'jde': {
                'file': 'jde_helper.py',
                'description': 'JDE system integration functions',
                'main_functions': [
                    'fetch_existing_ingredient', 'create_new_ingredient', 
                    'fetch_existing_ingredient_batch', 'submit_ingredient_batch_action'
                ]
            },
            'bakery_system': {
                'file': 'bakery_helper.py',
                'description': 'Bakery system API interactions',
                'main_functions': [
                    'get_data_from_bakery_system', 'fetch_existing_ingredient_by_id',
                    'get_streamlined_action_data', 'process_api_data'
                ]
            },
            'bakery_ops': {
                'file': 'bakery_ops_helper.py',
                'description': 'Bakery operations helper functions',
                'main_functions': [
                    'process_bakery_operations', 'validate_operations',
                    'sync_operations_data'
                ]
            },
            'session': {
                'file': 'session_helper.py',
                'description': 'Session management functions',
                'main_functions': [
                    'create_session', 'get_session', 'update_session', 'cleanup_sessions'
                ]
            },
            's3': {
                'file': 's3_helper.py',
                'description': 'S3 data lake operations',
                'main_functions': [
                    'upload_to_s3', 'download_from_s3', 'list_s3_objects', 'sync_data_to_s3'
                ]
            },
            'utility': {
                'file': 'utility.py',
                'description': 'General utility functions',
                'main_functions': [
                    'retry_request', 'normalize_quantity_for_transaction_id', 
                    'preserve_quantity_precision', 'validate_environment'
                ]
            }
        }
        
        print(f"🔧 Helper Manager initialized")
        print(f"   Available helpers: {len(self.helpers)}")
        print(f"   Verbose mode: {'Yes' if self.verbose else 'No'}")
        print(f"   Dry run mode: {'Yes' if self.dry_run else 'No'}")
        print()

    def load_helper_module(self, helper_name):
        """Dynamically load a helper module"""
        if helper_name not in self.helpers:
            raise ValueError(f"Unknown helper: {helper_name}")
            
        helper_info = self.helpers[helper_name]
        helper_file = self.backend_dir / helper_info['file']
        
        if not helper_file.exists():
            raise FileNotFoundError(f"Helper file not found: {helper_file}")
            
        # Load the module dynamically
        spec = importlib.util.spec_from_file_location(helper_name, helper_file)
        module = importlib.util.module_from_spec(spec)
        
        # Add backend directory to sys.path for imports
        if str(self.backend_dir) not in sys.path:
            sys.path.insert(0, str(self.backend_dir))
            
        spec.loader.exec_module(module)
        return module

    def list_helper_functions(self, helper_name):
        """List all available functions in a helper"""
        print(f"📋 Functions in '{helper_name}' helper:")
        print(f"   File: {self.helpers[helper_name]['file']}")
        print(f"   Description: {self.helpers[helper_name]['description']}")
        print()
        
        try:
            module = self.load_helper_module(helper_name)
            
            # Get all callable functions from the module
            functions = [name for name in dir(module) 
                        if callable(getattr(module, name)) and not name.startswith('_')]
            
            print(f"📝 Available functions ({len(functions)}):")
            for func_name in sorted(functions):
                func = getattr(module, func_name)
                doc = func.__doc__.split('\n')[0] if func.__doc__ else "No description"
                print(f"   • {func_name:30} - {doc}")
                
            print()
            print(f"🌟 Main functions:")
            for main_func in self.helpers[helper_name]['main_functions']:
                if main_func in functions:
                    func = getattr(module, main_func)
                    doc = func.__doc__.split('\n')[0] if func.__doc__ else "No description"
                    print(f"   ⭐ {main_func:30} - {doc}")
                    
        except Exception as e:
            print(f"❌ Error loading helper '{helper_name}': {e}")

    def test_helper_connectivity(self, helper_name):
        """Test basic connectivity for a helper"""
        print(f"🧪 Testing '{helper_name}' helper connectivity...")
        
        if self.dry_run:
            print("   🔍 DRY RUN: Would test connectivity")
            return True
            
        try:
            module = self.load_helper_module(helper_name)
            
            # Run helper-specific tests
            if helper_name == 'jde':
                # Test JDE helper
                print("   Testing JDE database connection...")
                if hasattr(module, 'get_db_connection'):
                    conn = module.get_db_connection()
                    conn.close()
                    print("   ✅ JDE database connection successful")
                else:
                    print("   ⚠️  JDE database connection function not found")
                    
            elif helper_name == 'bakery_system':
                # Test Bakery system API
                print("   Testing Bakery System API connectivity...")
                if hasattr(module, 'get_data_from_bakery_system'):
                    # Just check if we can load environment variables
                    from dotenv import load_dotenv
                    load_dotenv()
                    outlet_id = os.getenv("OUTLET_ID")
                    bakery_system_base_url = os.getenv("BAKERY_SYSTEM_BASE_URL")
                    if outlet_id and bakery_system_base_url:
                        print("   ✅ Bakery System configuration available")
                    else:
                        print("   ⚠️  Bakery System configuration missing")
                else:
                    print("   ⚠️  Bakery System function not found")
                    
            elif helper_name == 's3':
                # Test S3 connectivity
                print("   Testing S3 connectivity...")
                if hasattr(module, 'list_s3_objects'):
                    print("   ✅ S3 functions available")
                else:
                    print("   ⚠️  S3 functions not found")
                    
            elif helper_name == 'session':
                # Test session management
                print("   Testing session management...")
                if hasattr(module, 'create_session'):
                    print("   ✅ Session functions available")
                else:
                    print("   ⚠️  Session functions not found")
                    
            elif helper_name == 'utility':
                # Test utility functions
                print("   Testing utility functions...")
                if hasattr(module, 'retry_request'):
                    print("   ✅ Utility functions available")
                else:
                    print("   ⚠️  Utility functions not found")
                    
            else:
                print(f"   ✅ Helper module '{helper_name}' loaded successfully")
                
            return True
            
        except Exception as e:
            print(f"   ❌ Error testing helper '{helper_name}': {e}")
            return False

    def show_all_helpers_info(self):
        """Show information about all available helpers"""
        print("📚 AVAILABLE HELPERS")
        print("="*60)
        
        for helper_name, info in self.helpers.items():
            print(f"\n🔧 {helper_name.upper()}")
            print(f"   File: {info['file']}")
            print(f"   Description: {info['description']}")
            print(f"   Main Functions:")
            for func in info['main_functions']:
                print(f"     • {func}")
                
    def run_helper_action(self, helper_name, action, **kwargs):
        """Run a specific action on a helper"""
        actions = {
            'list': self.list_helper_functions,
            'test': self.test_helper_connectivity,
            'info': lambda h: print(f"Helper: {h}\nDescription: {self.helpers[h]['description']}")
        }
        
        if action not in actions:
            print(f"❌ Unknown action: {action}")
            print(f"Available actions: {list(actions.keys())}")
            return False
            
        try:
            if action in ['list', 'test', 'info']:
                return actions[action](helper_name)
            else:
                # For other actions, load the module and try to find the function
                module = self.load_helper_module(helper_name)
                # Implementation would depend on specific action requirements
                print(f"⚠️  Action '{action}' not yet implemented for helper '{helper_name}'")
                return True
                
        except Exception as e:
            print(f"❌ Error running action '{action}' on helper '{helper_name}': {e}")
            return False

def main():
    parser = argparse.ArgumentParser(
        description="Consolidated Helper Script Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--helper',
        choices=['jde', 'bakery_system', 'bakery_ops', 'session', 's3', 'utility'],
        help='Helper to use'
    )
    
    parser.add_argument(
        '--action',
        choices=['list', 'test', 'fetch_data', 'info'],
        default='info',
        help='Action to perform (default: info)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview operations without executing them'
    )
    
    args = parser.parse_args()
    
    print("🔧 CONSOLIDATED HELPER SCRIPT MANAGER")
    print("="*50)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Helper: {args.helper or 'All'}")
    print(f"Action: {args.action}")
    print()
    
    try:
        manager = HelperManager(verbose=args.verbose, dry_run=args.dry_run)
        
        if args.helper:
            success = manager.run_helper_action(args.helper, args.action)
        else:
            if args.action == 'info':
                manager.show_all_helpers_info()
                success = True
            else:
                print("❌ Please specify a helper when using actions other than 'info'")
                success = False
        
        if success:
            print(f"\n✅ Operation completed successfully!")
            exit(0)
        else:
            print(f"\n❌ Operation failed!")
            exit(1)
            
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        exit(1)

if __name__ == "__main__":
    main()
