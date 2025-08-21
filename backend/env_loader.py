"""
Environment loader utility - ensures environment variables are loaded once at module level
"""
import os
from dotenv import load_dotenv

# Load environment variables once when this module is imported
load_dotenv()

def get_env_var(var_name: str, default=None):
    """Get environment variable with optional default"""
    return os.getenv(var_name, default)

def ensure_env_loaded():
    """Ensure environment variables are loaded - can be called multiple times safely"""
    if not hasattr(ensure_env_loaded, '_loaded'):
        load_dotenv()
        ensure_env_loaded._loaded = True
