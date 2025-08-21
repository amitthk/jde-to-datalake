#!/usr/bin/env python3
"""
Startup script for the FastAPI server that ensures environment variables are loaded
"""
import os
import sys
from dotenv import load_dotenv

# Load environment variables before importing the app
load_dotenv()

# Verify critical environment variables are set
required_vars = [
    'JDE_CARDEX_CHANGES_TO_BAKERYOPS_URL',
    'JDE_CARDEX_USERNAME', 
    'JDE_CARDEX_PASSWORD',
    'OUTLET_ID',
    'BAKERY_SYSTEM_BASE_URL',
    'BAKERY_SYSTEM_API_TOKEN',
    'PG_DATABASE_URL'
]

missing_vars = []
for var in required_vars:
    if not os.getenv(var):
        missing_vars.append(var)

if missing_vars:
    print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
    print("Please check your .env file")
    sys.exit(1)

print("✅ Environment variables loaded successfully")

# Now import and run the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
