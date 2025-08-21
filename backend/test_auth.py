#!/usr/bin/env python3
"""
Test script for authentication functionality
"""
import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BASE_URL = "http://localhost:8000"

def test_health():
    """Test health endpoint"""
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"Health check: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

def test_login(username, password):
    """Test login endpoint"""
    try:
        data = {
            "username": username,
            "password": password
        }
        response = requests.post(
            f"{BASE_URL}/token",
            json=data,
            headers={"Content-Type": "application/json"}
        )
        print(f"Login test: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"Login successful! Token: {result.get('access_token', 'N/A')[:50]}...")
            return result.get('access_token')
        else:
            print(f"Login failed: {response.text}")
            return None
    except Exception as e:
        print(f"Login test failed: {e}")
        return None

def test_authenticated_endpoint(token):
    """Test an authenticated endpoint"""
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        response = requests.get(f"{BASE_URL}/data/joined_df3", headers=headers)
        print(f"Authenticated endpoint test: {response.status_code}")
        if response.status_code == 200:
            print("Authenticated request successful!")
            return True
        else:
            print(f"Authenticated request failed: {response.text}")
            return False
    except Exception as e:
        print(f"Authenticated endpoint test failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing authentication system...")
    
    # Test health endpoint
    if not test_health():
        print("Backend is not running or health endpoint failed")
        exit(1)
    
    # Test with development credentials
    print("\nTesting with development credentials...")
    username = input("Enter username (or 'admin' for dev test): ").strip()
    password = input("Enter password (or 'test' for dev test): ").strip()
    
    token = test_login(username, password)
    
    if token:
        print("\nTesting authenticated endpoint...")
        test_authenticated_endpoint(token)
    else:
        print("Authentication failed, cannot test protected endpoints")
