#!/usr/bin/env python3
"""
CI/CD API Integration Test - Core functionality verification
Tests essential VectorDB operations for CI/CD pipeline
"""

import requests
import json
import time
import sys
import random

BASE_URL = "http://localhost:8888/api"

class CIAPITest:
    def __init__(self):
        self.base_url = BASE_URL
        self.db_name = "ci_test_db"
        self.test_passed = 0
        self.test_failed = 0
        
    def log(self, message, success=True):
        status = "✓" if success else "✗"
        print(f"[{status}] {message}")
        
    def test(self, name, method, endpoint, data=None, expected_status=200):
        """Simple test helper"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method == "GET":
                response = requests.get(url)
            elif method == "POST":
                response = requests.post(url, json=data)
            elif method == "DELETE":
                response = requests.delete(url)
            else:
                raise ValueError(f"Unsupported method: {method}")
                
            if response.status_code == expected_status:
                self.log(f"{name} - PASSED", True)
                self.test_passed += 1
                return True
            else:
                self.log(f"{name} - FAILED: Expected {expected_status}, got {response.status_code}", False)
                print(f"    Response: {response.text}")
                self.test_failed += 1
                return False
                
        except Exception as e:
            self.log(f"{name} - ERROR: {str(e)}", False)
            self.test_failed += 1
            return False
            
    def run_ci_tests(self):
        """Run essential CI tests"""
        print("="*50)
        print("  VectorDB CI/CD API Integration Test")
        print("="*50)
        print()
        
        # Test 1: Health check
        if not self.test("Health Check", "GET", "/health"):
            return False
            
        # Test 2: Create database
        if not self.test("Create Database", "POST", "/load", {
            "name": self.db_name,
            "path": f"/tmp/{self.db_name}",
            "vectorScale": 100
        }):
            return False
            
        # Test 3: Create table
        if not self.test("Create Table", "POST", f"/{self.db_name}/schema/tables", {
            "name": "test_table",
            "fields": [
                {"name": "id", "dataType": "INT", "primaryKey": True},
                {"name": "vector", "dataType": "VECTOR_FLOAT", "dimensions": 4}
            ]
        }):
            return False
            
        # Test 4: Insert data
        if not self.test("Insert Data", "POST", f"/{self.db_name}/data/insert", {
            "table": "test_table",
            "data": [
                {"id": 1, "vector": [1.0, 2.0, 3.0, 4.0]},
                {"id": 2, "vector": [2.0, 3.0, 4.0, 5.0]}
            ]
        }):
            return False
            
        # Test 5: Vector search
        if not self.test("Vector Search", "POST", f"/{self.db_name}/data/query", {
            "table": "test_table",
            "queryVector": [1.5, 2.5, 3.5, 4.5],
            "limit": 2
        }):
            return False
            
        # Test 6: Get data
        if not self.test("Get Data", "POST", f"/{self.db_name}/data/get", {
            "table": "test_table",
            "primaryKeys": [1, 2]
        }):
            return False
            
        # Test 7: Configuration
        if not self.test("Get Config", "GET", "/config"):
            return False
            
        # Test 8: Statistics
        if not self.test("Get Statistics", "GET", f"/{self.db_name}/statistics"):
            return False
            
        # Test 9: Clean up
        self.test("Drop Database", "DELETE", f"/{self.db_name}/drop")
        
        # Results
        print()
        print("="*50)
        print("          Test Results")
        print("="*50)
        print(f"Passed: {self.test_passed}")
        print(f"Failed: {self.test_failed}")
        
        if self.test_failed == 0:
            print("✅ All CI tests passed!")
            return True
        else:
            print("❌ Some CI tests failed!")
            return False

def main():
    # Quick health check
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print("❌ Server health check failed")
            sys.exit(1)
    except:
        print("❌ Cannot connect to server")
        sys.exit(1)
        
    # Run tests
    test = CIAPITest()
    success = test.run_ci_tests()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()