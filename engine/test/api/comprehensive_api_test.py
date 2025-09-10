#!/usr/bin/env python3
"""
Comprehensive API Test Suite for Epsilla VectorDB
Tests all API endpoints with various scenarios
"""

import requests
import json
import time
import sys
import random
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# Configuration
BASE_URL = "http://localhost:8888/api"
TEST_DB_NAME = "test_api_db"
TEST_DB_PATH = "/tmp/test_api_db"

# Test result tracking
class TestStatus(Enum):
    PASS = "✓"
    FAIL = "✗"
    SKIP = "○"
    ERROR = "⚠"

@dataclass
class TestResult:
    name: str
    endpoint: str
    method: str
    status: TestStatus
    message: str = ""
    duration_ms: float = 0
    
class APITestSuite:
    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url
        self.results: List[TestResult] = []
        self.db_name = TEST_DB_NAME
        self.db_path = TEST_DB_PATH
        self.headers = {"Content-Type": "application/json"}
        
    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        prefix = {
            "INFO": "ℹ",
            "SUCCESS": "✓",
            "ERROR": "✗",
            "WARNING": "⚠"
        }.get(level, "•")
        print(f"[{timestamp}] {prefix} {message}")
        
    def add_result(self, result: TestResult):
        self.results.append(result)
        status_color = {
            TestStatus.PASS: "\033[92m",  # Green
            TestStatus.FAIL: "\033[91m",  # Red
            TestStatus.ERROR: "\033[93m", # Yellow
            TestStatus.SKIP: "\033[90m"   # Gray
        }.get(result.status, "")
        reset_color = "\033[0m"
        
        print(f"{status_color}{result.status.value}{reset_color} {result.name} "
              f"({result.duration_ms:.2f}ms) {result.message}")
        
    def make_request(self, method: str, endpoint: str, 
                    data: Optional[Dict] = None, 
                    params: Optional[Dict] = None) -> Tuple[int, Any]:
        """Make HTTP request and return status code and response"""
        url = f"{self.base_url}{endpoint}"
        start_time = time.time()
        
        try:
            if method == "GET":
                response = requests.get(url, params=params, headers=self.headers)
            elif method == "POST":
                response = requests.post(url, json=data, headers=self.headers)
            elif method == "PUT":
                response = requests.put(url, json=data, headers=self.headers)
            elif method == "DELETE":
                response = requests.delete(url, headers=self.headers)
            else:
                raise ValueError(f"Unsupported method: {method}")
                
            duration_ms = (time.time() - start_time) * 1000
            
            try:
                json_response = response.json() if response.text else {}
            except:
                json_response = response.text
                
            return response.status_code, json_response, duration_ms
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return -1, str(e), duration_ms
            
    def test_endpoint(self, name: str, method: str, endpoint: str,
                     data: Optional[Dict] = None,
                     params: Optional[Dict] = None,
                     expected_status: int = 200) -> bool:
        """Test a single endpoint"""
        status_code, response, duration_ms = self.make_request(method, endpoint, data, params)
        
        if status_code == expected_status:
            self.add_result(TestResult(
                name=name,
                endpoint=endpoint,
                method=method,
                status=TestStatus.PASS,
                duration_ms=duration_ms
            ))
            return True
        else:
            self.add_result(TestResult(
                name=name,
                endpoint=endpoint,
                method=method,
                status=TestStatus.FAIL,
                message=f"Expected {expected_status}, got {status_code}: {response}",
                duration_ms=duration_ms
            ))
            return False
            
    # ========== Database Management Tests ==========
    def test_database_operations(self):
        """Test database load, unload, release, drop operations"""
        self.log("Testing Database Management APIs", "INFO")
        
        # Clean up any existing test database
        self.test_endpoint(
            "Drop existing test DB",
            "DELETE", f"/{self.db_name}/drop",
            expected_status=200
        )
        
        # Test load/create database
        success = self.test_endpoint(
            "Load/Create database",
            "POST", "/load",
            data={
                "name": self.db_name,
                "path": self.db_path,
                "vectorScale": 1000,
                "walEnabled": True
            }
        )
        
        if not success:
            return False
            
        # Test database statistics
        self.test_endpoint(
            "Get database statistics",
            "GET", f"/{self.db_name}/statistics"
        )
        
        # Test unload database
        self.test_endpoint(
            "Unload database",
            "POST", f"/{self.db_name}/unload"
        )
        
        # Reload for further tests
        self.test_endpoint(
            "Reload database",
            "POST", "/load",
            data={
                "name": self.db_name,
                "path": self.db_path,
                "vectorScale": 1000
            }
        )
        
        return True
        
    # ========== Table Schema Tests ==========
    def test_table_operations(self):
        """Test table creation, listing, dropping"""
        self.log("Testing Table Schema APIs", "INFO")
        
        # Create table with various field types
        table_schema = {
            "name": "test_table",
            "fields": [
                {"name": "id", "dataType": "INT", "primaryKey": True},
                {"name": "name", "dataType": "STRING"},
                {"name": "active", "dataType": "BOOL"},
                {"name": "embedding", "dataType": "VECTOR_FLOAT", "dimensions": 128},
                {"name": "sparse_vec", "dataType": "SPARSE_VECTOR_FLOAT", "dimensions": 1000}
            ]
        }
        
        success = self.test_endpoint(
            "Create table",
            "POST", f"/{self.db_name}/schema/tables",
            data=table_schema
        )
        
        if not success:
            return False
            
        # List tables
        self.test_endpoint(
            "List tables",
            "GET", f"/{self.db_name}/schema/tables"
        )
        
        # Show tables (alternative endpoint)
        self.test_endpoint(
            "Show tables",
            "GET", f"/{self.db_name}/schema/tables/show"
        )
        
        # Create another table for multi-table tests
        self.test_endpoint(
            "Create second table",
            "POST", f"/{self.db_name}/schema/tables",
            data={
                "name": "test_table_2",
                "fields": [
                    {"name": "id", "dataType": "INT", "primaryKey": True},
                    {"name": "data", "dataType": "STRING"}
                ]
            }
        )
        
        return True
        
    # ========== Data CRUD Tests ==========
    def test_data_operations(self):
        """Test insert, update, delete, get operations"""
        self.log("Testing Data CRUD APIs", "INFO")
        
        # Generate test data
        test_records = []
        for i in range(10):
            test_records.append({
                "id": i,
                "name": f"Item {i}",
                "active": i % 2 == 0,
                "embedding": [random.random() for _ in range(128)],
                "sparse_vec": {
                    "indices": [j for j in range(0, 100, 10)],
                    "values": [random.random() for _ in range(10)]
                }
            })
        
        # Test insert
        success = self.test_endpoint(
            "Insert records",
            "POST", f"/{self.db_name}/data/insert",
            data={
                "table": "test_table",
                "data": test_records
            }
        )
        
        if not success:
            return False
            
        # Test upsert
        updated_record = test_records[0].copy()
        updated_record["name"] = "Updated Item 0"
        self.test_endpoint(
            "Upsert record",
            "POST", f"/{self.db_name}/data/insert",
            data={
                "table": "test_table",
                "data": [updated_record],
                "upsert": True
            }
        )
        
        # Test get by primary keys
        self.test_endpoint(
            "Get records by primary keys",
            "POST", f"/{self.db_name}/data/get",
            data={
                "table": "test_table",
                "primaryKeys": [0, 1, 2],
                "response": ["id", "name", "active"]
            }
        )
        
        # Test get with filter
        self.test_endpoint(
            "Get records with filter",
            "POST", f"/{self.db_name}/data/get",
            data={
                "table": "test_table",
                "filter": "active == true",
                "response": ["id", "name"],
                "limit": 5
            }
        )
        
        # Test delete by primary keys
        self.test_endpoint(
            "Delete by primary keys",
            "POST", f"/{self.db_name}/data/delete",
            data={
                "table": "test_table",
                "primaryKeys": [8, 9]
            }
        )
        
        # Test delete with filter
        self.test_endpoint(
            "Delete with filter",
            "POST", f"/{self.db_name}/data/delete",
            data={
                "table": "test_table",
                "filter": "id > 6"
            }
        )
        
        # Get record count
        self.test_endpoint(
            "Get record count",
            "GET", f"/{self.db_name}/records/count",
            params={"table": "test_table"}
        )
        
        return True
        
    # ========== Vector Search Tests ==========
    def test_vector_search(self):
        """Test vector similarity search"""
        self.log("Testing Vector Search APIs", "INFO")
        
        # Insert more vectors for search
        search_records = []
        for i in range(20):
            # Create vectors with some pattern
            base_value = i * 0.1
            search_records.append({
                "id": 100 + i,
                "name": f"Search Item {i}",
                "active": True,
                "embedding": [base_value + random.random() * 0.1 for _ in range(128)]
            })
        
        self.test_endpoint(
            "Insert search test data",
            "POST", f"/{self.db_name}/data/insert",
            data={
                "table": "test_table",
                "data": search_records
            }
        )
        
        # Test vector search with query vector
        query_vector = [0.5 + random.random() * 0.1 for _ in range(128)]
        success = self.test_endpoint(
            "Vector similarity search",
            "POST", f"/{self.db_name}/data/query",
            data={
                "table": "test_table",
                "queryVector": query_vector,
                "limit": 5,
                "withDistance": True,
                "response": ["id", "name"]
            }
        )
        
        # Test search with filter
        self.test_endpoint(
            "Vector search with filter",
            "POST", f"/{self.db_name}/data/query",
            data={
                "table": "test_table",
                "queryVector": query_vector,
                "limit": 3,
                "filter": "id >= 100",
                "withDistance": True
            }
        )
        
        # Test search with facets
        self.test_endpoint(
            "Vector search with facets",
            "POST", f"/{self.db_name}/data/query",
            data={
                "table": "test_table",
                "queryVector": query_vector,
                "limit": 10,
                "facets": {
                    "active": {
                        "type": "term"
                    }
                }
            }
        )
        
        return success
        
    # ========== Configuration Management Tests ==========
    def test_configuration(self):
        """Test configuration get/set operations"""
        self.log("Testing Configuration APIs", "INFO")
        
        # Get all configuration
        self.test_endpoint(
            "Get all configuration",
            "GET", "/config"
        )
        
        # Get specific config value
        self.test_endpoint(
            "Get IntraQueryThreads config",
            "GET", "/config/IntraQueryThreads"
        )
        
        # Set configuration value
        self.test_endpoint(
            "Set SearchQueueSize",
            "PUT", "/config/SearchQueueSize",
            data={"value": "1000"}
        )
        
        # Batch update configuration
        self.test_endpoint(
            "Batch update config",
            "POST", "/config/batch",
            data={
                "IntraQueryThreads": 4,
                "RebuildThreads": 2,
                "WALFlushInterval": 20,
                "WALAutoFlush": True
            }
        )
        
        # Get configuration schema
        self.test_endpoint(
            "Get config schema",
            "GET", "/config/schema"
        )
        
        return True
        
    # ========== System Operations Tests ==========
    def test_system_operations(self):
        """Test rebuild, compact, and other system operations"""
        self.log("Testing System Operations APIs", "INFO")
        
        # Test rebuild
        self.test_endpoint(
            "Rebuild indices",
            "POST", "/rebuild"
        )
        
        # Test compact for specific database
        self.test_endpoint(
            "Compact database",
            "POST", f"/{self.db_name}/compact",
            data={
                "threshold": 0.3
            }
        )
        
        # Test compact specific table
        self.test_endpoint(
            "Compact table",
            "POST", f"/{self.db_name}/compact",
            data={
                "tableName": "test_table",
                "threshold": 0.2
            }
        )
        
        # Test global compact
        self.test_endpoint(
            "Global compact",
            "POST", "/compact",
            data={
                "threshold": 0.3
            }
        )
        
        return True
        
    # ========== Health and Monitoring Tests ==========
    def test_health_monitoring(self):
        """Test health check and monitoring endpoints"""
        self.log("Testing Health & Monitoring APIs", "INFO")
        
        # Root endpoint
        self.test_endpoint(
            "Root welcome message",
            "GET", ""
        )
        
        # Health check
        self.test_endpoint(
            "Health check",
            "GET", "/health"
        )
        
        # Server state
        self.test_endpoint(
            "Server state",
            "GET", "/state"
        )
        
        return True
        
    # ========== Advanced Features Tests ==========
    def test_advanced_features(self):
        """Test auto-embeddings, indices, and other advanced features"""
        self.log("Testing Advanced Features", "INFO")
        
        # Create table with auto-embeddings
        advanced_table = {
            "name": "advanced_table",
            "fields": [
                {"name": "id", "dataType": "INT", "primaryKey": True},
                {"name": "text", "dataType": "STRING"},
                {"name": "text_embedding", "dataType": "VECTOR_FLOAT", "dimensions": 768}
            ],
            "autoEmbeddings": [
                {
                    "sourceField": "text",
                    "targetField": "text_embedding",
                    "model": "BAAI/bge-base-en-v1.5"
                }
            ]
        }
        
        success = self.test_endpoint(
            "Create table with auto-embeddings",
            "POST", f"/{self.db_name}/schema/tables",
            data=advanced_table
        )
        
        if success:
            # Test search by content (requires embedding service)
            self.test_endpoint(
                "Search by text content",
                "POST", f"/{self.db_name}/data/query",
                data={
                    "table": "advanced_table",
                    "query": "sample search text",
                    "queryField": "text_embedding",
                    "limit": 5
                }
            )
        
        return True
        
    # ========== Error Handling Tests ==========
    def test_error_handling(self):
        """Test API error handling with invalid requests"""
        self.log("Testing Error Handling", "INFO")
        
        # Non-existent database
        self.test_endpoint(
            "Query non-existent database",
            "GET", "/nonexistent_db/statistics",
            expected_status=404
        )
        
        # Invalid table schema
        self.test_endpoint(
            "Create table with invalid schema",
            "POST", f"/{self.db_name}/schema/tables",
            data={
                "name": "invalid_table"
                # Missing required fields
            },
            expected_status=400
        )
        
        # Invalid filter syntax
        self.test_endpoint(
            "Query with invalid filter",
            "POST", f"/{self.db_name}/data/get",
            data={
                "table": "test_table",
                "filter": "invalid syntax @#$"
            },
            expected_status=400
        )
        
        # Duplicate primary key
        self.test_endpoint(
            "Insert duplicate primary key",
            "POST", f"/{self.db_name}/data/insert",
            data={
                "table": "test_table",
                "data": [{"id": 100, "name": "Duplicate"}]
            },
            expected_status=400
        )
        
        return True
        
    # ========== Performance Tests ==========
    def test_performance(self):
        """Test API performance with bulk operations"""
        self.log("Testing Performance", "INFO")
        
        # Bulk insert test
        bulk_size = 1000
        bulk_records = []
        for i in range(bulk_size):
            bulk_records.append({
                "id": 10000 + i,
                "name": f"Bulk Item {i}",
                "active": True,
                "embedding": [random.random() for _ in range(128)]
            })
        
        start_time = time.time()
        success = self.test_endpoint(
            f"Bulk insert {bulk_size} records",
            "POST", f"/{self.db_name}/data/insert",
            data={
                "table": "test_table",
                "data": bulk_records
            }
        )
        insert_time = time.time() - start_time
        
        if success:
            self.log(f"Bulk insert rate: {bulk_size/insert_time:.2f} records/sec", "SUCCESS")
        
        # Bulk search test
        search_iterations = 10
        start_time = time.time()
        for _ in range(search_iterations):
            query_vector = [random.random() for _ in range(128)]
            self.make_request(
                "POST", f"/{self.db_name}/data/query",
                data={
                    "table": "test_table",
                    "queryVector": query_vector,
                    "limit": 10
                }
            )
        search_time = time.time() - start_time
        self.log(f"Search QPS: {search_iterations/search_time:.2f} queries/sec", "SUCCESS")
        
        return success
        
    # ========== Cleanup ==========
    def cleanup(self):
        """Clean up test data"""
        self.log("Cleaning up test data", "INFO")
        
        # Drop test tables
        self.test_endpoint(
            "Drop test_table",
            "DELETE", f"/{self.db_name}/schema/tables/test_table"
        )
        
        self.test_endpoint(
            "Drop test_table_2",
            "DELETE", f"/{self.db_name}/schema/tables/test_table_2"
        )
        
        self.test_endpoint(
            "Drop advanced_table",
            "DELETE", f"/{self.db_name}/schema/tables/advanced_table"
        )
        
        # Drop test database
        self.test_endpoint(
            "Drop test database",
            "DELETE", f"/{self.db_name}/drop"
        )
        
    # ========== Main Test Runner ==========
    def run_all_tests(self):
        """Run all test suites"""
        print("\n" + "="*60)
        print("       Epsilla VectorDB Comprehensive API Test Suite")
        print("="*60 + "\n")
        
        test_suites = [
            ("Database Management", self.test_database_operations),
            ("Table Operations", self.test_table_operations),
            ("Data CRUD", self.test_data_operations),
            ("Vector Search", self.test_vector_search),
            ("Configuration", self.test_configuration),
            ("System Operations", self.test_system_operations),
            ("Health Monitoring", self.test_health_monitoring),
            ("Advanced Features", self.test_advanced_features),
            ("Error Handling", self.test_error_handling),
            ("Performance", self.test_performance)
        ]
        
        for suite_name, test_func in test_suites:
            print(f"\n{'='*40}")
            print(f"  {suite_name}")
            print(f"{'='*40}")
            
            try:
                test_func()
            except Exception as e:
                self.log(f"Suite failed: {str(e)}", "ERROR")
                traceback.print_exc()
                
        # Cleanup
        self.cleanup()
        
        # Generate report
        self.generate_report()
        
    def generate_report(self):
        """Generate test report"""
        print("\n" + "="*60)
        print("                    Test Report")
        print("="*60 + "\n")
        
        # Count results by status
        status_counts = {status: 0 for status in TestStatus}
        total_duration = 0
        
        for result in self.results:
            status_counts[result.status] += 1
            total_duration += result.duration_ms
            
        total_tests = len(self.results)
        pass_rate = (status_counts[TestStatus.PASS] / total_tests * 100) if total_tests > 0 else 0
        
        # Summary
        print("Summary:")
        print(f"  Total Tests: {total_tests}")
        print(f"  Passed: {status_counts[TestStatus.PASS]} ({pass_rate:.1f}%)")
        print(f"  Failed: {status_counts[TestStatus.FAIL]}")
        print(f"  Errors: {status_counts[TestStatus.ERROR]}")
        print(f"  Skipped: {status_counts[TestStatus.SKIP]}")
        print(f"  Total Duration: {total_duration:.2f}ms")
        print(f"  Average Duration: {total_duration/total_tests:.2f}ms" if total_tests > 0 else "N/A")
        
        # Failed tests details
        failed_tests = [r for r in self.results if r.status in [TestStatus.FAIL, TestStatus.ERROR]]
        if failed_tests:
            print("\nFailed Tests:")
            for result in failed_tests:
                print(f"  {result.status.value} {result.name}")
                print(f"    Endpoint: {result.method} {result.endpoint}")
                if result.message:
                    print(f"    Message: {result.message}")
                    
        # Performance metrics
        print("\nPerformance Metrics:")
        slow_tests = sorted([r for r in self.results if r.duration_ms > 100], 
                           key=lambda x: x.duration_ms, reverse=True)[:5]
        if slow_tests:
            print("  Slowest Operations:")
            for result in slow_tests:
                print(f"    {result.name}: {result.duration_ms:.2f}ms")
                
        # Overall result
        print("\n" + "="*60)
        if pass_rate >= 95:
            print("✅ TEST SUITE PASSED")
        elif pass_rate >= 80:
            print("⚠️  TEST SUITE PASSED WITH WARNINGS")
        else:
            print("❌ TEST SUITE FAILED")
        print("="*60 + "\n")
        
        # Save report to file
        with open("api_test_report.json", "w") as f:
            report_data = {
                "timestamp": datetime.now().isoformat(),
                "summary": {
                    "total": total_tests,
                    "passed": status_counts[TestStatus.PASS],
                    "failed": status_counts[TestStatus.FAIL],
                    "errors": status_counts[TestStatus.ERROR],
                    "skipped": status_counts[TestStatus.SKIP],
                    "pass_rate": pass_rate,
                    "total_duration_ms": total_duration
                },
                "results": [
                    {
                        "name": r.name,
                        "endpoint": r.endpoint,
                        "method": r.method,
                        "status": r.status.name,
                        "message": r.message,
                        "duration_ms": r.duration_ms
                    }
                    for r in self.results
                ]
            }
            json.dump(report_data, f, indent=2)
            print(f"Report saved to api_test_report.json")

def main():
    # Check if server is running
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=2)
        if response.status_code != 200:
            print("❌ VectorDB server is not healthy")
            sys.exit(1)
    except:
        print("❌ VectorDB server is not running at", BASE_URL)
        print("Please start the server with: ./vectordb")
        sys.exit(1)
        
    # Run tests
    test_suite = APITestSuite()
    test_suite.run_all_tests()

if __name__ == "__main__":
    main()