# VectorDB API Testing Guide

This guide covers the comprehensive API testing suite for Epsilla VectorDB.

## Test Files Overview

### 1. Comprehensive API Test (`test/api/comprehensive_api_test.py`)
Full-featured API test suite covering all endpoints:
- Database management (load, unload, drop, statistics)
- Table schema operations (create, list, drop)
- Data CRUD operations (insert, upsert, get, delete)
- Vector similarity search
- Configuration management
- System operations (rebuild, compact)
- Health monitoring
- Advanced features (auto-embeddings, indices)
- Error handling
- Performance benchmarking

**Usage:**
```bash
# Run comprehensive tests
./run_api_tests.sh

# Or run directly
python3 test/api/comprehensive_api_test.py
```

### 2. CI/CD Integration Test (`test/api/ci_api_test.py`)
Lightweight test for CI/CD pipeline:
- Essential functionality verification
- Health check
- Basic CRUD operations
- Vector search
- Fast execution (~30 seconds)

**Usage:**
```bash
python3 test/api/ci_api_test.py
```

### 3. API Test Runner (`run_api_tests.sh`)
Shell script wrapper for comprehensive testing:
- Automatic server startup/shutdown
- Environment setup
- Log management
- Report generation

## API Endpoints Tested

### Core Database APIs
- `POST /api/load` - Create/load database
- `POST /api/{db}/unload` - Unload database
- `DELETE /api/{db}/drop` - Drop database
- `GET /api/{db}/statistics` - Database statistics

### Table Management APIs
- `POST /api/{db}/schema/tables` - Create table
- `GET /api/{db}/schema/tables` - List tables
- `DELETE /api/{db}/schema/tables/{table}` - Drop table

### Data Operations APIs
- `POST /api/{db}/data/insert` - Insert/upsert records
- `POST /api/{db}/data/get` - Query records
- `POST /api/{db}/data/query` - Vector similarity search
- `POST /api/{db}/data/delete` - Delete records
- `GET /api/{db}/records/count` - Record count

### Configuration APIs
- `GET /api/config` - Get all configuration
- `GET /api/config/{key}` - Get specific config
- `PUT /api/config/{key}` - Set config value
- `POST /api/config/batch` - Batch update config

### System APIs
- `POST /api/rebuild` - Rebuild indices
- `POST /api/{db}/compact` - Compact database
- `GET /api/health` - Health check
- `GET /api/state` - Server state

## Test Features

### Data Types Tested
- **Integers**: Primary keys, numeric fields
- **Strings**: Text data, names
- **Booleans**: Status flags
- **Dense Vectors**: Float arrays for similarity search
- **Sparse Vectors**: Indexed sparse representations

### Search Capabilities
- **Vector Similarity**: Euclidean distance
- **Filtering**: Boolean expressions
- **Faceting**: Term aggregations
- **Pagination**: Limit/skip parameters

### Error Scenarios
- Invalid database names
- Malformed requests
- Duplicate primary keys
- Non-existent resources
- Invalid filter syntax

### Performance Metrics
- **Insertion Rate**: Records/second
- **Query Latency**: Response time
- **Search QPS**: Queries per second
- **Bulk Operations**: Large dataset handling

## Configuration Options

### Environment Variables
```bash
# WAL settings
export WAL_AUTO_FLUSH=true
export WAL_FLUSH_INTERVAL=30

# Delete mode
export SOFT_DELETE=false

# Server settings
export EPSILLA_INITIAL_CAPACITY=1000
```

### Test Configuration
```python
BASE_URL = "http://localhost:8888/api"
TEST_DB_NAME = "test_api_db"
TEST_DB_PATH = "/tmp/test_api_db"
```

## Running Tests in CI/CD

### GitHub Actions Integration
The CI pipeline includes API tests in the Release build:

```yaml
- name: Run API Integration Tests
  if: matrix.build-type == 'Release'
  run: |
    python3 -m pip install requests
    ./vectordb -d /tmp/ci_vectordb &
    python3 test/api/ci_api_test.py
```

### Local Development
```bash
# Quick test during development
make -j4
./vectordb -d /tmp/test_db &
python3 test/api/ci_api_test.py
kill %1
```

## Test Results and Reporting

### Console Output
```
==================================================
       Epsilla VectorDB Comprehensive API Test Suite
==================================================

========================================
  Database Management
========================================
✓ Load/Create database (45.23ms)
✓ Get database statistics (12.34ms)
✓ Unload database (23.45ms)
```

### JSON Report
```json
{
  "timestamp": "2025-01-15T10:30:00",
  "summary": {
    "total": 45,
    "passed": 43,
    "failed": 2,
    "pass_rate": 95.6,
    "total_duration_ms": 1234.56
  },
  "results": [...]
}
```

## Test Data Management

### Automatic Cleanup
- Tests create temporary databases with unique names
- Automatic cleanup after each test suite
- No interference between test runs

### Test Data Patterns
- **Synthetic Vectors**: Random float arrays for search
- **Realistic Data**: Names, IDs, status flags
- **Edge Cases**: Empty strings, large numbers, special characters

## Performance Benchmarks

### Typical Results
- **Insert Rate**: 1000+ records/second
- **Search Latency**: <50ms for simple queries
- **Search QPS**: 100+ queries/second
- **Bulk Operations**: 10K+ records efficiently

### Performance Factors
- Hardware specifications
- Vector dimensions
- Index configuration
- Concurrent load

## Troubleshooting

### Common Issues

**Server Not Starting**
```bash
# Check if port is available
lsof -i :8888

# Check server logs
tail -f /tmp/vectordb_test/server.log
```

**Test Failures**
```bash
# Run with verbose output
python3 test/api/comprehensive_api_test.py --verbose

# Check specific endpoint
curl -v http://localhost:8888/api/health
```

**Memory Issues**
```bash
# Monitor memory usage
top -p $(pgrep vectordb)

# Adjust test dataset size
BULK_SIZE=100 python3 test/api/comprehensive_api_test.py
```

### Debug Mode
```python
# Enable detailed logging in test script
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Best Practices

### Test Development
1. **Isolated Tests**: Each test should be independent
2. **Cleanup**: Always cleanup test data
3. **Error Handling**: Test both success and failure cases
4. **Performance**: Include timing measurements
5. **Documentation**: Document test purpose and expected behavior

### Production Testing
1. **Load Testing**: Verify performance under load
2. **Stress Testing**: Test system limits
3. **Endurance Testing**: Long-running stability
4. **Recovery Testing**: Server restart scenarios

### Monitoring
1. **Success Rate**: Track test pass/fail rates
2. **Performance Trends**: Monitor response times
3. **Error Patterns**: Identify common failure modes
4. **Resource Usage**: CPU, memory, disk utilization

## Contributing

### Adding New Tests
1. Add test method to appropriate test class
2. Follow naming convention: `test_feature_name`
3. Include error cases and edge conditions
4. Update documentation

### Test Categories
- **Functional**: Core feature verification
- **Integration**: Multi-component interactions  
- **Performance**: Speed and scalability
- **Security**: Error handling and validation
- **Regression**: Previous bug scenarios

This comprehensive API testing suite ensures VectorDB reliability, performance, and correctness across all supported operations.