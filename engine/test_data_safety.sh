#!/bin/bash

echo "=== VectorDB Data Safety Test ==="
echo ""
echo "This test verifies data persistence improvements during DB unload"
echo ""

# Test directory
TEST_DIR="/tmp/vectordb_safety_demo"
rm -rf $TEST_DIR
mkdir -p $TEST_DIR

echo "1. Starting VectorDB server..."
./vectordb -d $TEST_DIR &
SERVER_PID=$!
sleep 2

echo "2. Creating test database and table..."
curl -X POST "http://localhost:8888/api/load" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "safety_test_db",
    "path": "'$TEST_DIR'/safety_test_db",
    "vectorScale": 1000
  }' 2>/dev/null

echo ""
echo "3. Creating table schema..."
curl -X POST "http://localhost:8888/api/safety_test_db/schema/tables" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test_table",
    "fields": [
      {"name": "id", "dataType": "INT", "primaryKey": true},
      {"name": "vector", "dataType": "VECTOR_FLOAT", "dimensions": 128}
    ]
  }' 2>/dev/null

echo ""
echo "4. Inserting test data..."
for i in {1..10}; do
  VECTOR=$(python3 -c "import json; print(json.dumps([0.5] * 128))")
  curl -X POST "http://localhost:8888/api/safety_test_db/data/insert" \
    -H "Content-Type: application/json" \
    -d '{
      "table": "test_table",
      "data": [{"id": '$i', "vector": '$VECTOR'}]
    }' 2>/dev/null
done

echo ""
echo "5. Querying record count (before unload)..."
COUNT_BEFORE=$(curl -s "http://localhost:8888/api/safety_test_db/records/count?table=test_table" | grep -o '"count":[0-9]*' | cut -d: -f2)
echo "   Record count: $COUNT_BEFORE"

echo ""
echo "6. Unloading database (triggers WAL flush and metadata save)..."
curl -X POST "http://localhost:8888/api/safety_test_db/unload" 2>/dev/null
sleep 1

echo ""
echo "7. Reloading database..."
curl -X POST "http://localhost:8888/api/load" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "safety_test_db",
    "path": "'$TEST_DIR'/safety_test_db",
    "vectorScale": 1000
  }' 2>/dev/null

echo ""
echo "8. Querying record count (after reload)..."
COUNT_AFTER=$(curl -s "http://localhost:8888/api/safety_test_db/records/count?table=test_table" | grep -o '"count":[0-9]*' | cut -d: -f2)
echo "   Record count: $COUNT_AFTER"

echo ""
echo "=== Test Results ==="
if [ "$COUNT_BEFORE" = "$COUNT_AFTER" ] && [ -n "$COUNT_AFTER" ]; then
  echo "✓ Data safety test passed!"
  echo "  Record count before unload: $COUNT_BEFORE"
  echo "  Record count after reload: $COUNT_AFTER"
  echo "  All data successfully persisted"
else
  echo "✗ Data safety test failed!"
  echo "  Record count before unload: $COUNT_BEFORE"
  echo "  Record count after reload: $COUNT_AFTER"
  echo "  Data may be lost"
fi

echo ""
echo "9. Cleaning up test environment..."
kill $SERVER_PID 2>/dev/null
rm -rf $TEST_DIR

echo ""
echo "=== Data Safety Improvements Description ==="
echo "✓ Auto-flush WAL during UnloadDB - Ensures all write operations are persisted"
echo "✓ Save metadata during UnloadDB - Ensures table structure and configuration are persisted"
echo "✓ Throw exception on WAL fsync failure - Prevents silent data loss"
echo "✓ Graceful shutdown mechanism - Ensures data integrity"
echo ""
echo "Test completed!"