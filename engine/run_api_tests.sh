#!/bin/bash

echo "=================================================="
echo "     Epsilla VectorDB API Test Runner"
echo "=================================================="
echo ""

# Configuration
TEST_DIR="/tmp/vectordb_api_test"
SERVER_LOG="$TEST_DIR/server.log"
TEST_LOG="$TEST_DIR/test.log"
PYTHON_SCRIPT="test/api/comprehensive_api_test.py"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check Python3
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python3 is not installed${NC}"
    exit 1
fi

# Check requests module
if ! python3 -c "import requests" 2>/dev/null; then
    echo -e "${YELLOW}Installing requests module...${NC}"
    pip3 install requests || {
        echo -e "${RED}Failed to install requests module${NC}"
        exit 1
    }
fi

# Clean and create test directory
echo "1. Preparing test environment..."
rm -rf $TEST_DIR
mkdir -p $TEST_DIR

# Check if vectordb binary exists
if [ ! -f "./vectordb" ]; then
    echo -e "${RED}Error: vectordb binary not found. Please compile first.${NC}"
    echo "Run: make -j4"
    exit 1
fi

# Start VectorDB server
echo "2. Starting VectorDB server..."
export WAL_AUTO_FLUSH=true
export WAL_FLUSH_INTERVAL=10
export SOFT_DELETE=false

./vectordb -d $TEST_DIR > $SERVER_LOG 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo "   Waiting for server to start..."
for i in {1..10}; do
    if curl -s http://localhost:8888/api/health > /dev/null 2>&1; then
        echo -e "${GREEN}   Server started successfully (PID: $SERVER_PID)${NC}"
        break
    fi
    sleep 1
done

# Check if server started
if ! curl -s http://localhost:8888/api/health > /dev/null 2>&1; then
    echo -e "${RED}Error: Server failed to start${NC}"
    echo "Server log:"
    cat $SERVER_LOG
    exit 1
fi

echo ""
echo "3. Running API tests..."
echo "=================================================="

# Run the Python test suite
python3 $PYTHON_SCRIPT 2>&1 | tee $TEST_LOG

# Get test result
if grep -q "TEST SUITE PASSED" $TEST_LOG; then
    TEST_RESULT=0
    echo -e "\n${GREEN}✅ All API tests completed successfully!${NC}"
else
    TEST_RESULT=1
    echo -e "\n${RED}❌ Some API tests failed. Check the report for details.${NC}"
fi

echo ""
echo "4. Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "5. Test artifacts:"
echo "   - Server log: $SERVER_LOG"
echo "   - Test log: $TEST_LOG"
echo "   - Test report: api_test_report.json"

echo ""
echo "=================================================="
echo "                Test Summary"
echo "=================================================="

# Extract summary from report
if [ -f "api_test_report.json" ]; then
    python3 -c "
import json
with open('api_test_report.json', 'r') as f:
    report = json.load(f)
    s = report['summary']
    print(f\"Total Tests: {s['total']}\")
    print(f\"Passed: {s['passed']} ({s['pass_rate']:.1f}%)\")
    print(f\"Failed: {s['failed']}\")
    print(f\"Errors: {s['errors']}\")
    print(f\"Total Duration: {s['total_duration_ms']:.2f}ms\")
    "
fi

echo "=================================================="

# Cleanup option
read -p "Do you want to clean up test data? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -rf $TEST_DIR
    echo "Test data cleaned up."
fi

exit $TEST_RESULT