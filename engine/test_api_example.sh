#!/bin/bash

echo "=== VectorDB API Testing Example ==="
echo ""
echo "This demonstrates how to run API tests on VectorDB"
echo ""

# Check if vectordb binary exists
if [ ! -f "./vectordb" ]; then
    echo "❌ Please compile VectorDB first:"
    echo "   make -j4"
    exit 1
fi

# Check Python dependencies
if ! python3 -c "import requests" 2>/dev/null; then
    echo "📦 Installing Python requests..."
    pip3 install requests
fi

echo "1. Starting VectorDB server..."
export WAL_AUTO_FLUSH=true
export WAL_FLUSH_INTERVAL=10

./vectordb -d /tmp/api_demo &
SERVER_PID=$!
echo "   Server PID: $SERVER_PID"

# Wait for startup
echo "   Waiting for server startup..."
for i in {1..10}; do
    if curl -s http://localhost:8888/api/health > /dev/null 2>&1; then
        echo "   ✅ Server is running"
        break
    fi
    sleep 1
done

# Verify server health
if ! curl -s http://localhost:8888/api/health > /dev/null 2>&1; then
    echo "   ❌ Server failed to start"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

echo ""
echo "2. Running quick API test..."
echo ""

# Run the lightweight CI test
python3 test/api/ci_api_test.py

echo ""
echo "3. Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "🎉 API test example completed!"
echo ""
echo "Next steps:"
echo "• Run full test suite: ./run_api_tests.sh"
echo "• Run comprehensive tests: python3 test/api/comprehensive_api_test.py"
echo "• See API_TESTING_GUIDE.md for more details"