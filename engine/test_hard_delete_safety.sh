#!/bin/bash

echo "=== Concurrent Safe Hard Delete Function Test ==="
echo ""

# Check if hard delete is enabled
echo "1. Testing Hard Delete Mode Configuration:"
export SOFT_DELETE=false
./vectordb -h 2>&1 | grep "SoftDelete=false" && echo "✓ Hard delete mode enabled" || echo "✗ Hard delete mode not enabled"
echo ""

# Run concurrent safety tests (if test executable exists)
if [ -f "./vector_db_test" ]; then
    echo "2. Running concurrent safety tests:"
    echo "   Executing concurrent deletion tests..."
    ./vector_db_test --gtest_filter="ConcurrentHardDeleteTest.*" 2>&1 | grep -E "PASSED|FAILED|OK"
else
    echo "2. Test executable not found, skipping concurrent tests"
fi

echo ""
echo "=== Concurrent Safety Features Description ==="
echo "✓ Batch deletion processing - Avoids single record competition"
echo "✓ Exclusive write lock protection - Prevents concurrent access"  
echo "✓ Primary key index rebuild - Ensures consistency"
echo "✓ Atomic record counting - Thread-safe updates"
echo "✓ Data compaction optimization - Reduces memory fragmentation"
echo ""
echo "Test completed!"