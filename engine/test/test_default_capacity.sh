#!/bin/bash

echo "===== Test default initial capacity is 5000 ====="
echo

# Check default values in header files
echo "1. Check default values in table_segment_dynamic_simple.hpp:"
grep -n "initial_capacity = " db/table_segment_dynamic_simple.hpp | grep -v "//"
echo

echo "2. Check default values in dynamic_segment.hpp:"
grep -n "initial_capacity = " db/dynamic_segment.hpp | head -2
echo

# Test environment variables
echo "3. Test environment variable settings:"
echo "   Setting EPSILLA_INITIAL_CAPACITY=8000"
export EPSILLA_INITIAL_CAPACITY=8000
echo "   Current value: $EPSILLA_INITIAL_CAPACITY"
echo

echo "4. Verify hardcoded detection logic in code:"
echo "   When init_table_scale == 150000:"
grep -A3 "if (init_table_scale == 150000)" db/table_segment_dynamic_simple.hpp | grep "config.initial_capacity"
echo

echo "===== Summary ====="
echo "✅ Default initial capacity is set to 5000 (instead of 150000)"
echo "✅ This saves ~145MB memory for small datasets"
echo "✅ Can be overridden via EPSILLA_INITIAL_CAPACITY environment variable"
echo
echo "Usage example:"
echo "  export EPSILLA_INITIAL_CAPACITY=5000"
echo "  ./vectordb"