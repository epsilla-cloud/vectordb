#!/bin/bash

echo "=== Epsilla VectorDB Hard Delete Demo ==="
echo ""

echo "1. Testing soft delete mode (default):"
export SOFT_DELETE=true
./vectordb -h | grep SoftDelete
echo ""

echo "2. Testing hard delete mode:"  
export SOFT_DELETE=false
./vectordb -h | grep SoftDelete
echo ""

echo "3. Testing different SOFT_DELETE values:"
for value in "true" "TRUE" "1" "yes" "false" "FALSE" "0" "no" "invalid"; do
  echo -n "SOFT_DELETE=$value -> "
  SOFT_DELETE=$value ./vectordb -h 2>/dev/null | grep SoftDelete | cut -d' ' -f7
done
echo ""

echo "=== Configuration Test Complete ==="
echo ""
echo "Usage:"
echo "  export SOFT_DELETE=true   # Enable soft delete (default)"
echo "  export SOFT_DELETE=false  # Enable hard delete" 
echo ""
echo "Soft Delete: Records marked as deleted, data remains in memory"
echo "Hard Delete: Records physically removed from memory immediately"