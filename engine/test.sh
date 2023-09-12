export PYTHONPATH=./build/ 
export DB_PATH=/tmp/db2
rm -rf "$DB_PATH"
if [ "$1" == "--single-thread" ]; then
  echo "running single-thread mode"
  python3 test/bindings/python/test.py
else
  python3 test/bindings/python/concurrent_test.py
fi
