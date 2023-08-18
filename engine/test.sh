export PYTHONPATH=./build/ 
export DB_PATH=/tmp/db2
rm -rf "$DB_PATH"
echo $1
if [ "$1" == "--single-thread" ]; then
  python3 test/bindings/python/test.py
else
  python3 test/bindings/python/concurrent_test.py
fi
