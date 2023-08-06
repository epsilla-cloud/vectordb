export PYTHONPATH=./build/ 
export DB_PATH=/tmp/db2
rm -rf "$DB_PATH"
python3 test/bindings/python/test.py
