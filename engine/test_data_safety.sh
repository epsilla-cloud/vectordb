#!/bin/bash

echo "=== VectorDB数据安全性测试 ==="
echo ""
echo "本测试验证DB unload时的数据持久化改进"
echo ""

# 测试目录
TEST_DIR="/tmp/vectordb_safety_demo"
rm -rf $TEST_DIR
mkdir -p $TEST_DIR

echo "1. 启动VectorDB服务器..."
./vectordb -d $TEST_DIR &
SERVER_PID=$!
sleep 2

echo "2. 创建测试数据库和表..."
curl -X POST "http://localhost:8888/api/load" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "safety_test_db",
    "path": "'$TEST_DIR'/safety_test_db",
    "vectorScale": 1000
  }' 2>/dev/null

echo ""
echo "3. 创建表结构..."
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
echo "4. 插入测试数据..."
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
echo "5. 查询记录数(unload前)..."
COUNT_BEFORE=$(curl -s "http://localhost:8888/api/safety_test_db/records/count?table=test_table" | grep -o '"count":[0-9]*' | cut -d: -f2)
echo "   记录数: $COUNT_BEFORE"

echo ""
echo "6. Unload数据库(触发WAL flush和元数据保存)..."
curl -X POST "http://localhost:8888/api/safety_test_db/unload" 2>/dev/null
sleep 1

echo ""
echo "7. 重新加载数据库..."
curl -X POST "http://localhost:8888/api/load" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "safety_test_db",
    "path": "'$TEST_DIR'/safety_test_db",
    "vectorScale": 1000
  }' 2>/dev/null

echo ""
echo "8. 查询记录数(reload后)..."
COUNT_AFTER=$(curl -s "http://localhost:8888/api/safety_test_db/records/count?table=test_table" | grep -o '"count":[0-9]*' | cut -d: -f2)
echo "   记录数: $COUNT_AFTER"

echo ""
echo "=== 测试结果 ==="
if [ "$COUNT_BEFORE" = "$COUNT_AFTER" ] && [ -n "$COUNT_AFTER" ]; then
  echo "✓ 数据安全性测试通过！"
  echo "  Unload前记录数: $COUNT_BEFORE"
  echo "  Reload后记录数: $COUNT_AFTER"
  echo "  所有数据成功持久化"
else
  echo "✗ 数据安全性测试失败！"
  echo "  Unload前记录数: $COUNT_BEFORE"
  echo "  Reload后记录数: $COUNT_AFTER"
  echo "  数据可能丢失"
fi

echo ""
echo "9. 清理测试环境..."
kill $SERVER_PID 2>/dev/null
rm -rf $TEST_DIR

echo ""
echo "=== 数据安全改进说明 ==="
echo "✓ UnloadDB时自动刷新WAL - 确保所有写入操作持久化"
echo "✓ UnloadDB时保存元数据 - 确保表结构和配置持久化"
echo "✓ WAL fsync失败抛异常 - 防止静默数据丢失"
echo "✓ 优雅关闭机制 - 确保数据完整性"
echo ""
echo "测试完成！"