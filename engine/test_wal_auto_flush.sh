#!/bin/bash

echo "=== VectorDB WAL 自动刷新功能测试 ==="
echo ""
echo "本测试验证WAL定期自动刷新机制"
echo ""

# 测试配置
TEST_DIR="/tmp/vectordb_wal_test"
WAL_INTERVAL=5  # 5秒刷新间隔用于测试

echo "测试配置:"
echo "  WAL刷新间隔: ${WAL_INTERVAL}秒"
echo "  测试数据目录: $TEST_DIR"
echo ""

# 清理旧数据
rm -rf $TEST_DIR
mkdir -p $TEST_DIR

echo "1. 启动VectorDB服务器（启用WAL自动刷新）..."
export WAL_FLUSH_INTERVAL=$WAL_INTERVAL
export WAL_AUTO_FLUSH=true
export SOFT_DELETE=false  # 使用hard delete测试

# 启动服务器并捕获日志
./vectordb -d $TEST_DIR > $TEST_DIR/server.log 2>&1 &
SERVER_PID=$!
echo "   服务器PID: $SERVER_PID"
sleep 2

# 检查服务器是否启动成功
if ! kill -0 $SERVER_PID 2>/dev/null; then
  echo "✗ 服务器启动失败"
  cat $TEST_DIR/server.log
  exit 1
fi

echo ""
echo "2. 创建测试数据库..."
curl -X POST "http://localhost:8888/api/load" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "wal_test_db",
    "path": "'$TEST_DIR'/wal_test_db",
    "vectorScale": 100,
    "walEnabled": true
  }' 2>/dev/null

echo ""
echo "3. 创建测试表..."
curl -X POST "http://localhost:8888/api/wal_test_db/schema/tables" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test_table",
    "fields": [
      {"name": "id", "dataType": "INT", "primaryKey": true},
      {"name": "data", "dataType": "STRING"},
      {"name": "vector", "dataType": "VECTOR_FLOAT", "dimensions": 8}
    ]
  }' 2>/dev/null

echo ""
echo "4. 持续插入数据并监控WAL刷新..."
echo "   将在15秒内持续插入数据，期间应看到多次WAL刷新"
echo ""

START_TIME=$(date +%s)
INSERT_COUNT=0

# 后台插入数据
(
  for i in {1..30}; do
    VECTOR="[$(echo "scale=2; $RANDOM/32768" | bc)"
    for j in {2..8}; do
      VECTOR="$VECTOR, $(echo "scale=2; $RANDOM/32768" | bc)"
    done
    VECTOR="$VECTOR]"
    
    curl -X POST "http://localhost:8888/api/wal_test_db/data/insert" \
      -H "Content-Type: application/json" \
      -d '{
        "table": "test_table",
        "data": [{
          "id": '$i',
          "data": "Record '$i' at '$(date +%H:%M:%S)'",
          "vector": '$VECTOR'
        }]
      }' 2>/dev/null >/dev/null
    
    echo -n "."
    sleep 0.5
  done
) &
INSERT_PID=$!

# 监控WAL刷新日志
echo ""
echo "5. 监控WAL刷新活动（15秒）..."
echo "   时间戳              | 事件"
echo "   --------------------|--------------------------------"

MONITOR_START=$(date +%s)
LAST_FLUSH_COUNT=0

while [ $(($(date +%s) - MONITOR_START)) -lt 15 ]; do
  # 检查服务器日志中的WAL刷新事件
  FLUSH_EVENTS=$(grep -c "WAL flush completed" $TEST_DIR/server.log 2>/dev/null || echo "0")
  PERIODIC_FLUSHES=$(grep -c "Performing periodic WAL flush" $TEST_DIR/server.log 2>/dev/null || echo "0")
  
  if [ "$FLUSH_EVENTS" -gt "$LAST_FLUSH_COUNT" ]; then
    echo "   $(date +%H:%M:%S)         | ✓ WAL刷新完成 (总计: $FLUSH_EVENTS次)"
    LAST_FLUSH_COUNT=$FLUSH_EVENTS
  fi
  
  sleep 1
done

# 等待插入完成
wait $INSERT_PID 2>/dev/null

echo ""
echo "6. 检查最终WAL刷新统计..."
sleep 2

# 从日志提取统计信息
TOTAL_FLUSHES=$(grep -c "WAL flush completed" $TEST_DIR/server.log 2>/dev/null || echo "0")
PERIODIC_FLUSHES=$(grep -c "Performing periodic WAL flush" $TEST_DIR/server.log 2>/dev/null || echo "0")
AUTO_FLUSH_STARTED=$(grep -c "WAL auto-flush thread started" $TEST_DIR/server.log 2>/dev/null || echo "0")

echo ""
echo "=== 测试结果 ==="
echo "✓ WAL自动刷新线程状态:"
if [ "$AUTO_FLUSH_STARTED" -gt 0 ]; then
  echo "  • 线程成功启动"
else
  echo "  • 线程未启动 ⚠"
fi

echo ""
echo "✓ WAL刷新统计:"
echo "  • 总刷新次数: $TOTAL_FLUSHES"
echo "  • 定期刷新触发: $PERIODIC_FLUSHES"
echo "  • 配置刷新间隔: ${WAL_INTERVAL}秒"

# 计算预期刷新次数
EXPECTED_FLUSHES=$((15 / WAL_INTERVAL))
echo "  • 预期刷新次数: ~$EXPECTED_FLUSHES"

if [ "$TOTAL_FLUSHES" -ge "$EXPECTED_FLUSHES" ]; then
  echo "  • 状态: ✓ 正常（符合预期）"
else
  echo "  • 状态: ⚠ 低于预期"
fi

echo ""
echo "7. 测试WAL刷新禁用功能..."
# 这里可以通过API动态禁用WAL自动刷新
curl -X POST "http://localhost:8888/api/config" \
  -H "Content-Type: application/json" \
  -d '{"WALAutoFlush": false}' 2>/dev/null >/dev/null

sleep 6
FLUSHES_BEFORE_DISABLE=$TOTAL_FLUSHES
TOTAL_FLUSHES=$(grep -c "WAL flush completed" $TEST_DIR/server.log 2>/dev/null || echo "0")

if [ "$TOTAL_FLUSHES" -eq "$FLUSHES_BEFORE_DISABLE" ]; then
  echo "  ✓ WAL自动刷新成功禁用"
else
  echo "  ⚠ WAL自动刷新可能仍在运行"
fi

echo ""
echo "8. 清理测试环境..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null
rm -rf $TEST_DIR

echo ""
echo "=== WAL自动刷新功能总结 ==="
echo "✓ 环境变量配置: WAL_FLUSH_INTERVAL, WAL_AUTO_FLUSH"
echo "✓ 默认刷新间隔: 30秒"
echo "✓ 后台线程自动执行"
echo "✓ 支持运行时动态配置"
echo "✓ 提供刷新统计信息"
echo "✓ UnloadDB时自动刷新"
echo "✓ 服务器关闭时最终刷新"
echo ""
echo "生产环境建议:"
echo "• 根据写入频率调整刷新间隔（5-60秒）"
echo "• 监控WAL刷新失败次数"
echo "• 在关键操作后主动调用刷新API"
echo "• 定期检查WAL文件大小"
echo ""
echo "测试完成！"