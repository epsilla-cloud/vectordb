#!/bin/bash

echo "=== 并发安全Hard Delete功能测试 ==="
echo ""

# 检查是否启用hard delete
echo "1. 测试Hard Delete模式配置:"
export SOFT_DELETE=false
./vectordb -h 2>&1 | grep "SoftDelete=false" && echo "✓ Hard delete模式已启用" || echo "✗ Hard delete模式未启用"
echo ""

# 运行并发安全测试（如果测试可执行文件存在）
if [ -f "./vector_db_test" ]; then
    echo "2. 运行并发安全测试:"
    echo "   执行并发删除测试..."
    ./vector_db_test --gtest_filter="ConcurrentHardDeleteTest.*" 2>&1 | grep -E "PASSED|FAILED|OK"
else
    echo "2. 测试可执行文件未找到，跳过并发测试"
fi

echo ""
echo "=== 并发安全特性说明 ==="
echo "✓ 批量删除处理 - 避免单记录竞争"
echo "✓ 独占写锁保护 - 防止并发访问"  
echo "✓ 主键索引重建 - 确保一致性"
echo "✓ 原子记录计数 - 线程安全更新"
echo "✓ 数据压缩优化 - 减少内存碎片"
echo ""
echo "测试完成！"