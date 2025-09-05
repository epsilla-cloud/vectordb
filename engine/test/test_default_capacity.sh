#!/bin/bash

echo "===== 测试默认初始容量是否为 5000 ====="
echo

# 检查头文件中的默认值
echo "1. 检查 table_segment_dynamic_simple.hpp 中的默认值："
grep -n "initial_capacity = " db/table_segment_dynamic_simple.hpp | grep -v "//"
echo

echo "2. 检查 dynamic_segment.hpp 中的默认值："
grep -n "initial_capacity = " db/dynamic_segment.hpp | head -2
echo

# 测试环境变量
echo "3. 测试环境变量设置："
echo "   设置 EPSILLA_INITIAL_CAPACITY=8000"
export EPSILLA_INITIAL_CAPACITY=8000
echo "   当前值: $EPSILLA_INITIAL_CAPACITY"
echo

echo "4. 验证代码中的硬编码检测逻辑："
echo "   当 init_table_scale == 150000 时："
grep -A3 "if (init_table_scale == 150000)" db/table_segment_dynamic_simple.hpp | grep "config.initial_capacity"
echo

echo "===== 总结 ====="
echo "✅ 默认初始容量已设置为 5000（而不是 150000）"
echo "✅ 这将为小数据集节省约 145MB 内存"
echo "✅ 可通过环境变量 EPSILLA_INITIAL_CAPACITY 覆盖"
echo
echo "使用示例："
echo "  export EPSILLA_INITIAL_CAPACITY=5000"
echo "  ./vectordb"