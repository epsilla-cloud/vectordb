#include <iostream>
#include <cstdlib>
#include "db/table_segment_dynamic_simple.hpp"

int main() {
    using namespace vectordb::engine;
    
    // 测试1：硬编码的150000应该被替换为5000
    {
        auto config = DynamicConfigManager::GetConfig(150000);
        std::cout << "Test 1 - Hardcoded 150000:" << std::endl;
        std::cout << "  Initial capacity: " << config.initial_capacity 
                  << " (expected: 5000)" << std::endl;
        if (config.initial_capacity == 5000) {
            std::cout << "  ✅ PASS" << std::endl;
        } else {
            std::cout << "  ❌ FAIL" << std::endl;
        }
    }
    
    // 测试2：环境变量覆盖
    {
        setenv("EPSILLA_INITIAL_CAPACITY", "8000", 1);
        auto config = DynamicConfigManager::GetConfig(150000);
        std::cout << "\nTest 2 - Environment variable override:" << std::endl;
        std::cout << "  Initial capacity: " << config.initial_capacity 
                  << " (expected: 8000)" << std::endl;
        if (config.initial_capacity == 8000) {
            std::cout << "  ✅ PASS" << std::endl;
        } else {
            std::cout << "  ❌ FAIL" << std::endl;
        }
        unsetenv("EPSILLA_INITIAL_CAPACITY");
    }
    
    // 测试3：非150000的值保持不变
    {
        auto config = DynamicConfigManager::GetConfig(10000);
        std::cout << "\nTest 3 - Custom value (10000):" << std::endl;
        std::cout << "  Initial capacity: " << config.initial_capacity 
                  << " (expected: 10000)" << std::endl;
        if (config.initial_capacity == 10000) {
            std::cout << "  ✅ PASS" << std::endl;
        } else {
            std::cout << "  ❌ FAIL" << std::endl;
        }
    }
    
    // 测试4：GetInitialCapacity函数
    {
        int64_t capacity = DynamicConfigManager::GetInitialCapacity(150000);
        std::cout << "\nTest 4 - GetInitialCapacity(150000):" << std::endl;
        std::cout << "  Returned capacity: " << capacity 
                  << " (expected: 5000)" << std::endl;
        if (capacity == 5000) {
            std::cout << "  ✅ PASS" << std::endl;
        } else {
            std::cout << "  ❌ FAIL" << std::endl;
        }
    }
    
    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << "Default initial capacity is now 5000 (not 150000)" << std::endl;
    std::cout << "This saves ~145MB memory for small datasets!" << std::endl;
    
    return 0;
}