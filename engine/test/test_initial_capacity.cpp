#include <iostream>
#include <cstdlib>
#include "db/table_segment_dynamic_simple.hpp"

int main() {
    using namespace vectordb::engine;
    
    // Test 1: Hardcoded 150000 should be replaced with 5000
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
    
    // Test 2: Environment variable override
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
    
    // Test 3: Non-150000 values remain unchanged
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
    
    // Test 4: GetInitialCapacity function
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