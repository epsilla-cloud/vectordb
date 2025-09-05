#pragma once

#include <memory>
#include <string>
#include <cstdlib>
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

/**
 * @brief Dynamic segment configuration manager (simplified version)
 * 
 * Change from hardcoded 150000 to configurable dynamic capacity
 */
class DynamicConfigManager {
public:
    struct Config {
        size_t initial_capacity = 5000;      // Default 5000 instead of 150000
        size_t max_capacity = 100000000;     // Maximum 100 million
        double growth_factor = 2.0;          // Growth factor
        bool allow_dynamic = true;           // Whether to allow dynamic expansion
    };
    
    /**
     * @brief Get dynamic configuration from legacy init_table_scale parameter
     */
    static Config GetConfig(int64_t init_table_scale) {
        Config config;
        
        // If hardcoded 150000, use more reasonable defaults
        if (init_table_scale == 150000) {
            // Read from environment variable, allow override
            const char* env_initial = std::getenv("EPSILLA_INITIAL_CAPACITY");
            if (env_initial) {
                config.initial_capacity = std::stoul(env_initial);
            } else {
                config.initial_capacity = 5000;  // Default 5000, not 150000
            }
            
            const char* env_max = std::getenv("EPSILLA_MAX_CAPACITY"); 
            if (env_max) {
                config.max_capacity = std::stoul(env_max);
            }
            
            const char* env_growth = std::getenv("EPSILLA_GROWTH_FACTOR");
            if (env_growth) {
                config.growth_factor = std::stod(env_growth);
            }
            
            Logger logger;
            logger.Info("Dynamic memory configuration:");
            logger.Info("  Initial capacity: " + std::to_string(config.initial_capacity) + 
                       " (replacing hardcoded 150000)");
            logger.Info("  Max capacity: " + std::to_string(config.max_capacity));
            logger.Info("  Growth factor: " + std::to_string(config.growth_factor));
            
            config.allow_dynamic = true;
        } else {
            // User specified value, maintain compatibility
            config.initial_capacity = init_table_scale;
            config.max_capacity = init_table_scale * 100;  // Allow 100x growth
            config.growth_factor = 1.5;
            config.allow_dynamic = true;
            
            // Allow environment variable override
            const char* override_capacity = std::getenv("EPSILLA_OVERRIDE_CAPACITY");
            if (override_capacity && std::string(override_capacity) == "true") {
                const char* env_initial = std::getenv("EPSILLA_INITIAL_CAPACITY");
                if (env_initial) {
                    config.initial_capacity = std::stoul(env_initial);
                    
                    Logger logger;
                    logger.Info("Overriding capacity to " + 
                               std::to_string(config.initial_capacity) + 
                               " (from " + std::to_string(init_table_scale) + ")");
                }
            }
        }
        
        return config;
    }
    
    /**
     * @brief Determine whether to use dynamic configuration
     */
    static bool ShouldUseDynamic(int64_t init_table_scale) {
        // If environment variable explicitly requests legacy version
        const char* use_legacy = std::getenv("EPSILLA_USE_LEGACY");
        if (use_legacy && std::string(use_legacy) == "true") {
            return false;
        }
        
        // If hardcoded 150000, recommend using dynamic
        if (init_table_scale == 150000) {
            return true;
        }
        
        // Default to use dynamic
        const char* use_dynamic = std::getenv("EPSILLA_USE_DYNAMIC");
        if (use_dynamic && std::string(use_dynamic) == "false") {
            return false;
        }
        
        return true;
    }
    
    /**
     * @brief Get the actual initial capacity to use
     */
    static int64_t GetInitialCapacity(int64_t init_table_scale) {
        if (ShouldUseDynamic(init_table_scale)) {
            auto config = GetConfig(init_table_scale);
            return config.initial_capacity;
        }
        return init_table_scale;
    }
};

} // namespace engine
} // namespace vectordb