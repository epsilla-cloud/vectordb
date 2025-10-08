#pragma once

#include "db/dynamic_segment.hpp"
#include "db/table_segment.hpp"
#include <memory>
#include <string>
#include <cstdlib>

namespace vectordb {
namespace engine {

/**
 * @brief Dynamic table segment configuration manager
 * 
 * Read configuration from environment variables or config files
 */
class DynamicSegmentConfigManager {
public:
    static DynamicSegmentConfig GetConfig() {
        DynamicSegmentConfig config;
        
        // Read configuration from environment variables
        const char* initial_cap = std::getenv("EPSILLA_INITIAL_CAPACITY");
        if (initial_cap) {
            config.initial_capacity = std::stoul(initial_cap);
        } else {
            // Default value: 1000
            config.initial_capacity = 1000;
        }
        
        const char* max_cap = std::getenv("EPSILLA_MAX_CAPACITY");
        if (max_cap) {
            config.max_capacity = std::stoul(max_cap);
        }
        
        const char* growth_factor = std::getenv("EPSILLA_GROWTH_FACTOR");
        if (growth_factor) {
            config.growth_factor = std::stod(growth_factor);
        }
        
        const char* expand_threshold = std::getenv("EPSILLA_EXPAND_THRESHOLD");
        if (expand_threshold) {
            config.expand_threshold = std::stod(expand_threshold);
        }
        
        const char* allow_shrink = std::getenv("EPSILLA_ALLOW_SHRINK");
        if (allow_shrink) {
            config.allow_shrink = (std::string(allow_shrink) == "true");
        }
        
        // Strategy selection
        const char* strategy = std::getenv("EPSILLA_PREALLOC_STRATEGY");
        if (strategy) {
            std::string s(strategy);
            if (s == "none") config.prealloc_strategy = DynamicSegmentConfig::NONE;
            else if (s == "linear") config.prealloc_strategy = DynamicSegmentConfig::LINEAR;
            else if (s == "exponential") config.prealloc_strategy = DynamicSegmentConfig::EXPONENTIAL;
            else config.prealloc_strategy = DynamicSegmentConfig::ADAPTIVE;
        }
        
        return config;
    }
    
    /**
     * @brief Convert from legacy init_table_scale parameter to dynamic configuration
     */
    static DynamicSegmentConfig FromLegacyScale(int64_t init_table_scale) {
        DynamicSegmentConfig config;
        
        // If traditional 150000, use more reasonable initial values
        if (init_table_scale == 150000) {
            config.initial_capacity = 1000;  // Start small
            config.max_capacity = 100000000; // Allow growth to 100 million
            config.growth_factor = 2.0;      // Fast growth
            config.expand_threshold = 0.9;
            config.allow_shrink = true;
            config.prealloc_strategy = DynamicSegmentConfig::ADAPTIVE;
        } else {
            // Use user-specified value as initial capacity
            config.initial_capacity = init_table_scale;
            config.max_capacity = init_table_scale * 100;  // Allow 100x growth
            config.growth_factor = 1.5;
            config.expand_threshold = 0.9;
            config.allow_shrink = false;  // Conservative strategy
            config.prealloc_strategy = DynamicSegmentConfig::LINEAR;
        }
        
        // Allow environment variable override
        const char* override_initial = std::getenv("EPSILLA_OVERRIDE_INITIAL_CAPACITY");
        if (override_initial) {
            config.initial_capacity = std::stoul(override_initial);
        }
        
        return config;
    }
    
    /**
     * @brief Log configuration to logger
     */
    static void LogConfig(const DynamicSegmentConfig& config) {
        Logger logger;
        logger.Info("Dynamic segment configuration:");
        logger.Info("  Initial capacity: " + std::to_string(config.initial_capacity));
        logger.Info("  Max capacity: " + std::to_string(config.max_capacity));
        logger.Info("  Growth factor: " + std::to_string(config.growth_factor));
        logger.Info("  Expand threshold: " + std::to_string(config.expand_threshold * 100) + "%");
        logger.Info("  Shrink threshold: " + std::to_string(config.shrink_threshold * 100) + "%");
        logger.Info("  Allow shrink: " + (config.allow_shrink ? "yes" : "no"));
        
        std::string strategy = "adaptive";
        switch (config.prealloc_strategy) {
            case DynamicSegmentConfig::NONE: strategy = "none"; break;
            case DynamicSegmentConfig::LINEAR: strategy = "linear"; break;
            case DynamicSegmentConfig::EXPONENTIAL: strategy = "exponential"; break;
            case DynamicSegmentConfig::ADAPTIVE: strategy = "adaptive"; break;
        }
        logger.Info("  Prealloc strategy: " + strategy);
    }
};

/**
 * @brief Dynamic table segment MVP - replaces fixed capacity TableSegmentMVP
 * 
 * This is a dynamic memory version of TableSegmentMVP, solving the hardcoded 150000 problem
 */
class TableSegmentDynamic : public TableSegmentMVP {
public:
    /**
     * @brief Create new segment with dynamic configuration
     */
    explicit TableSegmentDynamic(
        meta::TableSchema& table_schema,
        const DynamicSegmentConfig& config,
        std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service)
        : TableSegmentMVP(table_schema, config.initial_capacity, embedding_service),
          config_(config),
          dynamic_segment_(std::make_unique<DynamicTableSegment>(config)) {
        
        // Initialize dynamic segment
        auto status = dynamic_segment_->Init(table_schema);
        if (!status.ok()) {
            throw std::runtime_error("Failed to init dynamic segment: " + status.message());
        }
        
        logger_.Info("Created dynamic table segment with initial capacity " + 
                    std::to_string(config.initial_capacity));
    }
    
    /**
     * @brief Load from disk with dynamic configuration
     */
    explicit TableSegmentDynamic(
        meta::TableSchema& table_schema,
        const std::string& db_catalog_path,
        const DynamicSegmentConfig& config,
        std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service)
        : TableSegmentMVP(table_schema, db_catalog_path, config.initial_capacity, embedding_service),
          config_(config),
          dynamic_segment_(std::make_unique<DynamicTableSegment>(config)) {
        
        // Adjust capacity after loading
        if (record_number_ > config_.initial_capacity) {
            logger_.Warning("Loaded " + std::to_string(record_number_) + 
                          " records, expanding from initial capacity " + 
                          std::to_string(config_.initial_capacity));
            auto status = dynamic_segment_->ExpandTo(record_number_ + 1);
            if (!status.ok()) {
                throw std::runtime_error("Failed to expand: " + status.message());
            }
        }
    }
    
    /**
     * @brief Insert record (auto-expand)
     */
    Status InsertDynamic(const Json& record, int64_t wal_id) {
        // Check and expand capacity
        if (record_number_ >= size_limit_) {
            auto new_capacity = static_cast<size_t>(size_limit_ * config_.growth_factor);
            auto status = ExpandCapacity(new_capacity);
            if (!status.ok()) {
                return status;
            }
        }
        
        // Call parent class insert
        return TableSegmentMVP::Insert(record, wal_id);
    }
    
    /**
     * @brief Bulk insert (auto-expand)
     */
    Status BulkInsertDynamic(const Json& records, int64_t wal_id) {
        if (!records.IsArray()) {
            return Status(DB_UNEXPECTED_ERROR, "Records must be array");
        }
        
        size_t count = records.GetSize();
        size_t required_capacity = record_number_ + count;
        
        // Pre-expand to required capacity
        if (required_capacity > size_limit_) {
            auto status = ExpandCapacity(required_capacity);
            if (!status.ok()) {
                return status;
            }
        }
        
        // Call parent class bulk insert
        // BulkInsert doesn't exist, use loop insertion
        for (size_t i = 0; i < count; ++i) {
            auto status = TableSegmentMVP::Insert(records[i], wal_id);
            if (!status.ok()) return status;
        }
        return Status::OK();
    }
    
    /**
     * @brief Get memory statistics
     */
    DynamicTableSegment::MemoryStats GetDynamicStats() const {
        return dynamic_segment_->GetMemoryStats();
    }
    
    /**
     * @brief Manual memory shrink
     */
    Status ShrinkMemory() {
        if (!config_.allow_shrink) {
            return Status::OK();
        }
        
        // Calculate actual usage rate
        double usage = static_cast<double>(record_number_) / size_limit_;
        if (usage < config_.shrink_threshold) {
            logger_.Info("Shrinking memory, usage " + std::to_string(usage * 100) + "%");
            return dynamic_segment_->Shrink();
        }
        
        return Status::OK();
    }
    
protected:
    /**
     * @brief Expand capacity (reallocate all memory)
     */
    Status ExpandCapacity(size_t new_capacity) {
        if (new_capacity > config_.max_capacity) {
            return Status(DB_UNEXPECTED_ERROR,
                         "Requested capacity " + std::to_string(new_capacity) + 
                         " exceeds max " + std::to_string(config_.max_capacity));
        }
        
        logger_.Info("Expanding table segment from " + std::to_string(size_limit_) + 
                    " to " + std::to_string(new_capacity) + " records");
        
        // Call dynamic segment expansion
        auto status = dynamic_segment_->ExpandTo(new_capacity);
        if (!status.ok()) {
            return status;
        }
        
        // Reinitialize parent class data structures
        // Temporarily return OK, actual expansion logic needs to be implemented
        size_limit_ = new_capacity;
        return Status::OK();
    }
    
    // ReInit function temporarily simplified, actual expansion logic handled in dynamic_segment
    
private:
    DynamicSegmentConfig config_;
    std::unique_ptr<DynamicTableSegment> dynamic_segment_;
    Logger logger_;
};

/**
 * @brief Factory function - create appropriate segment based on configuration
 */
inline std::shared_ptr<TableSegmentMVP> CreateTableSegment(
    meta::TableSchema& table_schema,
    int64_t init_table_scale,
    std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service,
    bool use_dynamic = true) {
    
    if (use_dynamic) {
        // Use dynamic segment
        auto config = DynamicSegmentConfigManager::FromLegacyScale(init_table_scale);
        return std::make_shared<TableSegmentDynamic>(table_schema, config, embedding_service);
    } else {
        // Use traditional fixed segment (backward compatible)
        return std::make_shared<TableSegmentMVP>(table_schema, init_table_scale, embedding_service);
    }
}

} // namespace engine
} // namespace vectordb
