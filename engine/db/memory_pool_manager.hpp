#pragma once

#include <atomic>
#include <memory>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <cstring>
#include "utils/status.hpp"
#include "utils/error.hpp"
#include "config/config.hpp"

namespace vectordb {
namespace engine {

/**
 * @brief Memory allocation strategy for vector database
 */
enum class MemoryStrategy {
    FIXED,      // Fixed pre-allocation (old behavior)
    DYNAMIC,    // Dynamic growth
    LAZY,       // Lazy allocation on first use
    ADAPTIVE    // Adaptive based on usage patterns
};

/**
 * @brief Memory pool statistics
 */
struct MemoryStats {
    std::atomic<size_t> total_allocated{0};     // Total memory allocated
    std::atomic<size_t> total_used{0};          // Memory actually in use
    std::atomic<size_t> peak_usage{0};          // Peak memory usage
    std::atomic<size_t> allocation_count{0};    // Number of allocations
    std::atomic<size_t> deallocation_count{0};  // Number of deallocations
    std::atomic<size_t> reallocation_count{0};  // Number of reallocations

    double GetUsageRatio() const {
        size_t allocated = total_allocated.load();
        if (allocated == 0) return 0.0;
        return static_cast<double>(total_used.load()) / allocated;
    }

    Json ToJson() const {
        Json stats;
        stats.SetInt("total_allocated_mb", total_allocated.load() / (1024 * 1024));
        stats.SetInt("total_used_mb", total_used.load() / (1024 * 1024));
        stats.SetInt("peak_usage_mb", peak_usage.load() / (1024 * 1024));
        stats.SetDouble("usage_ratio", GetUsageRatio());
        stats.SetInt("allocations", allocation_count.load());
        stats.SetInt("deallocations", deallocation_count.load());
        stats.SetInt("reallocations", reallocation_count.load());
        return stats;
    }
};

// DynamicMemoryBlock is now defined in dynamic_memory_block.hpp
// to avoid duplicate definitions
#include "db/dynamic_memory_block.hpp"

/**
 * @brief Global memory pool manager (Singleton)
 */
class MemoryPoolManager {
public:
    static MemoryPoolManager& GetInstance() {
        static MemoryPoolManager instance;
        return instance;
    }

    // Delete copy constructor and assignment operator
    MemoryPoolManager(const MemoryPoolManager&) = delete;
    MemoryPoolManager& operator=(const MemoryPoolManager&) = delete;

    /**
     * @brief Set memory allocation strategy
     */
    void SetStrategy(MemoryStrategy strategy) {
        strategy_ = strategy;
        // Memory strategy updated
    }

    /**
     * @brief Get current memory strategy
     */
    MemoryStrategy GetStrategy() const {
        return strategy_;
    }

    /**
     * @brief Allocate memory with tracking
     */
    template<typename T>
    std::unique_ptr<T[]> Allocate(size_t count) {
        size_t bytes = count * sizeof(T);
        stats_.total_allocated += bytes;
        stats_.allocation_count++;

        // Update peak usage
        size_t current_allocated = stats_.total_allocated.load();
        size_t peak = stats_.peak_usage.load();
        while (peak < current_allocated &&
               !stats_.peak_usage.compare_exchange_weak(peak, current_allocated)) {
            // Loop until successful
        }

        return std::make_unique<T[]>(count);
    }

    /**
     * @brief Deallocate memory with tracking
     */
    void Deallocate(size_t bytes) {
        stats_.total_allocated -= bytes;
        stats_.deallocation_count++;
    }

    /**
     * @brief Record memory usage
     */
    void RecordUsage(size_t bytes) {
        stats_.total_used = bytes;
    }

    /**
     * @brief Get memory statistics
     */
    const MemoryStats& GetStats() const {
        return stats_;
    }

    /**
     * @brief Get memory statistics as JSON
     */
    Json GetStatsAsJson() const {
        Json result;
        result.SetObject("memory_stats", stats_.ToJson());
        result.SetString("strategy", StrategyToString(strategy_));
        result.SetInt("initial_capacity", globalConfig.InitialTableCapacity.load());
        return result;
    }

    /**
     * @brief Check if memory limit is exceeded
     */
    bool IsMemoryPressure(size_t max_memory_bytes = 0) const {
        if (max_memory_bytes == 0) {
            // Default to 80% of system memory
            max_memory_bytes = GetSystemMemory() * 0.8;
        }
        return stats_.total_allocated.load() > max_memory_bytes;
    }

    /**
     * @brief Suggest optimal initial capacity based on usage patterns
     */
    size_t SuggestInitialCapacity() const {
        if (strategy_ == MemoryStrategy::ADAPTIVE) {
            // Based on average table size from history
            if (stats_.allocation_count > 0) {
                size_t avg_size = stats_.total_allocated / stats_.allocation_count;
                // Start with 10% of average
                return std::max(size_t(100), avg_size / 10);
            }
        }
        return globalConfig.InitialTableCapacity.load();
    }

private:
    MemoryPoolManager() : strategy_(MemoryStrategy::DYNAMIC) {}
    ~MemoryPoolManager() = default;

    static size_t GetSystemMemory() {
        // Simple implementation - can be enhanced with platform-specific code
        return 8ULL * 1024 * 1024 * 1024;  // Default 8GB
    }

    static std::string StrategyToString(MemoryStrategy strategy) {
        switch (strategy) {
            case MemoryStrategy::FIXED: return "FIXED";
            case MemoryStrategy::DYNAMIC: return "DYNAMIC";
            case MemoryStrategy::LAZY: return "LAZY";
            case MemoryStrategy::ADAPTIVE: return "ADAPTIVE";
            default: return "UNKNOWN";
        }
    }

    MemoryStrategy strategy_;
    MemoryStats stats_;
};

}  // namespace engine
}  // namespace vectordb