#pragma once

#include "db/index/base_index.hpp"
#include "db/index/index_selector.hpp"
#include "db/index/index_prewarmer.hpp"
#include "db/index/memory_optimizer.hpp"
#include "logger/logger.hpp"
#include <memory>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <atomic>

namespace vectordb {
namespace engine {
namespace index {

/**
 * @brief Enhanced index manager with advanced features
 * 
 * This manager provides:
 * - Dynamic index selection
 * - Index prewarming
 * - Memory optimization
 * - Performance monitoring
 * - Multi-index support
 */
class EnhancedIndexManager {
private:
  // Index management
  std::unordered_map<std::string, BaseIndexPtr> indexes_;
  mutable std::shared_mutex indexes_mutex_;
  
  // Components
  std::unique_ptr<IndexPrewarmer> prewarmer_;
  std::unique_ptr<IndexMemoryOptimizer> memory_optimizer_;
  std::unique_ptr<PrewarmingScheduler> prewarming_scheduler_;
  
  // Configuration
  bool auto_prewarming_enabled_ = true;
  bool memory_optimization_enabled_ = true;
  bool performance_monitoring_enabled_ = true;
  
  // Monitoring
  std::atomic<bool> monitoring_active_{false};
  std::thread monitoring_thread_;
  
  // Statistics
  struct ManagerStats {
    size_t total_indexes = 0;
    size_t active_indexes = 0;
    size_t total_vectors = 0;
    size_t total_memory_bytes = 0;
    size_t total_searches = 0;
    std::chrono::microseconds avg_search_time{0};
    std::chrono::system_clock::time_point last_updated;
  };
  mutable ManagerStats stats_;
  
  mutable Logger logger_;

public:
  /**
   * @brief Constructor
   */
  explicit EnhancedIndexManager();
  
  /**
   * @brief Destructor
   */
  ~EnhancedIndexManager();

  /**
   * @brief Create and register an index with auto-selection
   * @param index_name Unique name for the index
   * @param vector_count Number of vectors to index
   * @param dimension Vector dimension
   * @param selection_criteria Criteria for index selection
   * @return Pointer to created index
   */
  BaseIndexPtr CreateIndex(const std::string& index_name,
                          size_t vector_count,
                          size_t dimension,
                          const SelectionCriteria& criteria);

  /**
   * @brief Create and register an index with specific type
   */
  BaseIndexPtr CreateIndex(const std::string& index_name,
                          IndexType index_type,
                          size_t dimension,
                          meta::MetricType metric_type);

  /**
   * @brief Get an existing index
   */
  BaseIndexPtr GetIndex(const std::string& index_name) const;

  /**
   * @brief Remove an index
   */
  bool RemoveIndex(const std::string& index_name);

  /**
   * @brief List all registered indexes
   */
  std::vector<std::string> ListIndexes() const;

  /**
   * @brief Build an index with data
   */
  bool BuildIndex(const std::string& index_name,
                 size_t nb,
                 const VectorColumnData& data,
                 const int64_t* ids,
                 const IndexBuildParams& params);

  /**
   * @brief Prewarm an index
   */
  PrewarmingResults PrewarmIndex(const std::string& index_name);

  /**
   * @brief Prewarm all indexes
   */
  std::vector<std::pair<std::string, PrewarmingResults>> PrewarmAllIndexes();

  /**
   * @brief Optimize memory usage for an index
   */
  std::vector<MemoryOptimization> OptimizeIndexMemory(const std::string& index_name);

  /**
   * @brief Apply memory optimizations
   */
  bool ApplyMemoryOptimizations(const std::string& index_name,
                               const std::vector<MemoryOptimization>& optimizations);

  /**
   * @brief Get index recommendations
   */
  std::vector<IndexPerformanceAdvisor::Recommendation> GetIndexRecommendations(
    const std::string& index_name) const;

  /**
   * @brief Get manager statistics
   */
  ManagerStats GetManagerStats() const;

  /**
   * @brief Get detailed index analysis
   */
  std::vector<SelectionScore> AnalyzeIndexOptions(size_t vector_count,
                                                 size_t dimension,
                                                 const SelectionCriteria& criteria) const;

  /**
   * @brief Configuration methods
   */
  void SetAutoPrewarmingEnabled(bool enabled) { 
    auto_prewarming_enabled_ = enabled; 
  }
  
  void SetMemoryOptimizationEnabled(bool enabled) { 
    memory_optimization_enabled_ = enabled; 
  }
  
  void SetPerformanceMonitoringEnabled(bool enabled) { 
    performance_monitoring_enabled_ = enabled; 
  }

  /**
   * @brief Start performance monitoring
   */
  void StartMonitoring();

  /**
   * @brief Stop performance monitoring
   */
  void StopMonitoring();

  /**
   * @brief Save all indexes to disk
   */
  bool SaveAllIndexes(const std::string& base_path) const;

  /**
   * @brief Load indexes from disk
   */
  bool LoadIndexes(const std::string& base_path);

private:
  /**
   * @brief Performance monitoring loop
   */
  void MonitoringLoop();

  /**
   * @brief Update manager statistics
   */
  void UpdateStats() const;

  /**
   * @brief Get index file path
   */
  std::string GetIndexFilePath(const std::string& base_path,
                              const std::string& index_name) const;

  /**
   * @brief Validate index name
   */
  bool IsValidIndexName(const std::string& index_name) const;

  /**
   * @brief Log index operation
   */
  void LogIndexOperation(const std::string& operation,
                        const std::string& index_name,
                        bool success) const;
};

/**
 * @brief Global index manager instance
 */
class GlobalIndexManager {
private:
  static std::unique_ptr<EnhancedIndexManager> instance_;
  static std::mutex instance_mutex_;

public:
  /**
   * @brief Get global instance
   */
  static EnhancedIndexManager& GetInstance();

  /**
   * @brief Initialize global instance with custom configuration
   */
  static void Initialize();

  /**
   * @brief Shutdown global instance
   */
  static void Shutdown();
};

} // namespace index
} // namespace engine
} // namespace vectordb