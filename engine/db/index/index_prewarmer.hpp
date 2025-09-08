#pragma once

#include "db/index/base_index.hpp"
#include "logger/logger.hpp"
#include <vector>
#include <random>
#include <thread>
#include <chrono>
#include <memory>

namespace vectordb {
namespace engine {
namespace index {

/**
 * @brief Prewarming strategy
 */
enum class PrewarmingStrategy {
  RANDOM_QUERIES = 1,      // Generate random queries
  SAMPLE_BASED = 2,        // Use sample from indexed data  
  PATTERN_BASED = 3,       // Use query patterns
  HYBRID = 4               // Combination of strategies
};

/**
 * @brief Prewarming configuration
 */
struct PrewarmingConfig {
  bool enabled = true;
  PrewarmingStrategy strategy = PrewarmingStrategy::HYBRID;
  
  // Query generation parameters
  size_t query_count = 100;
  size_t k_neighbors = 10;
  size_t max_warmup_time_seconds = 30;
  
  // Sampling parameters
  double sample_ratio = 0.01;        // Percentage of data to sample
  size_t min_sample_size = 100;
  size_t max_sample_size = 10000;
  
  // Pattern parameters
  bool include_corner_cases = true;   // Test edge cases
  bool include_dense_regions = true;  // Test high-density areas
  bool include_sparse_regions = true; // Test low-density areas
  
  // Performance monitoring
  bool measure_performance = true;
  bool cache_results = true;         // Cache warmup results
  
  // Threading
  size_t thread_count = 0;           // 0 = auto-detect
};

/**
 * @brief Prewarming results
 */
struct PrewarmingResults {
  std::chrono::milliseconds total_time{0};
  std::chrono::milliseconds avg_query_time{0};
  size_t queries_executed = 0;
  size_t successful_queries = 0;
  double success_rate = 0.0;
  
  // Performance metrics
  size_t cache_hits = 0;
  size_t cache_misses = 0;
  size_t memory_pages_touched = 0;
  
  std::string status_message;
  std::vector<std::string> warnings;
};

/**
 * @brief Index prewarmer for cache optimization and performance preparation
 * 
 * Prewarming helps improve initial query performance by:
 * - Loading index data into memory/CPU cache
 * - Exercising search paths
 * - Validating index integrity
 * - Measuring baseline performance
 */
class IndexPrewarmer {
private:
  mutable Logger logger_;
  PrewarmingConfig config_;

public:
  explicit IndexPrewarmer(const PrewarmingConfig& config = PrewarmingConfig{});

  /**
   * @brief Prewarm an index
   */
  PrewarmingResults PrewarmIndex(BaseIndex* index);

  /**
   * @brief Prewarm multiple indexes concurrently
   */
  std::vector<PrewarmingResults> PrewarmIndexes(
    std::vector<BaseIndex*> indexes);

  /**
   * @brief Generate prewarming queries based on strategy
   */
  std::vector<std::vector<float>> GeneratePrewarmingQueries(
    const BaseIndex* index,
    const PrewarmingConfig& config) const;

  /**
   * @brief Update prewarming configuration
   */
  void UpdateConfig(const PrewarmingConfig& config) { config_ = config; }

  /**
   * @brief Get current configuration
   */
  const PrewarmingConfig& GetConfig() const { return config_; }

private:
  /**
   * @brief Generate random queries
   */
  std::vector<std::vector<float>> GenerateRandomQueries(
    size_t dimension, 
    size_t count) const;

  /**
   * @brief Generate queries based on indexed data samples
   */
  std::vector<std::vector<float>> GenerateSampleBasedQueries(
    const BaseIndex* index,
    size_t count) const;

  /**
   * @brief Generate pattern-based queries (corner cases, etc.)
   */
  std::vector<std::vector<float>> GeneratePatternBasedQueries(
    size_t dimension,
    size_t count) const;

  /**
   * @brief Execute warming queries on index
   */
  PrewarmingResults ExecuteWarmingQueries(
    BaseIndex* index,
    const std::vector<std::vector<float>>& queries) const;

  /**
   * @brief Execute queries in parallel
   */
  PrewarmingResults ExecuteParallel(
    BaseIndex* index,
    const std::vector<std::vector<float>>& queries,
    size_t thread_count) const;

  /**
   * @brief Generate queries that cover different regions of the space
   */
  std::vector<std::vector<float>> GenerateSpaceCoveringQueries(
    size_t dimension,
    size_t count) const;

  /**
   * @brief Detect memory page touches during warming
   */
  size_t MonitorMemoryAccess() const;

  /**
   * @brief Validate index integrity during warming
   */
  bool ValidateIndexIntegrity(BaseIndex* index) const;
};

/**
 * @brief Prewarming scheduler for managing periodic prewarming
 */
class PrewarmingScheduler {
private:
  mutable Logger logger_;
  std::atomic<bool> running_{false};
  std::thread scheduler_thread_;
  
  struct ScheduledIndex {
    BaseIndex* index;
    PrewarmingConfig config;
    std::chrono::system_clock::time_point next_warmup;
    std::chrono::minutes interval{60}; // Default 1 hour
    size_t warmup_count = 0;
  };
  
  std::vector<ScheduledIndex> scheduled_indexes_;
  mutable std::mutex scheduler_mutex_;

public:
  /**
   * @brief Schedule an index for periodic prewarming
   */
  void ScheduleIndex(BaseIndex* index,
                    const PrewarmingConfig& config,
                    std::chrono::minutes interval = std::chrono::minutes{60});

  /**
   * @brief Remove index from scheduling
   */
  void UnscheduleIndex(BaseIndex* index);

  /**
   * @brief Start the prewarming scheduler
   */
  void Start();

  /**
   * @brief Stop the prewarming scheduler
   */
  void Stop();

  /**
   * @brief Force prewarming of all scheduled indexes
   */
  void ForcePrewarmAll();

private:
  /**
   * @brief Scheduler main loop
   */
  void SchedulerLoop();
};

} // namespace index
} // namespace engine  
} // namespace vectordb