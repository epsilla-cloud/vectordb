#pragma once

#include "db/index/index_types.hpp"
#include "logger/logger.hpp"
#include <unordered_map>
#include <functional>

// Forward declaration
namespace vectordb {
namespace engine {
namespace index {
class BaseIndex;
}
}
}

namespace vectordb {
namespace engine {
namespace index {

/**
 * @brief Index selection criteria
 */
struct SelectionCriteria {
  size_t vector_count;
  size_t dimension;
  size_t available_memory_mb;
  size_t expected_qps;      // Queries per second
  double target_accuracy;   // 0.0 - 1.0
  meta::MetricType metric_type;
  IndexSelectionStrategy strategy;
  
  // Performance constraints
  size_t max_build_time_minutes = 60;
  size_t max_memory_usage_mb = 0;  // 0 = no limit
  double min_accuracy = 0.9;
};

/**
 * @brief Selection scoring result
 */
struct SelectionScore {
  IndexType index_type;
  double total_score;        // Higher is better
  double performance_score;
  double memory_score;
  double accuracy_score;
  double build_time_score;
  std::string rationale;
};

/**
 * @brief Dynamic index selector
 * 
 * Automatically selects the best index type based on data characteristics,
 * resource constraints, and performance requirements.
 */
class IndexSelector {
private:
  mutable Logger logger_;
  
  // Scoring weights for different strategies
  struct ScoringWeights {
    double performance_weight = 0.25;
    double memory_weight = 0.25;
    double accuracy_weight = 0.25;
    double build_time_weight = 0.25;
  };

  // Predefined weight configurations
  static const std::unordered_map<IndexSelectionStrategy, ScoringWeights> strategy_weights_;

public:
  /**
   * @brief Select best index type for given criteria
   */
  static IndexType SelectBestIndex(const SelectionCriteria& criteria);

  /**
   * @brief Get detailed selection analysis with scores for all index types
   */
  static std::vector<SelectionScore> AnalyzeAllIndexTypes(const SelectionCriteria& criteria);

  /**
   * @brief Get recommended parameters for selected index type
   */
  static std::unique_ptr<IndexBuildParams> GetRecommendedParams(
    IndexType index_type, 
    const SelectionCriteria& criteria);

  /**
   * @brief Estimate memory usage for index type and data size
   */
  static size_t EstimateMemoryUsage(IndexType index_type,
                                   size_t vector_count,
                                   size_t dimension);

  /**
   * @brief Estimate build time for index type and data size
   */
  static std::chrono::minutes EstimateBuildTime(IndexType index_type,
                                               size_t vector_count,
                                               size_t dimension);

  /**
   * @brief Estimate search performance (queries per second)
   */
  static size_t EstimateSearchQPS(IndexType index_type,
                                 size_t vector_count,
                                 size_t dimension,
                                 size_t k);

private:
  /**
   * @brief Score an index type for the given criteria
   */
  static SelectionScore ScoreIndexType(IndexType index_type, 
                                      const SelectionCriteria& criteria);

  /**
   * @brief Calculate performance score (0.0 - 1.0, higher is better)
   */
  static double CalculatePerformanceScore(IndexType index_type,
                                         const SelectionCriteria& criteria);

  /**
   * @brief Calculate memory efficiency score (0.0 - 1.0, higher is better)
   */
  static double CalculateMemoryScore(IndexType index_type,
                                    const SelectionCriteria& criteria);

  /**
   * @brief Calculate accuracy score (0.0 - 1.0, higher is better)
   */
  static double CalculateAccuracyScore(IndexType index_type,
                                      const SelectionCriteria& criteria);

  /**
   * @brief Calculate build time score (0.0 - 1.0, higher is better)
   */
  static double CalculateBuildTimeScore(IndexType index_type,
                                       const SelectionCriteria& criteria);

  /**
   * @brief Check if index type meets hard constraints
   */
  static bool MeetsConstraints(IndexType index_type,
                              const SelectionCriteria& criteria);

  /**
   * @brief Generate rationale string for selection
   */
  static std::string GenerateRationale(IndexType index_type,
                                      const SelectionCriteria& criteria,
                                      const SelectionScore& score);

  /**
   * @brief Apply heuristic rules for special cases
   */
  static IndexType ApplyHeuristicRules(const SelectionCriteria& criteria);
};

/**
 * @brief Index performance advisor
 * 
 * Provides recommendations for optimizing index performance
 */
class IndexPerformanceAdvisor {
private:
  mutable Logger logger_;

public:
  struct Recommendation {
    std::string category;     // "memory", "performance", "accuracy", etc.
    std::string suggestion;   // Human-readable suggestion
    double impact_score;      // Expected improvement (0.0 - 1.0)
    bool is_critical;        // Whether this is a critical issue
  };

  /**
   * @brief Analyze current index performance and suggest improvements
   */
  static std::vector<Recommendation> AnalyzeIndex(
    const BaseIndex* index,
    const SelectionCriteria& original_criteria);

  /**
   * @brief Get optimization recommendations for build parameters
   */
  static std::vector<Recommendation> OptimizeBuildParams(
    IndexType index_type,
    const IndexBuildParams& current_params,
    const IndexStats& performance_stats);

  /**
   * @brief Monitor index performance and detect anomalies
   */
  static std::vector<Recommendation> MonitorPerformance(
    const IndexStats& current_stats,
    const IndexStats& baseline_stats);
};

} // namespace index
} // namespace engine
} // namespace vectordb