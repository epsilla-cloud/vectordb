#pragma once

#include <string>
#include <memory>
#include <chrono>
#include "db/catalog/meta_types.hpp"

namespace vectordb {
namespace engine {
namespace index {

/**
 * @brief Index type enumeration
 */
enum class IndexType {
  NSG = 1,       // Current NSG implementation
  IVF = 2,       // Inverted File index
  PQ = 3,        // Product Quantization
  IVF_PQ = 4,    // IVF with PQ compression
  FLAT = 5,      // Brute-force flat search
  AUTO = 99      // Auto-select based on data characteristics
};

/**
 * @brief Index selection strategy
 */
enum class IndexSelectionStrategy {
  PERFORMANCE_FIRST = 1,  // Optimize for search speed
  MEMORY_FIRST = 2,       // Optimize for memory usage
  BALANCED = 3,           // Balance between performance and memory
  CUSTOM = 4              // Use custom selection logic
};

/**
 * @brief Index performance characteristics
 */
struct IndexCharacteristics {
  // Build characteristics
  size_t build_time_complexity_factor;  // Relative build time (1-10)
  size_t memory_usage_factor;           // Relative memory usage (1-10)
  
  // Search characteristics
  size_t search_time_complexity_factor; // Relative search time (1-10)
  double accuracy_factor;               // Expected accuracy (0.0-1.0)
  
  // Data size recommendations
  size_t min_recommended_vectors;       // Minimum vectors for efficiency
  size_t max_recommended_vectors;       // Maximum vectors before performance degrades
  
  // Other characteristics
  bool supports_incremental_update;    // Can add vectors without full rebuild
  bool supports_deletion;              // Can delete vectors efficiently
  bool memory_mappable;                // Can use memory-mapped files
};

/**
 * @brief Index build parameters base class
 */
struct IndexBuildParams {
  IndexType index_type = IndexType::AUTO;
  meta::MetricType metric_type = meta::MetricType::EUCLIDEAN;
  
  // Index selection parameters
  IndexSelectionStrategy selection_strategy = IndexSelectionStrategy::BALANCED;
  
  // Performance hints
  size_t expected_query_per_second = 100;
  size_t available_memory_mb = 1024;
  double target_accuracy = 0.95;
  
  // Prewarming parameters
  bool enable_prewarming = true;
  size_t prewarming_queries = 100;
  
  virtual ~IndexBuildParams() = default;
};

/**
 * @brief NSG-specific parameters
 */
struct NSGBuildParams : public IndexBuildParams {
  size_t search_length = 45;
  size_t out_degree = 50;
  size_t candidate_pool_size = 300;
  size_t knng = 100;
  
  NSGBuildParams() {
    index_type = IndexType::NSG;
  }
};

/**
 * @brief IVF-specific parameters
 */
struct IVFBuildParams : public IndexBuildParams {
  size_t nlist = 1024;           // Number of clusters
  size_t nprobe = 128;           // Number of clusters to search
  size_t max_iterations = 25;    // K-means iterations
  double convergence_factor = 0.95;
  
  IVFBuildParams() {
    index_type = IndexType::IVF;
  }
};

/**
 * @brief PQ-specific parameters  
 */
struct PQBuildParams : public IndexBuildParams {
  size_t m = 8;                  // Number of subquantizers
  size_t nbits = 8;              // Bits per subquantizer
  size_t training_sample_ratio = 10;  // Percentage of data for training
  
  PQBuildParams() {
    index_type = IndexType::PQ;
  }
};

/**
 * @brief IVF+PQ combined parameters
 */
struct IVFPQBuildParams : public IndexBuildParams {
  // IVF parameters
  size_t nlist = 1024;
  size_t nprobe = 128;
  size_t max_iterations = 25;
  
  // PQ parameters
  size_t m = 8;
  size_t nbits = 8;
  size_t training_sample_ratio = 10;
  
  IVFPQBuildParams() {
    index_type = IndexType::IVF_PQ;
  }
};

/**
 * @brief Index statistics for monitoring and optimization
 */
struct IndexStats {
  // Build statistics
  std::chrono::milliseconds build_time{0};
  size_t memory_usage_bytes = 0;
  size_t disk_usage_bytes = 0;
  
  // Runtime statistics
  size_t search_count = 0;
  std::chrono::microseconds avg_search_time{0};
  double avg_accuracy = 0.0;
  
  // Index-specific stats
  size_t indexed_vectors = 0;
  size_t dimensions = 0;
  double index_efficiency = 0.0;  // 0.0-1.0, higher is better
  double compression_ratio = 1.0;  // For compressed indices like PQ
  
  // Last update
  std::chrono::system_clock::time_point last_updated = std::chrono::system_clock::now();
};

/**
 * @brief Get index characteristics by type
 */
IndexCharacteristics GetIndexCharacteristics(IndexType type);

/**
 * @brief Get index type name
 */
std::string GetIndexTypeName(IndexType type);

/**
 * @brief Parse index type from string
 */
IndexType ParseIndexType(const std::string& type_str);

} // namespace index
} // namespace engine
} // namespace vectordb