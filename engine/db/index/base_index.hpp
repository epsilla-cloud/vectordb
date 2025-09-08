#pragma once

#include <memory>
#include <vector>
#include <chrono>
#include <string>

#include "db/index/index_types.hpp"
#include "db/vector.hpp"
#include "db/ann_graph_segment.hpp"
#include "utils/concurrent_bitset.hpp"

namespace vectordb {
namespace engine {
namespace index {

/**
 * @brief Base interface for all vector indexes
 */
class BaseIndex {
public:
  virtual ~BaseIndex() = default;

  /**
   * @brief Get index type
   */
  virtual IndexType GetIndexType() const = 0;

  /**
   * @brief Build the index from vectors
   * @param nb Number of vectors
   * @param data Vector data
   * @param ids Vector IDs
   * @param params Build parameters
   * @return Number of vectors successfully indexed
   */
  virtual size_t Build(size_t nb, 
                      const VectorColumnData& data, 
                      const int64_t* ids,
                      const IndexBuildParams& params) = 0;

  /**
   * @brief Search for nearest neighbors
   * @param query Query vector
   * @param nq Number of queries
   * @param dim Dimension
   * @param k Number of neighbors to find
   * @param distances Output distances
   * @param ids Output IDs
   * @param bitset Optional filter bitset
   */
  virtual void Search(const DenseVectorPtr query,
                     const size_t nq,
                     const size_t dim,
                     const size_t k,
                     float* distances,
                     int64_t* ids,
                     ConcurrentBitsetPtr bitset = nullptr) = 0;

  /**
   * @brief Get memory usage in bytes
   */
  virtual size_t GetMemoryUsage() const = 0;

  /**
   * @brief Get index statistics
   */
  virtual IndexStats GetStats() const = 0;

  /**
   * @brief Check if index is trained/built
   */
  virtual bool IsTrained() const = 0;

  /**
   * @brief Get number of indexed vectors
   */
  virtual size_t GetVectorCount() const = 0;

  /**
   * @brief Get vector dimension
   */
  virtual size_t GetDimension() const = 0;

  /**
   * @brief Save index to file
   */
  virtual bool SaveToFile(const std::string& file_path) const = 0;

  /**
   * @brief Load index from file
   */
  virtual bool LoadFromFile(const std::string& file_path) = 0;

  /**
   * @brief Prewharm index with sample queries
   * @param sample_queries Sample queries for warming
   * @param query_count Number of sample queries
   * @param dim Dimension
   * @param k Number of neighbors per query
   */
  virtual void Prewarm(const DenseVectorPtr sample_queries,
                      size_t query_count,
                      size_t dim, 
                      size_t k = 10) = 0;

  /**
   * @brief Add vectors incrementally (if supported)
   * @param nb Number of vectors to add
   * @param data Vector data
   * @param ids Vector IDs
   * @return True if successful, false if not supported
   */
  virtual bool AddVectors(size_t nb,
                         const VectorColumnData& data,
                         const int64_t* ids) {
    // Default implementation: not supported
    return false;
  }

  /**
   * @brief Remove vectors by IDs (if supported)
   * @param ids Vector IDs to remove
   * @param count Number of IDs
   * @return True if successful, false if not supported
   */
  virtual bool RemoveVectors(const int64_t* ids, size_t count) {
    // Default implementation: not supported
    return false;
  }

  /**
   * @brief Get index characteristics
   */
  virtual IndexCharacteristics GetCharacteristics() const {
    return GetIndexCharacteristics(GetIndexType());
  }

protected:
  // Statistics tracking
  mutable IndexStats stats_;
  mutable std::chrono::system_clock::time_point last_search_time_;
  mutable size_t search_time_accumulator_us_ = 0;

  /**
   * @brief Update search statistics
   */
  void UpdateSearchStats(std::chrono::microseconds search_time) const {
    stats_.search_count++;
    search_time_accumulator_us_ += search_time.count();
    stats_.avg_search_time = std::chrono::microseconds(
      search_time_accumulator_us_ / stats_.search_count
    );
    stats_.last_updated = std::chrono::system_clock::now();
  }
};

using BaseIndexPtr = std::shared_ptr<BaseIndex>;

/**
 * @brief Index factory for creating different index types
 */
class IndexFactory {
public:
  /**
   * @brief Create an index of the specified type
   */
  static BaseIndexPtr CreateIndex(IndexType type, 
                                 size_t dimension,
                                 meta::MetricType metric_type);

  /**
   * @brief Auto-select best index type for given parameters
   */
  static IndexType SelectBestIndexType(size_t vector_count,
                                      size_t dimension,
                                      const IndexBuildParams& params);

  /**
   * @brief Create index with auto-selection
   */
  static BaseIndexPtr CreateAutoIndex(size_t vector_count,
                                     size_t dimension,
                                     const IndexBuildParams& params);
};

} // namespace index
} // namespace engine
} // namespace vectordb