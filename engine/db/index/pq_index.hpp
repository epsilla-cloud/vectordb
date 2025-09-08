#pragma once

#include "db/index/base_index.hpp"
#include "db/index/distances.hpp"
#include "logger/logger.hpp"
#include <vector>
#include <memory>
#include <random>

namespace vectordb {
namespace engine {
namespace index {

/**
 * @brief Product Quantization (PQ) Index implementation
 * 
 * PQ compresses high-dimensional vectors by dividing them into subvectors
 * and quantizing each subvector separately. This provides significant
 * memory savings at the cost of some accuracy.
 */
class PQIndex : public BaseIndex {
private:
  size_t dimension_;
  size_t m_;               // Number of subquantizers
  size_t nbits_;           // Bits per subquantizer
  size_t ksub_;            // Number of centroids per subquantizer (2^nbits)
  size_t dsub_;            // Dimension of each subvector (dimension / m)
  meta::MetricType metric_type_;
  bool is_trained_;

  // PQ codebooks: m subquantizers, each with ksub centroids of dsub dimensions
  std::vector<std::vector<std::vector<float>>> codebooks_;  // [m][ksub][dsub]
  
  // Compressed vectors: vector_id -> PQ codes (m codes per vector)
  std::vector<int64_t> vector_ids_;
  std::vector<std::vector<uint8_t>> pq_codes_;  // [vector_count][m]
  
  // Distance tables for fast search
  mutable std::vector<std::vector<float>> distance_tables_;  // [m][ksub]
  
  mutable Logger logger_;

public:
  explicit PQIndex(size_t dimension, meta::MetricType metric_type);
  virtual ~PQIndex() = default;

  // BaseIndex interface
  IndexType GetIndexType() const override { 
    return IndexType::PQ; 
  }

  size_t Build(size_t nb, 
              const VectorColumnData& data, 
              const int64_t* ids,
              const IndexBuildParams& params) override;

  void Search(const DenseVectorPtr query,
             const size_t nq,
             const size_t dim,
             const size_t k,
             float* distances,
             int64_t* ids,
             ConcurrentBitsetPtr bitset = nullptr) override;

  size_t GetMemoryUsage() const override;
  IndexStats GetStats() const override;
  bool IsTrained() const override { return is_trained_; }
  size_t GetVectorCount() const override { return vector_ids_.size(); }
  size_t GetDimension() const override { return dimension_; }

  bool SaveToFile(const std::string& file_path) const override;
  bool LoadFromFile(const std::string& file_path) override;

  void Prewarm(const DenseVectorPtr sample_queries,
              size_t query_count,
              size_t dim, 
              size_t k = 10) override;

  // PQ supports incremental updates
  bool AddVectors(size_t nb,
                 const VectorColumnData& data,
                 const int64_t* ids) override;

  bool RemoveVectors(const int64_t* ids, size_t count) override;

private:
  /**
   * @brief Train PQ codebooks using k-means on subvectors
   */
  void TrainCodebooks(const VectorColumnData& data, 
                     size_t nb,
                     const PQBuildParams& params);

  /**
   * @brief Encode vectors using trained codebooks
   */
  void EncodeVectors(const VectorColumnData& data,
                    const int64_t* ids,
                    size_t nb);

  /**
   * @brief Compute distance table for a query vector
   */
  void ComputeDistanceTable(const float* query) const;

  /**
   * @brief Compute asymmetric distance using distance table
   */
  float ComputeAsymmetricDistance(const std::vector<uint8_t>& codes) const;

  /**
   * @brief Train k-means for a specific subquantizer
   */
  void TrainSubquantizer(size_t subq_id,
                        const std::vector<std::vector<float>>& subvectors,
                        size_t iterations = 25);

  /**
   * @brief Encode a single vector to PQ codes
   */
  std::vector<uint8_t> EncodeVector(const float* vector) const;

  /**
   * @brief Find nearest centroid in a subquantizer
   */
  uint8_t FindNearestCentroid(size_t subq_id, const float* subvector) const;

  /**
   * @brief Extract PQ parameters from build params
   */
  PQBuildParams ExtractPQParams(const IndexBuildParams& params) const;

  /**
   * @brief Validate PQ parameters
   */
  bool ValidateParams(const PQBuildParams& params) const;

  /**
   * @brief Get subvector from full vector
   */
  void GetSubvector(const float* vector, size_t subq_id, float* subvector) const;

  /**
   * @brief Calculate distance between two subvectors
   */
  float SubvectorDistance(const float* a, const float* b, size_t dim) const;
};

} // namespace index
} // namespace engine
} // namespace vectordb