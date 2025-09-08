#pragma once

#include "db/index/base_index.hpp"
#include "db/index/distances.hpp"
#include "db/index/index.hpp"
#include "logger/logger.hpp"
#include <vector>
#include <memory>
#include <random>
#include <unordered_map>

namespace vectordb {
namespace engine {
namespace index {

/**
 * @brief IVF (Inverted File) Index implementation
 * 
 * IVF divides the space into clusters using k-means clustering.
 * Each cluster maintains a list of vectors assigned to it.
 * Search involves finding nearest clusters and searching within them.
 */
class IVFIndex : public BaseIndex {
private:
  size_t dimension_;
  size_t nlist_;           // Number of clusters
  size_t nprobe_;          // Number of clusters to search
  meta::MetricType metric_type_;
  bool is_trained_;

  // Cluster centroids (nlist x dimension)
  std::vector<float> centroids_;
  
  // Inverted lists: cluster_id -> list of (vector_id, vector_data)
  std::vector<std::vector<int64_t>> inverted_lists_;
  std::vector<std::vector<std::vector<float>>> cluster_vectors_;
  
  // Vector ID to cluster mapping for deletion support
  std::unordered_map<int64_t, size_t> vector_to_cluster_;
  
  // Distance computation
  DenseVecDistFunc<float> dist_func_;
  
  mutable Logger logger_;

public:
  explicit IVFIndex(size_t dimension, meta::MetricType metric_type);
  virtual ~IVFIndex() = default;

  // BaseIndex interface
  IndexType GetIndexType() const override { 
    return IndexType::IVF; 
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
  size_t GetVectorCount() const override;
  size_t GetDimension() const override { return dimension_; }

  bool SaveToFile(const std::string& file_path) const override;
  bool LoadFromFile(const std::string& file_path) override;

  void Prewarm(const DenseVectorPtr sample_queries,
              size_t query_count,
              size_t dim, 
              size_t k = 10) override;

  // IVF supports incremental updates
  bool AddVectors(size_t nb,
                 const VectorColumnData& data,
                 const int64_t* ids) override;

  bool RemoveVectors(const int64_t* ids, size_t count) override;

private:
  /**
   * @brief Train k-means clustering to create centroids
   */
  void TrainKMeans(const VectorColumnData& data, 
                  size_t nb,
                  const IVFBuildParams& params);

  /**
   * @brief Assign vectors to nearest clusters
   */
  void AssignVectorsToClusters(const VectorColumnData& data,
                              const int64_t* ids,
                              size_t nb);

  /**
   * @brief Find nearest clusters for a query vector
   */
  std::vector<std::pair<size_t, float>> FindNearestClusters(
    const float* query, 
    size_t nprobe) const;

  /**
   * @brief Search within a specific cluster
   */
  void SearchInCluster(size_t cluster_id,
                      const float* query,
                      size_t k,
                      std::vector<std::pair<float, int64_t>>& candidates,
                      ConcurrentBitsetPtr bitset) const;

  /**
   * @brief Extract IVF parameters from build params
   */
  IVFBuildParams ExtractIVFParams(const IndexBuildParams& params) const;

  /**
   * @brief Initialize distance function
   */
  void InitializeDistanceFunction();

  /**
   * @brief Calculate centroid for a set of vectors
   */
  void CalculateCentroid(const std::vector<std::vector<float>>& vectors,
                        float* centroid) const;

  /**
   * @brief Assign vector to nearest centroid
   */
  size_t AssignToNearestCentroid(const float* vector) const;
};

} // namespace index
} // namespace engine
} // namespace vectordb